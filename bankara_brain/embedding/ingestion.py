"""Directory and manifest ingestion pipelines."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

from google import genai
from google.genai import types

from bankara_media_utils import (
    build_manifest_record_id,
    build_text_chunks,
    find_sidecar_text_file,
    humanize_stem,
    infer_media_type_and_mime,
    infer_record_kind,
    load_sidecar_metadata,
    normalize_sidecar_metadata,
    shorten_text,
)

from bankara_brain.embedding.config import (
    PreparedMedia,
    EMBEDDING_MODEL,
    TRANSCRIPT_EXCERPT_CHARS,
)
from bankara_brain.embedding.client import with_transient_retries
from bankara_brain.embedding.vectors import embed_text_document, embed_media_document
from bankara_brain.embedding.media import (
    stable_file_id,
    stable_chunk_id,
    iter_supported_files,
    prepare_media_for_embedding,
    prepare_media_clips_for_full_embedding,
    extract_segment_clips,
    probe_media_duration,
)
from bankara_brain.embedding.store import (
    upsert_embedding,
    delete_embeddings,
    sanitize_metadata,
    load_state,
    save_state,
    coerce_float,
)
from bankara_brain.embedding.search import (
    normalize_matching_text,
)


def ingest_directory(
    client: genai.Client,
    index: Any,
    namespace: str,
    root_dir: Path,
    recursive: bool,
    use_files_api: bool,
    limit: int | None,
    state_file: Path,
    dry_run: bool,
    force: bool,
    allow_trim_long_media: bool,
    report_output: Path | None,
) -> None:
    if not root_dir.exists() or not root_dir.is_dir():
        raise FileNotFoundError(f"Directory not found: {root_dir}")

    files = iter_supported_files(root_dir, recursive=recursive)
    if limit is not None:
        files = files[:limit]

    if not files:
        print("No supported files found.")
        return

    state = load_state(state_file)
    inserted = 0
    skipped = 0
    unchanged = 0
    failed = 0

    report_handle = open_jsonl_writer(report_output)
    try:
        with tempfile.TemporaryDirectory(prefix="gemini_pinecone_ingest_") as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            for file_path in files:
                relative_path = str(file_path.relative_to(root_dir))
                fingerprint = fingerprint_file(file_path)
                source_type = infer_record_kind(file_path)
                previous_entry = state["files"].get(relative_path)

                if previous_entry and previous_entry.get("fingerprint") == fingerprint and not force:
                    print(f"Skipping unchanged: {relative_path}")
                    write_jsonl_row(
                        report_handle,
                        {
                            "run_type": "ingest_dir",
                            "status": "unchanged",
                            "relative_path": relative_path,
                            "source_path": str(file_path),
                            "source_type": source_type,
                            "fingerprint": fingerprint,
                        },
                    )
                    unchanged += 1
                    continue

                print(f"Ingesting: {relative_path}", flush=True)
                if dry_run:
                    write_jsonl_row(
                        report_handle,
                        {
                            "run_type": "ingest_dir",
                            "status": "dry_run",
                            "relative_path": relative_path,
                            "source_path": str(file_path),
                            "source_type": source_type,
                            "fingerprint": fingerprint,
                        },
                    )
                    inserted += 1
                    continue

                try:
                    record_ids = upsert_supported_file(
                        client=client,
                        index=index,
                        namespace=namespace,
                        root_dir=root_dir,
                        file_path=file_path,
                        use_files_api=use_files_api,
                        allow_trim_long_media=allow_trim_long_media,
                        temp_dir=temp_dir,
                    )

                    stale_ids = []
                    if previous_entry:
                        previous_ids = set(previous_entry.get("record_ids", []))
                        stale_ids = sorted(previous_ids - set(record_ids))
                    if stale_ids:
                        delete_embeddings(index, namespace=namespace, record_ids=stale_ids)

                    state["files"][relative_path] = {
                        "fingerprint": fingerprint,
                        "record_ids": record_ids,
                        "updated_at": int(time.time()),
                        "source_type": source_type,
                    }
                    save_state(state_file, state)
                    write_jsonl_row(
                        report_handle,
                        {
                            "run_type": "ingest_dir",
                            "status": "upserted",
                            "relative_path": relative_path,
                            "source_path": str(file_path),
                            "source_type": source_type,
                            "fingerprint": fingerprint,
                            "record_ids": record_ids,
                            "stale_record_ids": stale_ids,
                        },
                    )
                    inserted += 1
                except Exception as exc:
                    failed += 1
                    skipped += 1
                    write_jsonl_row(
                        report_handle,
                        {
                            "run_type": "ingest_dir",
                            "status": "error",
                            "relative_path": relative_path,
                            "source_path": str(file_path),
                            "source_type": source_type,
                            "fingerprint": fingerprint,
                            "error": str(exc),
                        },
                    )
                    print(f"  skipped: {exc}")
    finally:
        close_jsonl_writer(report_handle)

    total = len(files)
    print(
        "\nDirectory ingest completed. "
        f"processed={inserted} unchanged={unchanged} failed={failed} skipped={skipped} total={total}"
    )
    if report_output:
        print(f"Wrote ingest report: {report_output}")


def ingest_manifest(
    client: genai.Client,
    index: Any,
    default_namespace: str,
    manifest_path: Path,
    use_files_api: bool,
    allow_trim_long_media: bool,
    results_output: Path | None,
    limit: int | None,
    report_output: Path | None,
) -> None:
    processed = 0
    inserted = 0
    skipped = 0

    output_handle = None
    report_handle = open_jsonl_writer(report_output)
    if results_output:
        results_output.parent.mkdir(parents=True, exist_ok=True)
        output_handle = results_output.open("w", encoding="utf-8")

    try:
        with manifest_path.open("r", encoding="utf-8") as handle, tempfile.TemporaryDirectory(
            prefix="gemini_manifest_ingest_"
        ) as temp_dir_name:
            temp_dir = Path(temp_dir_name)

            for line_number, raw_line in enumerate(handle, start=1):
                raw_line = raw_line.strip()
                if not raw_line:
                    continue
                if limit is not None and processed >= limit:
                    break

                processed += 1
                payload = json.loads(raw_line)
                entry_type = payload.get("entry_type")
                namespace = payload.get("namespace") or default_namespace
                asset_id = payload.get("asset_id")
                title = payload.get("title") or "untitled"
                metadata = payload.get("metadata") or {}
                if not isinstance(metadata, dict):
                    raise ValueError(f"Manifest line {line_number} metadata must be an object")

                if entry_type in {"text_chunk", "timeline_segment"}:
                    record_id = payload.get("record_id") or build_manifest_record_id(payload)
                    text = payload.get("text", "")
                    if not text.strip():
                        skipped += 1
                        print(f"Skipping empty {entry_type} at line {line_number}")
                        write_jsonl_row(
                            report_handle,
                            {
                                "run_type": "ingest_manifest",
                                "status": "skipped_empty",
                                "line_number": line_number,
                                "entry_type": entry_type,
                                "asset_id": asset_id,
                                "title": title,
                                "namespace": namespace,
                            },
                        )
                        continue

                    vector = embed_text_document(client, title=title, text=text)
                    embedding_kind = metadata.get("embedding_kind") or entry_type
                    document_media_type = payload.get("media_type") or metadata.get("media_type") or "text"
                    upsert_metadata = {
                        **metadata,
                        "title": title,
                        "asset_id": asset_id,
                        "media_type": document_media_type,
                        "embedding_kind": embedding_kind,
                        "source_path": payload.get("source_path", ""),
                        "relative_path": payload.get("relative_path", ""),
                        "notes": shorten_text(payload.get("notes") or text, 500),
                        "chunk_index": payload.get("chunk_index"),
                        "chunk_count": payload.get("chunk_count"),
                    }
                    upsert_embedding(
                        index=index,
                        namespace=namespace,
                        record_id=record_id,
                        vector=vector,
                        metadata=upsert_metadata,
                    )
                    inserted += 1
                    maybe_write_embedding_result(
                        output_handle=output_handle,
                        asset_id=asset_id,
                        namespace=namespace,
                        record_id=record_id,
                        media_type=document_media_type,
                        chunk_index=payload.get("chunk_index", payload.get("segment_index")),
                        metadata=upsert_metadata,
                    )
                    write_jsonl_row(
                        report_handle,
                        {
                            "run_type": "ingest_manifest",
                            "status": "upserted",
                            "line_number": line_number,
                            "entry_type": entry_type,
                            "asset_id": asset_id,
                            "record_id": record_id,
                            "title": title,
                            "namespace": namespace,
                            "media_type": document_media_type,
                        },
                    )
                    continue

                if entry_type == "segment_media":
                    # Multimodal segment embedding: extract video/audio clips
                    # covering the FULL segment range and embed each one.
                    # Long segments are split into ≤120s clips so Gemini
                    # Embedding 2 sees the entire video — nothing is discarded.
                    source_path = Path(payload["source_path"]).expanduser().resolve()
                    if not source_path.exists():
                        raise FileNotFoundError(f"Manifest segment media file not found: {source_path}")

                    seg_start = float(payload.get("clip_start_seconds", 0))
                    seg_end = float(payload.get("clip_end_seconds", 0))
                    seg_media_type = payload.get("media_type") or "video"
                    base_record_id = payload.get("record_id") or build_manifest_record_id(payload)
                    notes = payload.get("notes", "")

                    try:
                        clip_list = extract_segment_clips(
                            source_path=source_path,
                            start_seconds=seg_start,
                            end_seconds=seg_end,
                            temp_dir=temp_dir,
                            media_type=seg_media_type,
                        )
                    except Exception as clip_exc:
                        # If clip extraction fails, fall back to text-only embedding
                        logger.warning(
                            "Clip extraction failed for %s [%.1fs-%.1fs], falling back to text-only: %s",
                            source_path.name, seg_start, seg_end, clip_exc,
                        )
                        fallback_text = notes or payload.get("text", "")
                        if not fallback_text.strip():
                            skipped += 1
                            continue
                        vector = embed_text_document(client, title=title, text=fallback_text)
                        upsert_metadata = {
                            **metadata,
                            "title": title,
                            "asset_id": asset_id,
                            "media_type": seg_media_type,
                            "embedding_kind": "timeline_segment",
                            "source_path": str(source_path),
                            "relative_path": payload.get("relative_path", ""),
                            "notes": shorten_text(fallback_text, 500),
                            "clip_start_seconds": seg_start,
                            "clip_end_seconds": seg_end,
                            "multimodal_segment": False,
                            "multimodal_fallback_reason": str(clip_exc)[:200],
                        }
                        upsert_embedding(
                            index=index,
                            namespace=namespace,
                            record_id=base_record_id,
                            vector=vector,
                            metadata=upsert_metadata,
                        )
                        inserted += 1
                        write_jsonl_row(
                            report_handle,
                            {
                                "run_type": "ingest_manifest",
                                "status": "upserted_text_fallback",
                                "line_number": line_number,
                                "entry_type": entry_type,
                                "asset_id": asset_id,
                                "record_id": base_record_id,
                                "title": title,
                                "namespace": namespace,
                                "fallback_reason": str(clip_exc)[:200],
                            },
                        )
                        continue

                    # Embed each clip (covers the full segment, no data discarded)
                    total_clips = len(clip_list)
                    for clip_idx, (prepared_media, c_start, c_end) in enumerate(clip_list):
                        # For multi-clip segments, append clip index to record_id
                        clip_record_id = (
                            f"{base_record_id}-clip{clip_idx}"
                            if total_clips > 1
                            else base_record_id
                        )
                        clip_title = (
                            f"{title} [clip {clip_idx + 1}/{total_clips}]"
                            if total_clips > 1
                            else title
                        )
                        # Add clip position context to the notes
                        clip_notes = notes
                        if total_clips > 1:
                            clip_notes = (
                                f"clip_position: {clip_idx + 1}/{total_clips} "
                                f"({c_start:.1f}s-{c_end:.1f}s of segment "
                                f"{seg_start:.1f}s-{seg_end:.1f}s)\n{notes}"
                            )

                        print(
                            f"  Embedding segment clip {clip_idx + 1}/{total_clips}: "
                            f"{source_path.name} [{c_start:.1f}s-{c_end:.1f}s] "
                            f"({prepared_media.embed_duration_seconds:.1f}s)"
                        )
                        vector = embed_media_document(
                            client=client,
                            title=clip_title,
                            prepared_media=prepared_media,
                            notes=clip_notes,
                            use_files_api=use_files_api,
                        )
                        upsert_metadata = {
                            **metadata,
                            "title": clip_title,
                            "asset_id": asset_id,
                            "media_type": prepared_media.media_type,
                            "mime_type": prepared_media.mime_type,
                            "embedding_kind": "timeline_segment",
                            "source_path": str(source_path),
                            "relative_path": payload.get("relative_path", ""),
                            "notes": shorten_text(clip_notes, 1500),
                            "source_duration_seconds": seg_end - seg_start,
                            "embedded_duration_seconds": prepared_media.embed_duration_seconds,
                            "was_trimmed": False,
                            "clip_start_seconds": c_start,
                            "clip_end_seconds": c_end,
                            "segment_start_seconds": seg_start,
                            "segment_end_seconds": seg_end,
                            "clip_index": clip_idx,
                            "clip_count": total_clips,
                            "multimodal_segment": True,
                        }
                        upsert_embedding(
                            index=index,
                            namespace=namespace,
                            record_id=clip_record_id,
                            vector=vector,
                            metadata=upsert_metadata,
                        )
                        inserted += 1
                        maybe_write_embedding_result(
                            output_handle=output_handle,
                            asset_id=asset_id,
                            namespace=namespace,
                            record_id=clip_record_id,
                            media_type=prepared_media.media_type,
                            chunk_index=payload.get("segment_index"),
                            metadata=upsert_metadata,
                        )
                        write_jsonl_row(
                            report_handle,
                            {
                                "run_type": "ingest_manifest",
                                "status": "upserted",
                                "line_number": line_number,
                                "entry_type": entry_type,
                                "asset_id": asset_id,
                                "record_id": clip_record_id,
                                "title": clip_title,
                                "namespace": namespace,
                                "media_type": prepared_media.media_type,
                                "source_path": str(source_path),
                                "clip_start_seconds": c_start,
                                "clip_end_seconds": c_end,
                                "clip_index": clip_idx,
                                "clip_count": total_clips,
                                "multimodal_segment": True,
                            },
                        )
                    continue

                if entry_type == "media":
                    source_path = Path(payload["source_path"]).expanduser().resolve()
                    if not source_path.exists():
                        raise FileNotFoundError(f"Manifest media file not found: {source_path}")

                    media_type = payload.get("media_type")
                    base_record_id = payload.get("record_id") or build_manifest_record_id(payload)
                    notes = payload.get("notes", "")

                    # Split the full media file into clips so Embedding 2 sees
                    # the entire video/audio — nothing is discarded.
                    clip_list = prepare_media_clips_for_full_embedding(
                        file_path=source_path,
                        declared_media_type=media_type,
                        temp_dir=temp_dir,
                    )
                    total_clips = len(clip_list)
                    for clip_idx, (prepared_media, c_start, c_end) in enumerate(clip_list):
                        clip_record_id = (
                            f"{base_record_id}-clip{clip_idx}"
                            if total_clips > 1
                            else base_record_id
                        )
                        clip_title = (
                            f"{title} [clip {clip_idx + 1}/{total_clips}]"
                            if total_clips > 1
                            else title
                        )
                        clip_notes = notes
                        if total_clips > 1:
                            clip_notes = (
                                f"clip_position: {clip_idx + 1}/{total_clips} "
                                f"({c_start:.1f}s-{c_end:.1f}s)\n{notes}"
                            )

                        if total_clips > 1:
                            print(
                                f"  Embedding media clip {clip_idx + 1}/{total_clips}: "
                                f"{source_path.name} [{c_start:.1f}s-{c_end:.1f}s]"
                            )

                        vector = embed_media_document(
                            client=client,
                            title=clip_title,
                            prepared_media=prepared_media,
                            notes=clip_notes,
                            use_files_api=use_files_api,
                        )
                        upsert_metadata = {
                            **metadata,
                            "title": clip_title,
                            "asset_id": asset_id,
                            "media_type": prepared_media.media_type,
                            "mime_type": prepared_media.mime_type,
                            "source_path": str(source_path),
                            "relative_path": payload.get("relative_path", ""),
                            "notes": shorten_text(clip_notes, 1500),
                            "source_duration_seconds": prepared_media.source_duration_seconds,
                            "embedded_duration_seconds": prepared_media.embed_duration_seconds,
                            "was_trimmed": False,
                            "clip_start_seconds": c_start,
                            "clip_end_seconds": c_end,
                            "clip_index": clip_idx,
                            "clip_count": total_clips,
                        }
                        upsert_embedding(
                            index=index,
                            namespace=namespace,
                            record_id=clip_record_id,
                            vector=vector,
                            metadata=upsert_metadata,
                        )
                        inserted += 1
                        maybe_write_embedding_result(
                            output_handle=output_handle,
                            asset_id=asset_id,
                            namespace=namespace,
                            record_id=clip_record_id,
                            media_type=prepared_media.media_type,
                            chunk_index=clip_idx if total_clips > 1 else None,
                            metadata=upsert_metadata,
                        )
                        write_jsonl_row(
                            report_handle,
                            {
                                "run_type": "ingest_manifest",
                                "status": "upserted",
                                "line_number": line_number,
                                "entry_type": entry_type,
                                "asset_id": asset_id,
                                "record_id": clip_record_id,
                                "title": clip_title,
                                "namespace": namespace,
                                "media_type": prepared_media.media_type,
                                "source_path": str(source_path),
                                "clip_start_seconds": c_start,
                                "clip_end_seconds": c_end,
                                "clip_index": clip_idx,
                                "clip_count": total_clips,
                            },
                        )
                    continue

                raise ValueError(f"Unsupported manifest entry_type at line {line_number}: {entry_type}")

    except Exception as exc:
        write_jsonl_row(
            report_handle,
            {
                "run_type": "ingest_manifest",
                "status": "error",
                "processed": processed,
                "inserted": inserted,
                "skipped": skipped,
                "error": str(exc),
            },
        )
        raise
    finally:
        if output_handle:
            output_handle.close()
        close_jsonl_writer(report_handle)

    print(f"Manifest ingest completed. processed={processed} inserted={inserted} skipped={skipped}")
    if report_output:
        print(f"Wrote ingest report: {report_output}")


def open_jsonl_writer(output_path: Path | None) -> Any:
    if output_path is None:
        return None
    output_path.parent.mkdir(parents=True, exist_ok=True)
    return output_path.open("w", encoding="utf-8")


def write_jsonl_row(handle: Any, payload: dict[str, Any]) -> None:
    if handle is None:
        return
    handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
    handle.flush()


def close_jsonl_writer(handle: Any) -> None:
    if handle is not None:
        handle.close()


def maybe_write_embedding_result(
    output_handle: Any,
    asset_id: str | None,
    namespace: str,
    record_id: str,
    media_type: str,
    chunk_index: int | None,
    metadata: dict[str, Any],
) -> None:
    if not output_handle:
        return

    output_handle.write(
        json.dumps(
            {
                "asset_id": asset_id,
                "namespace": namespace,
                "record_id": record_id,
                "embedding_model": EMBEDDING_MODEL,
                "media_type": media_type,
                "chunk_index": chunk_index,
                "metadata": sanitize_metadata(metadata),
            },
            ensure_ascii=False,
        )
        + "\n"
    )


def validate_manifest(manifest_path: Path, limit: int | None) -> None:
    processed = 0
    with manifest_path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            if limit is not None and processed >= limit:
                break
            payload = json.loads(raw_line)
            entry_type = payload.get("entry_type")
            if entry_type not in {"text_chunk", "timeline_segment", "segment_media", "media"}:
                raise ValueError(f"Manifest line {line_number} has unsupported entry_type: {entry_type}")
            if entry_type in {"text_chunk", "timeline_segment"} and not str(payload.get("text", "")).strip():
                raise ValueError(f"Manifest line {line_number} {entry_type} is missing text")
            if entry_type in {"media", "segment_media"}:
                source_path = Path(payload["source_path"]).expanduser().resolve()
                if not source_path.exists():
                    raise FileNotFoundError(f"Manifest line {line_number} {entry_type} file not found: {source_path}")
            processed += 1
    print(f"Manifest validated. entries={processed}")


def upsert_supported_file(
    client: genai.Client,
    index: Any,
    namespace: str,
    root_dir: Path,
    file_path: Path,
    use_files_api: bool,
    allow_trim_long_media: bool,
    temp_dir: Path,
) -> list[str]:
    kind = infer_record_kind(file_path)

    if kind == "text":
        return upsert_text_file(
            client=client,
            index=index,
            namespace=namespace,
            root_dir=root_dir,
            file_path=file_path,
        )

    return upsert_media_file(
        client=client,
        index=index,
        namespace=namespace,
        root_dir=root_dir,
        file_path=file_path,
        declared_media_type=kind,
        use_files_api=use_files_api,
        allow_trim_long_media=allow_trim_long_media,
        temp_dir=temp_dir,
    )


def upsert_text_file(
    client: genai.Client,
    index: Any,
    namespace: str,
    root_dir: Path,
    file_path: Path,
) -> list[str]:
    title, extra_metadata, notes = resolve_title_and_metadata(file_path)
    text_chunks = build_text_chunks(file_path)
    if not text_chunks:
        raise ValueError("text file is empty after normalization")

    record_ids: list[str] = []
    relative_path = str(file_path.relative_to(root_dir))

    for chunk in text_chunks:
        chunk_title = title
        if chunk.chunk_count > 1:
            chunk_title = f"{title} [chunk {chunk.chunk_index + 1}/{chunk.chunk_count}]"

        vector = embed_text_document(client, title=chunk_title, text=chunk.text)
        record_id = stable_chunk_id("text", file_path, root_dir, chunk.chunk_index)
        metadata = {
            "title": title,
            "media_type": "text",
            "embedding_kind": "text_chunk",
            "source_path": str(file_path),
            "relative_path": relative_path,
            "notes": shorten_text(notes or chunk.text, 500),
            "chunk_index": chunk.chunk_index,
            "chunk_count": chunk.chunk_count,
            "chunk_start_seconds": chunk.start_seconds,
            "chunk_end_seconds": chunk.end_seconds,
            "text_format": file_path.suffix.lower().lstrip("."),
            **extra_metadata,
        }
        upsert_embedding(index=index, namespace=namespace, record_id=record_id, vector=vector, metadata=metadata)
        record_ids.append(record_id)

    return record_ids


def upsert_media_file(
    client: genai.Client,
    index: Any,
    namespace: str,
    root_dir: Path,
    file_path: Path,
    declared_media_type: str,
    use_files_api: bool,
    allow_trim_long_media: bool,
    temp_dir: Path,
) -> list[str]:
    title, extra_metadata, notes = resolve_title_and_metadata(file_path)
    transcript_excerpt = load_sidecar_transcript_excerpt(file_path)
    relative_path = str(file_path.relative_to(root_dir))

    notes_payload = build_media_notes(
        relative_path=relative_path,
        notes=notes,
        transcript_excerpt=transcript_excerpt,
        extra_metadata=extra_metadata,
    )

    # Split the full media file into clips so Embedding 2 sees
    # the entire video/audio — nothing is discarded.
    clip_list = prepare_media_clips_for_full_embedding(
        file_path=file_path,
        declared_media_type=declared_media_type,
        temp_dir=temp_dir,
    )
    total_clips = len(clip_list)
    base_record_id = stable_file_id(
        clip_list[0][0].media_type, file_path, root_dir,
    )

    record_ids: list[str] = []
    for clip_idx, (prepared_media, c_start, c_end) in enumerate(clip_list):
        clip_record_id = (
            f"{base_record_id}-clip{clip_idx}"
            if total_clips > 1
            else base_record_id
        )
        clip_title = (
            f"{title} [clip {clip_idx + 1}/{total_clips}]"
            if total_clips > 1
            else title
        )
        clip_notes = notes_payload
        if total_clips > 1:
            clip_notes = (
                f"clip_position: {clip_idx + 1}/{total_clips} "
                f"({c_start:.1f}s-{c_end:.1f}s)\n{notes_payload}"
            )
            print(
                f"  Embedding clip {clip_idx + 1}/{total_clips}: "
                f"{file_path.name} [{c_start:.1f}s-{c_end:.1f}s]"
            )

        vector = embed_media_document(
            client=client,
            title=clip_title,
            prepared_media=prepared_media,
            notes=clip_notes,
            use_files_api=use_files_api,
        )
        metadata = {
            "title": clip_title,
            "media_type": prepared_media.media_type,
            "embedding_kind": "asset",
            "mime_type": prepared_media.mime_type,
            "source_path": str(file_path),
            "relative_path": relative_path,
            "notes": shorten_text(clip_notes, 1500),
            "source_duration_seconds": prepared_media.source_duration_seconds,
            "embedded_duration_seconds": prepared_media.embed_duration_seconds,
            "was_trimmed": False,
            "clip_start_seconds": c_start,
            "clip_end_seconds": c_end,
            "clip_index": clip_idx,
            "clip_count": total_clips,
            **extra_metadata,
        }
        upsert_embedding(
            index=index, namespace=namespace, record_id=clip_record_id,
            vector=vector, metadata=metadata,
        )
        record_ids.append(clip_record_id)

    return record_ids


def build_media_notes(
    relative_path: str,
    notes: str,
    transcript_excerpt: str,
    extra_metadata: dict[str, Any],
) -> str:
    lines = [f"source: {relative_path}"]
    if notes:
        lines.append(f"notes: {notes}")
    if extra_metadata.get("description"):
        lines.append(f"description: {extra_metadata['description']}")
    if extra_metadata.get("tags"):
        lines.append(f"tags: {', '.join(extra_metadata['tags'])}")
    if extra_metadata.get("published_at"):
        lines.append(f"published_at: {extra_metadata['published_at']}")
    if transcript_excerpt:
        lines.append(f"transcript_excerpt: {transcript_excerpt}")
    return "\n".join(lines)


def resolve_title_and_metadata(file_path: Path) -> tuple[str, dict[str, Any], str]:
    raw_sidecar = load_sidecar_metadata(file_path)
    normalized = normalize_sidecar_metadata(raw_sidecar)
    title = normalized.pop("title", None) or humanize_stem(file_path.stem)
    notes = normalized.get("notes") or normalized.get("description") or ""
    return title, normalized, notes


def load_sidecar_transcript_excerpt(file_path: Path) -> str:
    transcript_file = find_sidecar_text_file(file_path)
    if not transcript_file:
        return ""
    chunks = build_text_chunks(transcript_file)
    if not chunks:
        return ""
    excerpt = " ".join(chunk.text for chunk in chunks[:2])
    return shorten_text(excerpt, TRANSCRIPT_EXCERPT_CHARS)


def fingerprint_file(file_path: Path) -> str:
    stat = file_path.stat()
    payload = f"{stat.st_size}:{stat.st_mtime_ns}".encode("utf-8")
    return hashlib.sha1(payload).hexdigest()
