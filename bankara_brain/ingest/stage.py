"""File staging — hashing, fingerprinting, dataset ingestion."""
from __future__ import annotations

import hashlib
import json
import mimetypes
import uuid
from pathlib import Path
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.orm import Session, sessionmaker

from bankara_brain.db import BlobStore
from bankara_brain.models import Asset, TextSegment
from bankara_brain.utils import (
    SUPPORTED_SUFFIXES,
    build_text_chunks,
    find_sidecar_text_file,
    humanize_stem,
    infer_media_type_and_mime,
    infer_record_kind,
    load_sidecar_metadata,
    normalize_sidecar_metadata,
    probe_media_duration,
    shorten_text,
)
from bankara_brain.youtube.helpers import extract_youtube_video_id


# ── Low-level file helpers ────────────────────────────────────────────────────

def iter_supported_files(root_dir: Path, recursive: bool) -> list[Path]:
    """Return sorted list of supported media/text files under *root_dir*."""
    iterator = root_dir.rglob("*") if recursive else root_dir.glob("*")
    files = []
    for path in iterator:
        if not path.is_file() or path.suffix.lower() not in SUPPORTED_SUFFIXES:
            continue
        if is_text_sidecar_for_media(path):
            continue
        files.append(path)
    return sorted(files)


def is_text_sidecar_for_media(file_path: Path) -> bool:
    """True if *file_path* is a text sidecar (.txt/.md/.srt/.vtt) for a media file."""
    if file_path.suffix.lower() not in {".txt", ".md", ".srt", ".vtt"}:
        return False
    stem_base = file_path.with_suffix("")
    return any(stem_base.with_suffix(suffix).exists() for suffix in [".mp3", ".wav", ".mp4", ".mov"])


def file_sha256(file_path: Path) -> str:
    """Compute SHA-256 hex digest of a file."""
    hasher = hashlib.sha256()
    with file_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def file_fingerprint(file_path: Path, sha256_hex: str | None = None) -> str:
    """Return a fingerprint string combining size, mtime, and SHA-256."""
    sha256_hex = sha256_hex or file_sha256(file_path)
    stat = file_path.stat()
    return f"{stat.st_size}:{stat.st_mtime_ns}:{sha256_hex}"


# ── Text helpers ──────────────────────────────────────────────────────────────

def guess_text_mime(source_path: Path) -> tuple[str, str]:
    """Return (mime_type, extension) for a text file."""
    mime_type, _ = mimetypes.guess_type(source_path.name)
    return mime_type or "text/plain", source_path.suffix.lower().lstrip(".")


def build_transcript_excerpt(transcript_source: Path) -> str:
    """Build a short excerpt from a transcript file."""
    chunks = build_text_chunks(transcript_source)
    if not chunks:
        return ""
    excerpt = " ".join(chunk.text for chunk in chunks[:2])
    return shorten_text(excerpt, 1500)


# ── DB segment loading ───────────────────────────────────────────────────────

def load_segments_into_db(
    session: Session,
    asset: Asset,
    source_path: Path,
    transcript_source: Path | None,
) -> None:
    """Load text segments (source text or transcript) into the database."""
    if asset.media_type == "text":
        chunks = build_text_chunks(source_path)
        segment_kind = "source_text"
        for chunk in chunks:
            session.add(
                TextSegment(
                    asset_id=asset.id,
                    segment_kind=segment_kind,
                    chunk_index=chunk.chunk_index,
                    chunk_count=chunk.chunk_count,
                    start_seconds=chunk.start_seconds,
                    end_seconds=chunk.end_seconds,
                    text=chunk.text,
                )
            )
        return

    if transcript_source:
        chunks = build_text_chunks(transcript_source)
        segment_kind = "transcript"
        for chunk in chunks:
            session.add(
                TextSegment(
                    asset_id=asset.id,
                    segment_kind=segment_kind,
                    chunk_index=chunk.chunk_index,
                    chunk_count=chunk.chunk_count,
                    start_seconds=chunk.start_seconds,
                    end_seconds=chunk.end_seconds,
                    text=chunk.text,
                )
            )


# ── Asset staging ─────────────────────────────────────────────────────────────

def stage_asset(
    session: Session,
    blob_store: BlobStore,
    dataset_dir: Path,
    source_path: Path,
    relative_path: str,
    record_kind: str,
    sha256_hex: str,
    fingerprint: str,
    existing: Asset | None,
    copy_mode: str,
) -> None:
    """Stage a single file into the object store and create/update its Asset."""
    raw_sidecar = load_sidecar_metadata(source_path)
    normalized_metadata = normalize_sidecar_metadata(raw_sidecar)
    title = normalized_metadata.get("title") or humanize_stem(source_path.stem)
    notes = normalized_metadata.get("notes") or normalized_metadata.get("description") or ""
    youtube_video_id = normalized_metadata.get("video_id") or extract_youtube_video_id(normalized_metadata.get("source_url"))
    source_url = normalized_metadata.get("source_url")

    storage_path = blob_store.stage_file(source_path, sha256_hex=sha256_hex, copy_mode=copy_mode)
    transcript_source = find_sidecar_text_file(source_path) if record_kind in {"audio", "video"} else None
    transcript_storage_path = None
    transcript_excerpt = ""
    if transcript_source:
        transcript_sha256 = file_sha256(transcript_source)
        transcript_storage_path = blob_store.stage_file(
            transcript_source,
            sha256_hex=transcript_sha256,
            copy_mode=copy_mode,
        )
        transcript_excerpt = build_transcript_excerpt(transcript_source)

    if record_kind == "text":
        mime_type, _ = guess_text_mime(source_path)
        duration_seconds = None
    else:
        _, mime_type = infer_media_type_and_mime(source_path, record_kind)
        duration_seconds = probe_media_duration(source_path)

    stat = source_path.stat()
    asset = existing or Asset(id=str(uuid.uuid4()), relative_path=relative_path)
    asset.source_path = str(source_path.resolve())
    asset.storage_path = str(storage_path.resolve())
    asset.transcript_storage_path = str(transcript_storage_path.resolve()) if transcript_storage_path else None
    asset.media_type = record_kind
    asset.mime_type = mime_type
    asset.title = title
    asset.fingerprint = fingerprint
    asset.sha256 = sha256_hex
    asset.size_bytes = int(stat.st_size)
    asset.modified_time_ns = int(stat.st_mtime_ns)
    asset.duration_seconds = duration_seconds
    asset.notes = notes
    asset.transcript_excerpt = transcript_excerpt
    asset.channel = normalized_metadata.get("channel")
    asset.published_at = normalized_metadata.get("published_at")
    asset.youtube_video_id = youtube_video_id
    asset.source_url = source_url
    asset.metadata_json = json.dumps(normalized_metadata, ensure_ascii=False)

    session.add(asset)
    session.flush()

    session.execute(delete(TextSegment).where(TextSegment.asset_id == asset.id))
    load_segments_into_db(session, asset, source_path, transcript_source)


def stage_dataset(
    session_factory: sessionmaker[Session],
    blob_store: BlobStore,
    dataset_dir: Path,
    recursive: bool,
    copy_mode: str,
    force: bool,
    limit: int | None,
) -> None:
    """Stage all supported files from a dataset directory into the Brain."""
    if not dataset_dir.exists() or not dataset_dir.is_dir():
        raise FileNotFoundError(f"Directory not found: {dataset_dir}")

    files = iter_supported_files(dataset_dir, recursive=recursive)
    if limit is not None:
        files = files[:limit]

    staged = 0
    skipped = 0

    with session_factory() as session:
        for file_path in files:
            relative_path = str(file_path.relative_to(dataset_dir))
            record_kind = infer_record_kind(file_path)
            sha256_hex = file_sha256(file_path)
            fingerprint = file_fingerprint(file_path, sha256_hex=sha256_hex)
            existing = session.scalar(select(Asset).where(Asset.relative_path == relative_path))

            if existing and existing.fingerprint == fingerprint and not force:
                print(f"Skipping unchanged: {relative_path}")
                skipped += 1
                continue

            print(f"Staging: {relative_path}")
            stage_asset(
                session=session,
                blob_store=blob_store,
                dataset_dir=dataset_dir,
                source_path=file_path,
                relative_path=relative_path,
                record_kind=record_kind,
                sha256_hex=sha256_hex,
                fingerprint=fingerprint,
                existing=existing,
                copy_mode=copy_mode,
            )
            session.commit()
            staged += 1

    print(f"\nStage completed. staged={staged} skipped={skipped} total={len(files)}")
