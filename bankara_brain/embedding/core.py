#!/usr/bin/env python3
"""
Gemini Embedding 2 + Pinecone multimodal CLI.

All business logic now lives in ``bankara_brain.embedding.*`` submodules.
This file re-exports every public name and provides the CLI entry point.

Install:
  pip install --upgrade google-genai pinecone python-dotenv

Optional local media tooling:
  brew install ffmpeg

Minimal .env:
  GEMINI_API_KEY=your_gemini_api_key
  PINECONE_API_KEY=your_pinecone_api_key
  PINECONE_INDEX_NAME=bankara-brain-mvp
  PINECONE_NAMESPACE=bankara-radio
  PINECONE_CLOUD=aws
  PINECONE_REGION=us-east-1

Common commands:
  python gemini_pinecone_multimodal_mvp.py ensure-index

  python gemini_pinecone_multimodal_mvp.py ingest-dir \
    --dir ./dataset \
    --recursive \
    --use-files-api

  python gemini_pinecone_multimodal_mvp.py search \
    --query "学校ドッキリでテンポが速く、SEが強い動画" \
    --media-type video

Sidecar conventions:
  sample.mp4.json   -> metadata override for sample.mp4
  sample.json       -> fallback metadata override for sample.mp4
  sample.srt        -> transcript sidecar for sample.mp4/sample.mp3
  sample.vtt        -> transcript sidecar for sample.mp4/sample.mp3
  sample.txt        -> transcript/notes sidecar for sample.mp4/sample.mp3

Supported input files:
  Text:  .txt .md .srt .vtt
  Audio: .mp3 .wav
  Video: .mp4 .mov
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

from bankara_media_utils import shorten_text

# ── Re-exports from submodules ──────────────────────────────────────────────
from bankara_brain.embedding.config import (  # noqa: F401
    INDEX_DIMENSION,
    INDEX_METRIC,
    EMBEDDING_MODEL,
    INLINE_REQUEST_LIMIT_BYTES,
    VIDEO_DURATION_LIMIT_SECONDS,
    AUDIO_DURATION_LIMIT_SECONDS,
    STATE_VERSION,
    DEFAULT_STATE_FILE,
    TRANSCRIPT_EXCERPT_CHARS,
    FILE_READY_TIMEOUT_SECONDS,
    TRANSIENT_RETRY_ATTEMPTS,
    TRANSIENT_RETRY_BASE_SECONDS,
    FEEDBACK_SCORE_FIELDS,
    SUMMARY_TEXT_KEY,
    SUMMARY_JSON_KEY,
    QUERY_FACET_MODEL,
    SEGMENT_KIND_PRIORITY,
    FACET_CONFLICT_COMBINED_WEIGHT,
    LANE_CONFLICT_COMBINED_WEIGHT,
    CANONICAL_MATCH_TAGS,
    CANONICAL_TAG_GROUPS,
    QUERY_TARGET_LANE_HINTS,
    LANE_TARGET_GUARDS,
    Settings,
    PreparedMedia,
)
from bankara_brain.embedding.client import (  # noqa: F401
    default_namespace_from_env,
    create_genai_client,
    create_pinecone_client,
    ensure_pinecone_index,
    is_transient_error,
    with_transient_retries,
    _get_attr,
    _index_ready,
)
from bankara_brain.embedding.vectors import (  # noqa: F401
    _single_embedding_values,
    embed_text,
    embed_text_document,
    embed_media_document,
)
from bankara_brain.embedding.media import (  # noqa: F401
    wait_for_uploaded_file_ready,
    file_state_name,
    stable_file_id,
    stable_chunk_id,
    iter_supported_files,
    probe_media_duration,
    prepare_media_for_embedding,
    trim_media_to_limit,
    _extract_single_clip,
    extract_segment_clips,
)
from bankara_brain.embedding.store import (  # noqa: F401
    upsert_embedding,
    delete_embeddings,
    sanitize_metadata,
    prepare_metadata_for_index,
    flatten_feedback_summary_metadata,
    parse_feedback_summary_value,
    coerce_float,
    parse_generated_json_payload,
    load_state,
    save_state,
)
from bankara_brain.embedding.search import (  # noqa: F401
    normalize_matching_text,
    infer_canonical_match_tags,
    augment_matching_text,
    collect_canonical_tags_from_query_facets,
    group_canonical_tags,
    query_haystack_text,
    lane_broad_family_label,
    lane_cluster_label,
    lane_target_allowed,
    infer_query_target_lanes,
    score_target_lane_alignment,
    split_summary_field_values,
    extract_matching_fragments,
    extract_structured_summary_text,
    extract_summary_field_text,
    score_text_alignment,
    score_list_alignment,
    build_query_facets,
    query_facets_active,
    match_segment_priority,
    match_asset_group_key,
    diversify_matches_by_asset,
    score_query_facets_against_match,
    search_similar,
    normalize_search_matches,
    build_search_payload,
    write_search_payload,
    normalize_match_metadata,
    extract_match_feedback_score,
    print_matches,
    format_match_time_range,
    format_seconds,
)
from bankara_brain.embedding.ingestion import (  # noqa: F401
    ingest_directory,
    ingest_manifest,
    open_jsonl_writer,
    write_jsonl_row,
    close_jsonl_writer,
    maybe_write_embedding_result,
    validate_manifest,
    upsert_supported_file,
    upsert_text_file,
    upsert_media_file,
    build_media_notes,
    resolve_title_and_metadata,
    load_sidecar_transcript_excerpt,
    fingerprint_file,
)

from google import genai  # noqa: F401 — used by smoke test functions below


# ── Smoke test functions (CLI-only) ────────────────────────────────────────


def smoke_test_embedding2(
    client: genai.Client,
    text: str,
    audio_path: Path | None,
    video_path: Path | None,
    use_files_api: bool,
    allow_trim_long_media: bool,
    output_path: Path | None,
) -> None:
    results: dict[str, Any] = {
        "model": EMBEDDING_MODEL,
        "dimension": INDEX_DIMENSION,
        "text": run_text_smoke_test(client, text),
        "audio": None,
        "video": None,
    }

    with tempfile.TemporaryDirectory(prefix="gemini_embedding2_smoke_") as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        if audio_path is not None:
            results["audio"] = run_media_smoke_test(
                client=client,
                file_path=audio_path,
                declared_media_type="audio",
                use_files_api=use_files_api,
                allow_trim_long_media=allow_trim_long_media,
                temp_dir=temp_dir,
            )
        if video_path is not None:
            results["video"] = run_media_smoke_test(
                client=client,
                file_path=video_path,
                declared_media_type="video",
                use_files_api=use_files_api,
                allow_trim_long_media=allow_trim_long_media,
                temp_dir=temp_dir,
            )

    print_smoke_test_results(results)
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Wrote smoke test results: {output_path}")


def run_text_smoke_test(client: genai.Client, text: str) -> dict[str, Any]:
    started_at = time.time()
    try:
        vector = embed_text(client, text, task_type="RETRIEVAL_DOCUMENT")
        return {
            "status": "ok",
            "elapsed_ms": round((time.time() - started_at) * 1000, 1),
            "dimension": len(vector),
            "vector_preview": round_vector_preview(vector),
            "text_preview": shorten_text(text, 160),
        }
    except Exception as exc:
        return {
            "status": "error",
            "elapsed_ms": round((time.time() - started_at) * 1000, 1),
            "error": str(exc),
            "text_preview": shorten_text(text, 160),
        }


def run_media_smoke_test(
    client: genai.Client,
    file_path: Path,
    declared_media_type: str,
    use_files_api: bool,
    allow_trim_long_media: bool,
    temp_dir: Path,
) -> dict[str, Any]:
    started_at = time.time()
    payload: dict[str, Any] = {
        "file_path": str(file_path),
        "declared_media_type": declared_media_type,
        "use_files_api": use_files_api,
    }
    try:
        prepared_media = prepare_media_for_embedding(
            file_path=file_path,
            declared_media_type=declared_media_type,
            allow_trim_long_media=allow_trim_long_media,
            temp_dir=temp_dir,
        )
        vector = embed_media_document(
            client=client,
            title=f"smoke test {prepared_media.media_type}",
            prepared_media=prepared_media,
            notes="embedding2 smoke test",
            use_files_api=use_files_api,
        )
        payload.update(
            {
                "status": "ok",
                "elapsed_ms": round((time.time() - started_at) * 1000, 1),
                "dimension": len(vector),
                "vector_preview": round_vector_preview(vector),
                "embedded_media_type": prepared_media.media_type,
                "mime_type": prepared_media.mime_type,
                "source_duration_seconds": prepared_media.source_duration_seconds,
                "embedded_duration_seconds": prepared_media.embed_duration_seconds,
                "was_trimmed": prepared_media.was_trimmed,
            }
        )
        return payload
    except Exception as exc:
        payload.update(
            {
                "status": "error",
                "elapsed_ms": round((time.time() - started_at) * 1000, 1),
                "error": str(exc),
            }
        )
        return payload


def round_vector_preview(vector: list[float], limit: int = 8) -> list[float]:
    return [round(value, 6) for value in vector[:limit]]


def print_smoke_test_results(results: dict[str, Any]) -> None:
    print("\nEmbedding 2 smoke test:")
    print(f"model={results['model']} dimension={results['dimension']}")
    print_smoke_test_item("text", results.get("text"))
    if results.get("audio") is not None:
        print_smoke_test_item("audio", results["audio"])
    if results.get("video") is not None:
        print_smoke_test_item("video", results["video"])


def print_smoke_test_item(label: str, payload: dict[str, Any] | None) -> None:
    if payload is None:
        return
    status = payload.get("status", "unknown")
    elapsed_ms = payload.get("elapsed_ms")
    print(f"- {label}: status={status} elapsed_ms={elapsed_ms}")
    if status == "ok":
        print(f"  dimension={payload.get('dimension')}")
        if payload.get("mime_type"):
            print(f"  mime_type={payload['mime_type']}")
        if payload.get("file_path"):
            print(f"  file_path={payload['file_path']}")
        if payload.get("was_trimmed") is not None:
            print(
                "  duration="
                f"{payload.get('source_duration_seconds')} -> {payload.get('embedded_duration_seconds')} "
                f"trimmed={payload.get('was_trimmed')}"
            )
        if payload.get("vector_preview"):
            print(f"  vector_preview={payload['vector_preview']}")
    else:
        print(f"  error={payload.get('error')}")


# ── CLI ─────────────────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Gemini Embedding 2 + Pinecone multimodal ingestion/search script",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    ensure_index_parser = subparsers.add_parser(
        "ensure-index", help="Create/connect the Pinecone index."
    )
    ensure_index_parser.add_argument("--namespace", default=None, help="Optional namespace override.")

    upsert_text_parser = subparsers.add_parser(
        "upsert-text", help="Embed text and upsert it into Pinecone."
    )
    upsert_text_parser.add_argument("--id", dest="record_id", default=None, help="Custom record id.")
    upsert_text_parser.add_argument("--title", required=True, help="Human-readable title.")
    upsert_text_parser.add_argument("--text", required=True, help="Text payload to embed.")
    upsert_text_parser.add_argument("--namespace", default=None, help="Optional namespace override.")

    upsert_media_parser = subparsers.add_parser(
        "upsert-media", help="Embed a local audio/video file and upsert it into Pinecone."
    )
    upsert_media_parser.add_argument("--id", dest="record_id", default=None, help="Custom record id.")
    upsert_media_parser.add_argument("--title", required=True, help="Human-readable title.")
    upsert_media_parser.add_argument("--file", type=Path, required=True, help="Path to .mp3/.wav/.mp4/.mov.")
    upsert_media_parser.add_argument(
        "--media-type",
        choices=["audio", "video"],
        default=None,
        help="Optional override if extension inference is ambiguous.",
    )
    upsert_media_parser.add_argument(
        "--notes",
        default="",
        help="Short editorial context to aggregate with the media embedding.",
    )
    upsert_media_parser.add_argument(
        "--use-files-api",
        action="store_true",
        help="Upload the media through Gemini Files API instead of inline bytes.",
    )
    upsert_media_parser.add_argument(
        "--namespace",
        default=None,
        help="Optional namespace override.",
    )
    upsert_media_parser.add_argument(
        "--no-trim-long-media",
        action="store_true",
        help="Fail instead of auto-trimming media that exceeds Gemini MVP limits.",
    )

    search_parser = subparsers.add_parser(
        "search", help="Run semantic search against Pinecone using a text idea."
    )
    search_parser.add_argument("--query", required=True, help="Search idea or prompt.")
    search_parser.add_argument("--top-k", type=int, default=3, help="Number of nearest matches to return.")
    search_parser.add_argument(
        "--media-type",
        choices=["text", "audio", "video"],
        default=None,
        help="Optional Pinecone metadata filter.",
    )
    search_parser.add_argument(
        "--embedding-kind",
        choices=["asset", "text_chunk", "timeline_segment"],
        default=None,
        help="Optional filter for the embedding record type.",
    )
    search_parser.add_argument(
        "--selection-status",
        choices=["included", "excluded", "unset"],
        default=None,
        help="Optional Pinecone metadata filter for current curation state.",
    )
    search_parser.add_argument(
        "--cohort",
        default=None,
        help="Optional Pinecone metadata filter for curation cohort.",
    )
    search_parser.add_argument(
        "--subcohort",
        default=None,
        help="Optional Pinecone metadata filter for curation subcohort.",
    )
    search_parser.add_argument(
        "--rerank-feedback",
        action="store_true",
        help="Blend `feedback_score_v1` into ranking after the semantic search step.",
    )
    search_parser.add_argument(
        "--feedback-weight",
        type=float,
        default=0.15,
        help="How strongly feedback should influence reranking.",
    )
    search_parser.add_argument(
        "--facet-weight",
        type=float,
        default=0.18,
        help="How strongly structured-summary facet matching should influence reranking.",
    )
    search_parser.add_argument(
        "--no-diversify-assets",
        action="store_true",
        help="Allow multiple top results from the same asset instead of diversifying timeline results by asset.",
    )
    search_parser.add_argument(
        "--candidate-k",
        type=int,
        default=None,
        help="Initial Pinecone candidate pool before reranking.",
    )
    search_parser.add_argument(
        "--min-feedback-score",
        type=float,
        default=None,
        help="Optional lower bound for `feedback_score_v1` after retrieval.",
    )
    search_parser.add_argument(
        "--json-output",
        type=Path,
        default=None,
        help="Optional path to write structured search results as JSON.",
    )
    search_parser.add_argument(
        "--cross-encoder-rerank",
        action="store_true",
        help="Run a Gemini cross-encoder rerank step over the retrieved candidates.",
    )
    search_parser.add_argument(
        "--cross-encoder-top-k",
        type=int,
        default=12,
        help="How many top semantic candidates to send into the cross-encoder rerank step.",
    )
    search_parser.add_argument("--namespace", default=None, help="Optional namespace override.")

    smoke_parser = subparsers.add_parser(
        "smoke-test-embedding2",
        help="Call Gemini Embedding 2 directly for text/audio/video without Pinecone.",
    )
    smoke_parser.add_argument(
        "--text",
        default="Bankara Radio style smoke test: loud opening, fast pacing, sharp tsukkomi.",
        help="Text payload used for the text embedding test.",
    )
    smoke_parser.add_argument("--audio", type=Path, default=None, help="Optional local audio file to test.")
    smoke_parser.add_argument("--video", type=Path, default=None, help="Optional local video file to test.")
    smoke_parser.add_argument(
        "--use-files-api",
        action="store_true",
        help="Upload media through Gemini Files API instead of inline bytes.",
    )
    smoke_parser.add_argument(
        "--no-trim-long-media",
        action="store_true",
        help="Fail instead of auto-trimming media that exceeds preview limits.",
    )
    smoke_parser.add_argument(
        "--json-output",
        type=Path,
        default=None,
        help="Optional path to write structured smoke test results as JSON.",
    )

    ingest_dir_parser = subparsers.add_parser(
        "ingest-dir", help="Bulk-ingest supported text/audio/video files from a directory."
    )
    ingest_dir_parser.add_argument("--dir", type=Path, required=True, help="Directory to ingest.")
    ingest_dir_parser.add_argument(
        "--recursive",
        action="store_true",
        help="Recursively scan subdirectories.",
    )
    ingest_dir_parser.add_argument(
        "--use-files-api",
        action="store_true",
        help="Upload media files through Gemini Files API instead of inline bytes.",
    )
    ingest_dir_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional maximum number of files to ingest.",
    )
    ingest_dir_parser.add_argument(
        "--state-file",
        type=Path,
        default=Path(DEFAULT_STATE_FILE),
        help="JSON state file used to skip unchanged files across runs.",
    )
    ingest_dir_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List files that would be processed without calling Gemini or Pinecone.",
    )
    ingest_dir_parser.add_argument(
        "--force",
        action="store_true",
        help="Re-ingest files even if the state file says they are unchanged.",
    )
    ingest_dir_parser.add_argument(
        "--namespace",
        default=None,
        help="Optional namespace override.",
    )
    ingest_dir_parser.add_argument(
        "--no-trim-long-media",
        action="store_true",
        help="Fail instead of auto-trimming media that exceeds Gemini MVP limits.",
    )
    ingest_dir_parser.add_argument(
        "--report-output",
        type=Path,
        default=None,
        help="Optional JSONL report describing each processed file.",
    )

    ingest_manifest_parser = subparsers.add_parser(
        "ingest-manifest", help="Ingest JSONL manifest entries exported by the control plane."
    )
    ingest_manifest_parser.add_argument("--manifest", type=Path, required=True, help="Manifest JSONL file.")
    ingest_manifest_parser.add_argument(
        "--use-files-api",
        action="store_true",
        help="Upload media files through Gemini Files API instead of inline bytes.",
    )
    ingest_manifest_parser.add_argument(
        "--results-output",
        type=Path,
        default=None,
        help="Optional JSONL file to emit {asset_id, record_id} mapping rows.",
    )
    ingest_manifest_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional maximum number of manifest lines to process.",
    )
    ingest_manifest_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate the manifest without calling Gemini or Pinecone.",
    )
    ingest_manifest_parser.add_argument(
        "--namespace",
        default=None,
        help="Default namespace if a manifest line does not provide one.",
    )
    ingest_manifest_parser.add_argument(
        "--no-trim-long-media",
        action="store_true",
        help="Fail instead of auto-trimming media that exceeds Gemini MVP limits.",
    )
    ingest_manifest_parser.add_argument(
        "--report-output",
        type=Path,
        default=None,
        help="Optional JSONL report describing each processed manifest entry.",
    )

    return parser


def run(args: argparse.Namespace) -> None:
    if args.command == "ingest-dir" and args.dry_run:
        namespace = args.namespace or default_namespace_from_env()
        ingest_directory(
            client=None,
            index=None,
            namespace=namespace,
            root_dir=args.dir.expanduser().resolve(),
            recursive=args.recursive,
            use_files_api=args.use_files_api,
            limit=args.limit,
            state_file=args.state_file.expanduser().resolve(),
            dry_run=True,
            force=args.force,
            allow_trim_long_media=not args.no_trim_long_media,
            report_output=args.report_output.expanduser().resolve() if args.report_output else None,
        )
        return

    if args.command == "ingest-manifest" and args.dry_run:
        validate_manifest(
            manifest_path=args.manifest.expanduser().resolve(),
            limit=args.limit,
        )
        return

    settings = Settings.from_env()
    client: genai.Client | None = None
    index: Any | None = None
    namespace = getattr(args, "namespace", None) or settings.pinecone_namespace

    if args.command == "ensure-index":
        index = ensure_pinecone_index(settings)
        print(
            f"Index ready: {settings.pinecone_index_name} "
            f"({INDEX_DIMENSION} dims, metric={INDEX_METRIC}, namespace={namespace})"
        )
        return

    if args.command == "smoke-test-embedding2":
        client = create_genai_client(settings)
        smoke_test_embedding2(
            client=client,
            text=args.text,
            audio_path=args.audio.expanduser().resolve() if args.audio else None,
            video_path=args.video.expanduser().resolve() if args.video else None,
            use_files_api=args.use_files_api,
            allow_trim_long_media=not args.no_trim_long_media,
            output_path=args.json_output.expanduser().resolve() if args.json_output else None,
        )
        return

    client = create_genai_client(settings)
    index = ensure_pinecone_index(settings)

    if args.command == "upsert-text":
        record_id = args.record_id or f"text-{uuid.uuid4().hex}"
        vector = embed_text_document(client, title=args.title, text=args.text)
        upsert_embedding(
            index=index,
            namespace=namespace,
            record_id=record_id,
            vector=vector,
            metadata={
                "title": args.title,
                "media_type": "text",
                "embedding_kind": "text_chunk",
                "source_path": "",
                "relative_path": "",
                "notes": shorten_text(args.text, 500),
            },
        )
        print(f"Upserted text record: {record_id}")
        return

    if args.command == "upsert-media":
        file_path = args.file.expanduser().resolve()
        if not file_path.exists():
            raise FileNotFoundError(f"Media file not found: {file_path}")

        with tempfile.TemporaryDirectory(prefix="gemini_media_single_") as temp_dir_name:
            prepared_media = prepare_media_for_embedding(
                file_path=file_path,
                declared_media_type=args.media_type,
                allow_trim_long_media=not args.no_trim_long_media,
                temp_dir=Path(temp_dir_name),
            )
            notes_payload = args.notes
            transcript_excerpt = load_sidecar_transcript_excerpt(file_path)
            if transcript_excerpt:
                notes_payload = "\n".join(
                    line for line in [args.notes.strip(), f"transcript_excerpt: {transcript_excerpt}"] if line
                )

            record_id = args.record_id or f"media-{uuid.uuid4().hex}"
            vector = embed_media_document(
                client=client,
                title=args.title,
                prepared_media=prepared_media,
                notes=notes_payload,
                use_files_api=args.use_files_api,
            )
            upsert_embedding(
                index=index,
                namespace=namespace,
                record_id=record_id,
                vector=vector,
                metadata={
                    "title": args.title,
                    "media_type": prepared_media.media_type,
                    "embedding_kind": "asset",
                    "mime_type": prepared_media.mime_type,
                    "source_path": str(file_path),
                    "relative_path": file_path.name,
                    "notes": shorten_text(notes_payload, 500),
                    "source_duration_seconds": prepared_media.source_duration_seconds,
                    "embedded_duration_seconds": prepared_media.embed_duration_seconds,
                    "was_trimmed": prepared_media.was_trimmed,
                },
            )
        print(f"Upserted media record: {record_id}")
        return

    if args.command == "search":
        query_facets = build_query_facets(client, args.query, args.cohort, subcohort=args.subcohort)
        results = search_similar(
            client=client,
            index=index,
            namespace=namespace,
            query_text=args.query,
            top_k=args.top_k,
            media_type=args.media_type,
            embedding_kind=args.embedding_kind,
            selection_status=args.selection_status,
            cohort=args.cohort,
            subcohort=args.subcohort,
            rerank_by_feedback=args.rerank_feedback,
            feedback_weight=args.feedback_weight,
            facet_weight=args.facet_weight,
            query_facets=query_facets,
            diversify_by_asset=not args.no_diversify_assets,
            candidate_k=args.candidate_k,
            min_feedback_score=args.min_feedback_score,
            cross_encoder_rerank=args.cross_encoder_rerank,
            cross_encoder_top_k=args.cross_encoder_top_k,
        )
        if args.json_output:
            write_search_payload(
                args.json_output.expanduser().resolve(),
                build_search_payload(
                    query_text=args.query,
                    namespace=namespace,
                    media_type=args.media_type,
                    embedding_kind=args.embedding_kind,
                    selection_status=args.selection_status,
                    cohort=args.cohort,
                    subcohort=args.subcohort,
                    rerank_by_feedback=args.rerank_feedback,
                    feedback_weight=args.feedback_weight,
                    facet_weight=args.facet_weight,
                    candidate_k=args.candidate_k,
                    min_feedback_score=args.min_feedback_score,
                    cross_encoder_rerank=args.cross_encoder_rerank,
                    cross_encoder_top_k=args.cross_encoder_top_k,
                    query_facets=query_facets,
                    matches=results,
                ),
            )
        print_matches(results)
        return

    if args.command == "ingest-dir":
        ingest_directory(
            client=client,
            index=index,
            namespace=namespace,
            root_dir=args.dir.expanduser().resolve(),
            recursive=args.recursive,
            use_files_api=args.use_files_api,
            limit=args.limit,
            state_file=args.state_file.expanduser().resolve(),
            dry_run=args.dry_run,
            force=args.force,
            allow_trim_long_media=not args.no_trim_long_media,
            report_output=args.report_output.expanduser().resolve() if args.report_output else None,
        )
        return

    if args.command == "ingest-manifest":
        ingest_manifest(
            client=client,
            index=index,
            default_namespace=namespace,
            manifest_path=args.manifest.expanduser().resolve(),
            use_files_api=args.use_files_api,
            allow_trim_long_media=not args.no_trim_long_media,
            results_output=args.results_output.expanduser().resolve() if args.results_output else None,
            limit=args.limit,
            report_output=args.report_output.expanduser().resolve() if args.report_output else None,
        )
        return

    raise RuntimeError(f"Unsupported command: {args.command}")


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        run(args)
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
