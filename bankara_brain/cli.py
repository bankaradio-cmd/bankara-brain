"""Bankara Brain CLI — command-line interface for Brain-only operations.

This module owns the argparse parser, command dispatcher, and main entry point
for Brain data-accumulation / analysis / provision commands (37 commands).

Consumer commands (script-assistant: brief, draft, review, etc.) live in
``bankara_script_assistant.cli`` and are **not** imported here.

For backward-compatible access to all 45 commands, use
``bankara_brain_control_plane.py`` which calls ``build_parser()`` /
``run()`` with the ``extra_commands`` / ``fallback_dispatcher`` callbacks.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from bankara_brain.logging_config import setup_logging

# ── Already-extracted bankara_brain modules ───────────────────────────────────
from bankara_brain.db import AppConfig, BlobStore, init_db
from bankara_brain.ingest.stage import stage_dataset
from bankara_brain.utils import parse_date_value as parse_date
from bankara_brain.youtube.auth import auth_youtube
from bankara_brain.youtube.data_api import youtube_whoami

# ── Business logic from bankara_brain submodules ─────────────────────────────
from bankara_brain.pipelines import run_corpus_cycle, run_maintenance_pipeline
from bankara_brain.ingest.pipeline import run_ingest_pipeline
from bankara_brain.corpus.curation import (
    list_assets,
    corpus_status,
    curate_assets,
    auto_curate_bankara_assets,
    auto_assign_cohorts,
    audit_assets,
    quarantine_assets,
    DEFAULT_BANKARA_CHANNEL,
    DEFAULT_COMEDY_INCLUDE_KEYWORDS,
    DEFAULT_COMEDY_EXCLUDE_KEYWORDS,
)
from bankara_brain.embedding.sync import purge_embeddings, sync_embedding_metadata
from bankara_brain.maintenance import doctor, repair_assets
from bankara_brain.youtube.linking import list_youtube_videos, link_youtube_assets
from bankara_brain.youtube.public import (
    list_public_youtube_videos,
    download_public_youtube_videos,
    DEFAULT_BANKARA_PUBLIC_CHANNEL_URL,
    DEFAULT_BANKARA_PUBLIC_EXCLUDE_KEYWORDS,
)
from bankara_brain.analysis.enrichment import (
    enrich_structured_summaries,
    enrich_visual_audio_summaries,
    DEFAULT_GENERATION_MODEL,
)
from bankara_brain.embedding.manifest import export_embedding_manifest, import_embedding_results
from bankara_brain.corpus.timeline import (
    import_shot_timeline,
    list_timeline_segments,
    bootstrap_shot_timeline,
)
from bankara_brain.youtube.sync import import_analytics_csv, sync_youtube_analytics
from bankara_brain.analysis.scoring import (
    score_feedback,
    run_feedback_pipeline,
    list_feedback_scores,
    recommend_feedback_patterns,
    feedback_diagnostics,
)
from bankara_brain.embedding.benchmark import run_retrieval_benchmark


def build_parser(
    *,
    extra_commands: "Callable[[argparse._SubParsersAction], None] | None" = None,
) -> argparse.ArgumentParser:
    """Build the Brain CLI argument parser.

    Parameters
    ----------
    extra_commands:
        Optional callback that receives the *subparsers* action and may
        register additional sub-commands (e.g. consumer commands from
        ``bankara_script_assistant``).
    """
    parser = argparse.ArgumentParser(
        description="Bankara Brain CLI: relational catalog, object store, analytics",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init-db", help="Create database tables.")

    stage_parser = subparsers.add_parser("stage-dataset", help="Stage source files into object storage and DB.")
    stage_parser.add_argument("--dir", type=Path, required=True, help="Dataset directory to ingest.")
    stage_parser.add_argument("--recursive", action="store_true", help="Recursively scan subdirectories.")
    stage_parser.add_argument("--copy-mode", choices=["hardlink", "copy", "symlink"], default="hardlink")
    stage_parser.add_argument("--force", action="store_true", help="Restage changed files even if fingerprints match.")
    stage_parser.add_argument("--limit", type=int, default=None, help="Optional maximum number of files.")

    ingest_pipeline_parser = subparsers.add_parser(
        "run-ingest-pipeline",
        help="Stage data, bootstrap timelines, export a filtered manifest, ingest embeddings, and import results.",
    )
    ingest_pipeline_parser.add_argument("--dir", type=Path, required=True, help="Dataset directory to stage.")
    ingest_pipeline_parser.add_argument(
        "--out-dir",
        type=Path,
        required=True,
        help="Directory for generated manifest/results/report artifacts.",
    )
    ingest_pipeline_parser.add_argument("--recursive", action="store_true", help="Recursively scan subdirectories.")
    ingest_pipeline_parser.add_argument("--copy-mode", choices=["hardlink", "copy", "symlink"], default="hardlink")
    ingest_pipeline_parser.add_argument(
        "--force-stage",
        action="store_true",
        help="Restage changed files even if fingerprints match.",
    )
    ingest_pipeline_parser.add_argument(
        "--replace-bootstrap",
        action="store_true",
        help="Replace existing rough timeline segments.",
    )
    ingest_pipeline_parser.add_argument(
        "--skip-bootstrap-timeline",
        action="store_true",
        help="Skip rough timeline generation before export.",
    )
    ingest_pipeline_parser.add_argument("--max-segment-seconds", type=float, default=5.0)
    ingest_pipeline_parser.add_argument("--min-segment-seconds", type=float, default=1.5)
    ingest_pipeline_parser.add_argument("--gap-seconds", type=float, default=1.0)
    ingest_pipeline_parser.add_argument("--target-chars", type=int, default=180)
    ingest_pipeline_parser.add_argument("--limit", type=int, default=None, help="Optional maximum number of assets/manifest rows.")
    ingest_pipeline_parser.add_argument("--namespace", default=os.getenv("PINECONE_NAMESPACE", "bankara-radio"))
    ingest_pipeline_parser.add_argument("--only-missing-embeddings", action="store_true")
    ingest_pipeline_parser.add_argument("--use-files-api", action="store_true")
    ingest_pipeline_parser.add_argument("--no-trim-long-media", action="store_true")
    ingest_pipeline_parser.add_argument(
        "--embedding-python",
        type=Path,
        default=None,
        help="Python executable used to run the embedding pipeline.",
    )
    ingest_pipeline_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate the generated manifest without live embedding upserts.",
    )
    ingest_pipeline_parser.add_argument("--channel", default=None, help="Only export assets for this channel.")
    ingest_pipeline_parser.add_argument(
        "--selection-status",
        choices=["included", "excluded", "unset"],
        default=None,
        help="Only export assets with this persisted curation state.",
    )
    ingest_pipeline_parser.add_argument(
        "--require-tag",
        action="append",
        dest="require_tags",
        default=[],
        help="Only export assets whose metadata tags include this value. Repeatable.",
    )
    ingest_pipeline_parser.add_argument(
        "--exclude-tag",
        action="append",
        dest="exclude_tags",
        default=[],
        help="Skip assets whose metadata tags include this value. Repeatable.",
    )
    ingest_pipeline_parser.add_argument(
        "--title-contains",
        action="append",
        default=[],
        help="Only export assets whose title contains this text. Repeatable.",
    )
    ingest_pipeline_parser.add_argument(
        "--source-url-contains",
        action="append",
        default=[],
        help="Only export assets whose source_url contains this text. Repeatable.",
    )

    maintenance_parser = subparsers.add_parser(
        "run-maintenance-pipeline",
        help="Repair filtered assets, audit remaining gaps, and ingest only missing embeddings.",
    )
    maintenance_parser.add_argument(
        "--out-dir",
        type=Path,
        required=True,
        help="Directory for repair reports, manifests, and embedding artifacts.",
    )
    maintenance_parser.add_argument("--asset", default=None)
    maintenance_parser.add_argument("--media-type", choices=["text", "audio", "video"], default=None)
    maintenance_parser.add_argument("--channel", default=None)
    maintenance_parser.add_argument("--selection-status", choices=["included", "excluded", "unset"], default=None)
    maintenance_parser.add_argument("--require-tag", action="append", dest="require_tags", default=[])
    maintenance_parser.add_argument("--exclude-tag", action="append", dest="exclude_tags", default=[])
    maintenance_parser.add_argument("--title-contains", action="append", default=[])
    maintenance_parser.add_argument("--source-url-contains", action="append", default=[])
    maintenance_parser.add_argument("--limit", type=int, default=None)
    maintenance_parser.add_argument("--skip-duration-repair", action="store_true")
    maintenance_parser.add_argument("--skip-transcribe", action="store_true")
    maintenance_parser.add_argument("--force-transcribe", action="store_true")
    maintenance_parser.add_argument("--transcribe-script", type=Path, default=None)
    maintenance_parser.add_argument("--transcribe-language", default=None)
    maintenance_parser.add_argument("--transcribe-model", default=None)
    maintenance_parser.add_argument("--work-dir", type=Path, default=None)
    maintenance_parser.add_argument("--skip-bootstrap-timeline", action="store_true")
    maintenance_parser.add_argument("--replace-timeline", action="store_true")
    maintenance_parser.add_argument("--max-segment-seconds", type=float, default=5.0)
    maintenance_parser.add_argument("--min-segment-seconds", type=float, default=1.5)
    maintenance_parser.add_argument("--gap-seconds", type=float, default=1.0)
    maintenance_parser.add_argument("--target-chars", type=int, default=180)
    maintenance_parser.add_argument("--namespace", default=os.getenv("PINECONE_NAMESPACE", "bankara-radio"))
    maintenance_parser.set_defaults(only_missing_embeddings=True)
    maintenance_parser.add_argument(
        "--all-embeddings",
        dest="only_missing_embeddings",
        action="store_false",
        help="Re-export all embeddings instead of only missing records.",
    )
    maintenance_parser.add_argument("--use-files-api", action="store_true")
    maintenance_parser.add_argument("--no-trim-long-media", action="store_true")
    maintenance_parser.add_argument("--embedding-python", type=Path, default=None)
    maintenance_parser.add_argument("--dry-run", action="store_true")

    corpus_cycle_parser = subparsers.add_parser(
        "run-corpus-cycle",
        help="Quarantine optional problem assets, run maintenance, then optionally sync/score feedback and print corpus status.",
    )
    corpus_cycle_parser.add_argument("--out-dir", type=Path, required=True)
    corpus_cycle_parser.add_argument("--asset", default=None)
    corpus_cycle_parser.add_argument("--media-type", choices=["text", "audio", "video"], default=None)
    corpus_cycle_parser.add_argument("--channel", default=None)
    corpus_cycle_parser.add_argument("--selection-status", choices=["included", "excluded", "unset"], default=None)
    corpus_cycle_parser.add_argument("--require-tag", action="append", dest="require_tags", default=[])
    corpus_cycle_parser.add_argument("--exclude-tag", action="append", dest="exclude_tags", default=[])
    corpus_cycle_parser.add_argument("--title-contains", action="append", default=[])
    corpus_cycle_parser.add_argument("--source-url-contains", action="append", default=[])
    corpus_cycle_parser.add_argument("--limit", type=int, default=None)
    corpus_cycle_parser.add_argument("--quarantine-problem", action="append", dest="quarantine_problem_filters", default=[])
    corpus_cycle_parser.add_argument(
        "--quarantine-severity",
        action="append",
        dest="quarantine_severity_filters",
        choices=["blocker", "warning"],
        default=[],
    )
    corpus_cycle_parser.add_argument("--quarantine-cohort", default="quarantine")
    corpus_cycle_parser.add_argument("--quarantine-reason-prefix", default="auto-quarantine")
    corpus_cycle_parser.add_argument("--skip-duration-repair", action="store_true")
    corpus_cycle_parser.add_argument("--skip-transcribe", action="store_true")
    corpus_cycle_parser.add_argument("--force-transcribe", action="store_true")
    corpus_cycle_parser.add_argument("--transcribe-script", type=Path, default=None)
    corpus_cycle_parser.add_argument("--transcribe-language", default=None)
    corpus_cycle_parser.add_argument("--transcribe-model", default=None)
    corpus_cycle_parser.add_argument("--work-dir", type=Path, default=None)
    corpus_cycle_parser.add_argument("--skip-bootstrap-timeline", action="store_true")
    corpus_cycle_parser.add_argument("--replace-timeline", action="store_true")
    corpus_cycle_parser.add_argument("--max-segment-seconds", type=float, default=5.0)
    corpus_cycle_parser.add_argument("--min-segment-seconds", type=float, default=1.5)
    corpus_cycle_parser.add_argument("--gap-seconds", type=float, default=1.0)
    corpus_cycle_parser.add_argument("--target-chars", type=int, default=180)
    corpus_cycle_parser.add_argument("--namespace", default=os.getenv("PINECONE_NAMESPACE", "bankara-radio"))
    corpus_cycle_parser.set_defaults(only_missing_embeddings=True)
    corpus_cycle_parser.add_argument(
        "--all-embeddings",
        dest="only_missing_embeddings",
        action="store_false",
        help="Re-export all embeddings instead of only missing records.",
    )
    corpus_cycle_parser.add_argument("--use-files-api", action="store_true")
    corpus_cycle_parser.add_argument("--no-trim-long-media", action="store_true")
    corpus_cycle_parser.add_argument("--embedding-python", type=Path, default=None)
    corpus_cycle_parser.add_argument("--feedback-start-date", type=parse_date, default=None)
    corpus_cycle_parser.add_argument("--feedback-end-date", type=parse_date, default=None)
    corpus_cycle_parser.add_argument("--overwrite-feedback", action="store_true")
    corpus_cycle_parser.add_argument("--skip-feedback-sync", action="store_true")
    corpus_cycle_parser.add_argument("--require-feedback", action="store_true")
    corpus_cycle_parser.add_argument("--skip-metadata-sync", action="store_true")
    corpus_cycle_parser.add_argument("--auto-link-youtube-assets", action="store_true")
    corpus_cycle_parser.add_argument("--dry-run", action="store_true")

    list_parser = subparsers.add_parser("list-assets", help="List catalogued assets.")
    list_parser.add_argument("--media-type", choices=["text", "audio", "video"], default=None)
    list_parser.add_argument("--limit", type=int, default=100)
    list_parser.add_argument("--channel", default=None)
    list_parser.add_argument("--selection-status", choices=["included", "excluded", "unset"], default=None)
    list_parser.add_argument("--cohort", default=None)
    list_parser.add_argument("--subcohort", default=None)
    list_parser.add_argument("--require-tag", action="append", dest="require_tags", default=[])
    list_parser.add_argument("--exclude-tag", action="append", dest="exclude_tags", default=[])
    list_parser.add_argument("--title-contains", action="append", default=[])
    list_parser.add_argument("--source-url-contains", action="append", default=[])

    corpus_status_parser = subparsers.add_parser(
        "corpus-status",
        help="Show corpus coverage and embedding/feedback progress.",
    )
    corpus_status_parser.add_argument("--channel", default=None)
    corpus_status_parser.add_argument("--selection-status", choices=["included", "excluded", "unset"], default=None)
    corpus_status_parser.add_argument("--cohort", default=None)
    corpus_status_parser.add_argument("--subcohort", default=None)

    curate_parser = subparsers.add_parser(
        "curate-assets",
        help="Persist include/exclude selection for assets that match the provided filters.",
    )
    curate_parser.add_argument(
        "--selection-status",
        choices=["included", "excluded", "unset"],
        required=True,
    )
    curate_parser.add_argument("--media-type", choices=["text", "audio", "video"], default=None)
    curate_parser.add_argument("--channel", default=None)
    curate_parser.add_argument("--require-tag", action="append", dest="require_tags", default=[])
    curate_parser.add_argument("--exclude-tag", action="append", dest="exclude_tags", default=[])
    curate_parser.add_argument("--title-contains", action="append", default=[])
    curate_parser.add_argument("--source-url-contains", action="append", default=[])
    curate_parser.add_argument("--cohort", default="")
    curate_parser.add_argument("--reason", default="")
    curate_parser.add_argument("--limit", type=int, default=None)
    curate_parser.add_argument("--dry-run", action="store_true")

    auto_curate_parser = subparsers.add_parser(
        "auto-curate-bankara",
        help="Heuristically mark Bankara comedy assets as included/excluded.",
    )
    auto_curate_parser.add_argument("--target-channel", default=DEFAULT_BANKARA_CHANNEL)
    auto_curate_parser.add_argument("--include-keyword", action="append", dest="include_keywords", default=[])
    auto_curate_parser.add_argument("--exclude-keyword", action="append", dest="exclude_keywords", default=[])
    auto_curate_parser.add_argument("--include-threshold", type=float, default=3.0)
    auto_curate_parser.add_argument("--exclude-threshold", type=float, default=-2.0)
    auto_curate_parser.add_argument("--media-type", choices=["text", "audio", "video"], default=None)
    auto_curate_parser.add_argument("--selection-status", choices=["included", "excluded", "unset"], default="unset")
    auto_curate_parser.add_argument("--cohort", default="bankara-comedy")
    auto_curate_parser.add_argument("--reason-prefix", default="auto-curate-bankara")
    auto_curate_parser.add_argument("--limit", type=int, default=None)
    auto_curate_parser.add_argument("--dry-run", action="store_true")

    audit_parser = subparsers.add_parser(
        "audit-assets",
        help="Audit catalogued assets for missing metadata, embeddings, timelines, and feedback.",
    )
    audit_parser.add_argument("--media-type", choices=["text", "audio", "video"], default=None)
    audit_parser.add_argument("--channel", default=None)
    audit_parser.add_argument("--selection-status", choices=["included", "excluded", "unset"], default=None)
    audit_parser.add_argument("--cohort", default=None)
    audit_parser.add_argument("--require-tag", action="append", dest="require_tags", default=[])
    audit_parser.add_argument("--exclude-tag", action="append", dest="exclude_tags", default=[])
    audit_parser.add_argument("--title-contains", action="append", default=[])
    audit_parser.add_argument("--source-url-contains", action="append", default=[])
    audit_parser.add_argument("--only-problems", action="store_true")
    audit_parser.add_argument("--only-blockers", action="store_true")
    audit_parser.add_argument("--only-warnings", action="store_true")
    audit_parser.add_argument("--limit", type=int, default=500)
    audit_parser.add_argument("--json-output", type=Path, default=None)
    audit_parser.add_argument("--summary-output", type=Path, default=None)

    quarantine_parser = subparsers.add_parser(
        "quarantine-assets",
        help="Mark problem assets as excluded so they stop flowing into the comedy corpus.",
    )
    quarantine_parser.add_argument("--media-type", choices=["text", "audio", "video"], default=None)
    quarantine_parser.add_argument("--channel", default=None)
    quarantine_parser.add_argument("--selection-status", choices=["included", "excluded", "unset"], default=None)
    quarantine_parser.add_argument("--require-tag", action="append", dest="require_tags", default=[])
    quarantine_parser.add_argument("--exclude-tag", action="append", dest="exclude_tags", default=[])
    quarantine_parser.add_argument("--title-contains", action="append", default=[])
    quarantine_parser.add_argument("--source-url-contains", action="append", default=[])
    quarantine_parser.add_argument("--problem", action="append", dest="problem_filters", default=[])
    quarantine_parser.add_argument("--severity", action="append", choices=["blocker", "warning"], default=[])
    quarantine_parser.add_argument("--cohort", default="quarantine")
    quarantine_parser.add_argument("--reason-prefix", default="auto-quarantine")
    quarantine_parser.add_argument("--limit", type=int, default=None)
    quarantine_parser.add_argument("--dry-run", action="store_true")

    purge_parser = subparsers.add_parser(
        "purge-embeddings",
        help="Delete Pinecone vectors and local embedding records for assets that no longer belong in the active corpus.",
    )
    purge_parser.add_argument("--asset", default=None)
    purge_parser.add_argument("--media-type", choices=["text", "audio", "video"], default=None)
    purge_parser.add_argument("--channel", default=None)
    purge_parser.add_argument(
        "--selection-status",
        choices=["included", "excluded", "unset"],
        default="excluded",
        help="Default keeps purge focused on vectors that were intentionally excluded from the corpus.",
    )
    purge_parser.add_argument("--require-tag", action="append", dest="require_tags", default=[])
    purge_parser.add_argument("--exclude-tag", action="append", dest="exclude_tags", default=[])
    purge_parser.add_argument("--title-contains", action="append", default=[])
    purge_parser.add_argument("--source-url-contains", action="append", default=[])
    purge_parser.add_argument("--namespace", default=None, help="Restrict purge to a single Pinecone namespace.")
    purge_parser.add_argument("--limit", type=int, default=None)
    purge_parser.add_argument("--dry-run", action="store_true")
    purge_parser.add_argument("--report-output", type=Path, default=None)

    sync_metadata_parser = subparsers.add_parser(
        "sync-embedding-metadata",
        help="Refresh Pinecone metadata from the latest local asset/feedback/curation state without re-embedding.",
    )
    sync_metadata_parser.add_argument("--asset", default=None)
    sync_metadata_parser.add_argument("--media-type", choices=["text", "audio", "video"], default=None)
    sync_metadata_parser.add_argument("--channel", default=None)
    sync_metadata_parser.add_argument("--selection-status", choices=["included", "excluded", "unset"], default=None)
    sync_metadata_parser.add_argument("--cohort", default=None)
    sync_metadata_parser.add_argument("--subcohort", default=None)
    sync_metadata_parser.add_argument("--require-tag", action="append", dest="require_tags", default=[])
    sync_metadata_parser.add_argument("--exclude-tag", action="append", dest="exclude_tags", default=[])
    sync_metadata_parser.add_argument("--title-contains", action="append", default=[])
    sync_metadata_parser.add_argument("--source-url-contains", action="append", default=[])
    sync_metadata_parser.add_argument("--namespace", default=None)
    sync_metadata_parser.add_argument("--limit", type=int, default=None)
    sync_metadata_parser.add_argument("--dry-run", action="store_true")
    sync_metadata_parser.add_argument("--report-output", type=Path, default=None)

    doctor_parser = subparsers.add_parser(
        "doctor",
        help="Check runtime/API/OAuth/transcription prerequisites.",
    )
    doctor_parser.add_argument("--json-output", type=Path, default=None)

    auth_youtube_parser = subparsers.add_parser(
        "auth-youtube",
        help="Run the YouTube OAuth browser flow and persist the token.",
    )
    auth_youtube_parser.add_argument("--force-reauth", action="store_true")

    youtube_whoami_parser = subparsers.add_parser(
        "youtube-whoami",
        help="Show which YouTube channel is currently authorized by the OAuth token.",
    )
    youtube_whoami_parser.add_argument("--json-output", type=Path, default=None)

    youtube_list_parser = subparsers.add_parser(
        "list-youtube-videos",
        help="List videos from the OAuth-authorized YouTube channel uploads playlist.",
    )
    youtube_list_parser.add_argument("--limit", type=int, default=50)
    youtube_list_parser.add_argument("--title-contains", action="append", default=[])
    youtube_list_parser.add_argument("--json-output", type=Path, default=None)

    youtube_link_parser = subparsers.add_parser(
        "link-youtube-assets",
        help="Link local assets to authorized-channel YouTube videos via manual id or safe exact title matches.",
    )
    youtube_link_parser.add_argument("--asset", default=None)
    youtube_link_parser.add_argument("--video-id", default=None, help="Manual YouTube video id for a single asset.")
    youtube_link_parser.add_argument("--media-type", choices=["text", "audio", "video"], default=None)
    youtube_link_parser.add_argument("--channel", default=None)
    youtube_link_parser.add_argument("--selection-status", choices=["included", "excluded", "unset"], default=None)
    youtube_link_parser.add_argument("--cohort", default=None)
    youtube_link_parser.add_argument("--subcohort", default=None)
    youtube_link_parser.add_argument("--require-tag", action="append", dest="require_tags", default=[])
    youtube_link_parser.add_argument("--exclude-tag", action="append", dest="exclude_tags", default=[])
    youtube_link_parser.add_argument("--title-contains", action="append", default=[])
    youtube_link_parser.add_argument("--source-url-contains", action="append", default=[])
    youtube_link_parser.add_argument("--limit", type=int, default=None, help="Asset-side processing limit.")
    youtube_link_parser.add_argument("--catalog-limit", type=int, default=1000, help="How many channel videos to inspect.")
    youtube_link_parser.add_argument("--dry-run", action="store_true")
    youtube_link_parser.add_argument("--report-output", type=Path, default=None)

    public_youtube_list_parser = subparsers.add_parser(
        "list-public-youtube-videos",
        help="List public YouTube channel videos via yt-dlp flat-playlist metadata.",
    )
    public_youtube_list_parser.add_argument("--channel-url", default=DEFAULT_BANKARA_PUBLIC_CHANNEL_URL)
    public_youtube_list_parser.add_argument("--limit", type=int, default=50)
    public_youtube_list_parser.add_argument("--title-contains", action="append", default=[])
    public_youtube_list_parser.add_argument("--include-keyword", action="append", dest="include_keywords", default=[])
    public_youtube_list_parser.add_argument(
        "--exclude-keyword",
        action="append",
        dest="exclude_keywords",
        default=list(DEFAULT_BANKARA_PUBLIC_EXCLUDE_KEYWORDS),
    )
    public_youtube_list_parser.add_argument("--json-output", type=Path, default=None)

    public_youtube_download_parser = subparsers.add_parser(
        "download-public-youtube-videos",
        help="Download public YouTube videos with yt-dlp and emit sidecar metadata for stage-dataset.",
    )
    public_youtube_download_parser.add_argument("--channel-url", default=DEFAULT_BANKARA_PUBLIC_CHANNEL_URL)
    public_youtube_download_parser.add_argument("--out-dir", type=Path, required=True)
    public_youtube_download_parser.add_argument("--video-id", action="append", dest="video_ids", default=[])
    public_youtube_download_parser.add_argument("--title-contains", action="append", default=[])
    public_youtube_download_parser.add_argument("--include-keyword", action="append", dest="include_keywords", default=[])
    public_youtube_download_parser.add_argument(
        "--exclude-keyword",
        action="append",
        dest="exclude_keywords",
        default=list(DEFAULT_BANKARA_PUBLIC_EXCLUDE_KEYWORDS),
    )
    public_youtube_download_parser.add_argument("--limit", type=int, default=None)
    public_youtube_download_parser.add_argument("--sub-langs", default="ja.*,ja")
    public_youtube_download_parser.add_argument("--dry-run", action="store_true")
    public_youtube_download_parser.add_argument("--report-output", type=Path, default=None)

    repair_parser = subparsers.add_parser(
        "repair-assets",
        help="Repair missing media duration, transcript, and rough timeline for filtered assets.",
    )
    repair_parser.add_argument("--asset", default=None)
    repair_parser.add_argument("--media-type", choices=["text", "audio", "video"], default=None)
    repair_parser.add_argument("--channel", default=None)
    repair_parser.add_argument("--selection-status", choices=["included", "excluded", "unset"], default=None)
    repair_parser.add_argument("--require-tag", action="append", dest="require_tags", default=[])
    repair_parser.add_argument("--exclude-tag", action="append", dest="exclude_tags", default=[])
    repair_parser.add_argument("--title-contains", action="append", default=[])
    repair_parser.add_argument("--source-url-contains", action="append", default=[])
    repair_parser.add_argument("--limit", type=int, default=None)
    repair_parser.add_argument("--skip-duration-repair", action="store_true")
    repair_parser.add_argument("--skip-transcribe", action="store_true")
    repair_parser.add_argument("--force-transcribe", action="store_true")
    repair_parser.add_argument("--transcribe-script", type=Path, default=None)
    repair_parser.add_argument("--transcribe-language", default=None)
    repair_parser.add_argument("--transcribe-model", default=None)
    repair_parser.add_argument("--work-dir", type=Path, default=None)
    repair_parser.add_argument("--skip-bootstrap-timeline", action="store_true")
    repair_parser.add_argument("--replace-timeline", action="store_true")
    repair_parser.add_argument("--max-segment-seconds", type=float, default=5.0)
    repair_parser.add_argument("--min-segment-seconds", type=float, default=1.5)
    repair_parser.add_argument("--gap-seconds", type=float, default=1.0)
    repair_parser.add_argument("--target-chars", type=int, default=180)
    repair_parser.add_argument("--dry-run", action="store_true")
    repair_parser.add_argument("--report-output", type=Path, default=None)

    summary_enrichment_parser = subparsers.add_parser(
        "enrich-structured-summaries",
        help="Generate concise structured episode summaries and persist them into asset metadata_json.",
    )
    summary_enrichment_parser.add_argument("--asset", default=None)
    summary_enrichment_parser.add_argument("--media-type", choices=["text", "audio", "video"], default=None)
    summary_enrichment_parser.add_argument("--channel", default=None)
    summary_enrichment_parser.add_argument("--selection-status", choices=["included", "excluded", "unset"], default=None)
    summary_enrichment_parser.add_argument("--cohort", default=None)
    summary_enrichment_parser.add_argument("--subcohort", default=None)
    summary_enrichment_parser.add_argument("--require-tag", action="append", dest="require_tags", default=[])
    summary_enrichment_parser.add_argument("--exclude-tag", action="append", dest="exclude_tags", default=[])
    summary_enrichment_parser.add_argument("--title-contains", action="append", default=[])
    summary_enrichment_parser.add_argument("--source-url-contains", action="append", default=[])
    summary_enrichment_parser.add_argument("--limit", type=int, default=None)
    summary_enrichment_parser.add_argument("--overwrite", action="store_true")
    summary_enrichment_parser.add_argument(
        "--model",
        default=os.getenv("BANKARA_GENERATION_MODEL", DEFAULT_GENERATION_MODEL),
    )
    summary_enrichment_parser.add_argument("--temperature", type=float, default=0.2)
    summary_enrichment_parser.add_argument("--dry-run", action="store_true")
    summary_enrichment_parser.add_argument("--report-output", type=Path, default=None)

    vas_parser = subparsers.add_parser(
        "enrich-visual-audio-summaries",
        help="Generate shot-based visual+audio summaries for video assets using Gemini multimodal.",
    )
    vas_parser.add_argument("--asset", default=None)
    vas_parser.add_argument("--media-type", choices=["text", "audio", "video"], default="video")
    vas_parser.add_argument("--channel", default=None)
    vas_parser.add_argument("--selection-status", choices=["included", "excluded", "unset"], default=None)
    vas_parser.add_argument("--cohort", default=None)
    vas_parser.add_argument("--subcohort", default=None)
    vas_parser.add_argument("--require-tag", action="append", dest="require_tags", default=[])
    vas_parser.add_argument("--exclude-tag", action="append", dest="exclude_tags", default=[])
    vas_parser.add_argument("--title-contains", action="append", default=[])
    vas_parser.add_argument("--source-url-contains", action="append", default=[])
    vas_parser.add_argument("--limit", type=int, default=None)
    vas_parser.add_argument("--overwrite", action="store_true")
    vas_parser.add_argument(
        "--model",
        default=os.getenv("BANKARA_GENERATION_MODEL", DEFAULT_GENERATION_MODEL),
    )
    vas_parser.add_argument("--temperature", type=float, default=0.7)
    vas_parser.add_argument("--scene-threshold", type=float, default=0.3,
                            help="ffmpeg scene change threshold (0-1, lower = more sensitive)")
    vas_parser.add_argument("--dry-run", action="store_true")
    vas_parser.add_argument("--report-output", type=Path, default=None)

    manifest_parser = subparsers.add_parser(
        "export-embedding-manifest",
        help="Export JSONL entries that the embedding pipeline can ingest.",
    )
    manifest_parser.add_argument("--out", type=Path, required=True, help="Output JSONL file.")
    manifest_parser.add_argument("--namespace", default=os.getenv("PINECONE_NAMESPACE", "bankara-radio"))
    manifest_parser.add_argument("--limit", type=int, default=None)
    manifest_parser.add_argument("--only-missing-embeddings", action="store_true")
    manifest_parser.add_argument("--channel", default=None, help="Only export assets for this channel.")
    manifest_parser.add_argument(
        "--selection-status",
        choices=["included", "excluded", "unset"],
        default=None,
        help="Only export assets with this persisted curation state.",
    )
    manifest_parser.add_argument("--cohort", default=None, help="Only export assets in this curation cohort.")
    manifest_parser.add_argument("--subcohort", default=None, help="Only export assets in this curation subcohort.")
    manifest_parser.add_argument(
        "--require-tag",
        action="append",
        dest="require_tags",
        default=[],
        help="Only export assets whose metadata tags include this value. Repeatable.",
    )
    manifest_parser.add_argument(
        "--exclude-tag",
        action="append",
        dest="exclude_tags",
        default=[],
        help="Skip assets whose metadata tags include this value. Repeatable.",
    )
    manifest_parser.add_argument(
        "--title-contains",
        action="append",
        default=[],
        help="Only export assets whose title contains this text. Repeatable.",
    )
    manifest_parser.add_argument(
        "--source-url-contains",
        action="append",
        default=[],
        help="Only export assets whose source_url contains this text. Repeatable.",
    )

    results_parser = subparsers.add_parser(
        "import-embedding-results",
        help="Import embedding upsert results emitted by the embedding pipeline.",
    )
    results_parser.add_argument("--results", type=Path, required=True, help="Results JSONL file.")

    timeline_import_parser = subparsers.add_parser(
        "import-shot-timeline",
        help="Import a shot/beat timeline JSON or CSV for an asset.",
    )
    timeline_import_parser.add_argument("--asset", required=True, help="Asset id, relative path, or youtube video id.")
    timeline_import_parser.add_argument("--timeline", type=Path, required=True, help="Timeline .json or .csv file.")
    timeline_import_parser.add_argument(
        "--append",
        action="store_true",
        help="Append to existing timeline instead of replacing it.",
    )

    timeline_list_parser = subparsers.add_parser(
        "list-shot-timeline",
        help="List imported timeline segments for an asset.",
    )
    timeline_list_parser.add_argument("--asset", required=True, help="Asset id, relative path, or youtube video id.")
    timeline_list_parser.add_argument("--limit", type=int, default=200)

    timeline_bootstrap_parser = subparsers.add_parser(
        "bootstrap-shot-timeline",
        help="Generate a rough shot/beat timeline from transcript chunks.",
    )
    timeline_bootstrap_parser.add_argument(
        "--asset",
        default=None,
        help="Optional asset id, relative path, or youtube video id. Defaults to all audio/video assets.",
    )
    timeline_bootstrap_parser.add_argument(
        "--replace",
        action="store_true",
        help="Replace existing timeline segments if they already exist.",
    )
    timeline_bootstrap_parser.add_argument("--max-segment-seconds", type=float, default=5.0)
    timeline_bootstrap_parser.add_argument("--min-segment-seconds", type=float, default=1.5)
    timeline_bootstrap_parser.add_argument("--gap-seconds", type=float, default=1.0)
    timeline_bootstrap_parser.add_argument("--target-chars", type=int, default=180)

    analytics_csv_parser = subparsers.add_parser(
        "import-analytics-csv",
        help="Import YouTube analytics from CSV.",
    )
    analytics_csv_parser.add_argument("--csv", type=Path, required=True)
    analytics_csv_parser.add_argument("--report-kind", choices=["daily", "retention"], required=True)
    analytics_csv_parser.add_argument("--video-id", default=None)
    analytics_csv_parser.add_argument("--start-date", type=parse_date, default=None)
    analytics_csv_parser.add_argument("--end-date", type=parse_date, default=None)

    analytics_sync_parser = subparsers.add_parser(
        "sync-youtube-analytics",
        help="Fetch daily metrics and retention curves from YouTube Analytics API.",
    )
    analytics_sync_parser.add_argument("--asset", default=None)
    analytics_sync_parser.add_argument("--video-id", action="append", dest="video_ids", default=[])
    analytics_sync_parser.add_argument("--start-date", type=parse_date, required=True)
    analytics_sync_parser.add_argument("--end-date", type=parse_date, required=True)
    analytics_sync_parser.add_argument("--channel", default=None)
    analytics_sync_parser.add_argument("--selection-status", choices=["included", "excluded", "unset"], default=None)
    analytics_sync_parser.add_argument("--cohort", default=None)
    analytics_sync_parser.add_argument("--subcohort", default=None)
    analytics_sync_parser.add_argument("--require-tag", action="append", dest="require_tags", default=[])
    analytics_sync_parser.add_argument("--exclude-tag", action="append", dest="exclude_tags", default=[])
    analytics_sync_parser.add_argument("--title-contains", action="append", default=[])
    analytics_sync_parser.add_argument("--source-url-contains", action="append", default=[])

    comments_sync_parser = subparsers.add_parser(
        "sync-youtube-comments",
        help="Fetch and store YouTube comments for video assets.",
    )
    comments_sync_parser.add_argument("--asset", default=None)
    comments_sync_parser.add_argument("--video-id", action="append", dest="video_ids", default=[])
    comments_sync_parser.add_argument("--channel", default=None)
    comments_sync_parser.add_argument("--selection-status", choices=["included", "excluded", "unset"], default=None)
    comments_sync_parser.add_argument("--cohort", default=None)
    comments_sync_parser.add_argument("--subcohort", default=None)
    comments_sync_parser.add_argument("--require-tag", action="append", dest="require_tags", default=[])
    comments_sync_parser.add_argument("--exclude-tag", action="append", dest="exclude_tags", default=[])
    comments_sync_parser.add_argument("--title-contains", action="append", default=[])
    comments_sync_parser.add_argument("--source-url-contains", action="append", default=[])

    score_feedback_parser = subparsers.add_parser(
        "score-feedback",
        help="Project YouTube retention onto asset/timeline segments and store feedback scores.",
    )
    score_feedback_parser.add_argument(
        "--asset",
        default=None,
        help="Optional asset id, relative path, or youtube video id. Defaults to all assets with youtube_video_id.",
    )
    score_feedback_parser.add_argument("--start-date", type=parse_date, required=True)
    score_feedback_parser.add_argument("--end-date", type=parse_date, required=True)
    score_feedback_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Delete existing score rows for the same window before recalculating.",
    )
    score_feedback_parser.add_argument("--channel", default=None)
    score_feedback_parser.add_argument("--selection-status", choices=["included", "excluded", "unset"], default=None)
    score_feedback_parser.add_argument("--cohort", default=None)
    score_feedback_parser.add_argument("--subcohort", default=None)
    score_feedback_parser.add_argument("--require-tag", action="append", dest="require_tags", default=[])
    score_feedback_parser.add_argument("--exclude-tag", action="append", dest="exclude_tags", default=[])
    score_feedback_parser.add_argument("--title-contains", action="append", default=[])
    score_feedback_parser.add_argument("--source-url-contains", action="append", default=[])

    feedback_pipeline_parser = subparsers.add_parser(
        "run-feedback-pipeline",
        help="Sync YouTube analytics and score feedback for the selected corpus in one command.",
    )
    feedback_pipeline_parser.add_argument("--asset", default=None)
    feedback_pipeline_parser.add_argument("--video-id", action="append", dest="video_ids", default=[])
    feedback_pipeline_parser.add_argument("--start-date", type=parse_date, required=True)
    feedback_pipeline_parser.add_argument("--end-date", type=parse_date, required=True)
    feedback_pipeline_parser.add_argument("--overwrite", action="store_true")
    feedback_pipeline_parser.add_argument("--skip-sync", action="store_true")
    feedback_pipeline_parser.add_argument("--channel", default=None)
    feedback_pipeline_parser.add_argument("--selection-status", choices=["included", "excluded", "unset"], default=None)
    feedback_pipeline_parser.add_argument("--cohort", default=None)
    feedback_pipeline_parser.add_argument("--subcohort", default=None)
    feedback_pipeline_parser.add_argument("--require-tag", action="append", dest="require_tags", default=[])
    feedback_pipeline_parser.add_argument("--exclude-tag", action="append", dest="exclude_tags", default=[])
    feedback_pipeline_parser.add_argument("--title-contains", action="append", default=[])
    feedback_pipeline_parser.add_argument("--source-url-contains", action="append", default=[])
    feedback_pipeline_parser.add_argument("--auto-link-youtube-assets", action="store_true")

    list_feedback_parser = subparsers.add_parser(
        "list-feedback",
        help="List stored feedback scores.",
    )
    list_feedback_parser.add_argument("--asset", default=None)
    list_feedback_parser.add_argument("--scope-type", choices=["asset", "timeline_segment"], default=None)
    list_feedback_parser.add_argument("--score-name", default=None)
    list_feedback_parser.add_argument("--limit", type=int, default=100)

    recommend_feedback_parser = subparsers.add_parser(
        "recommend-feedback",
        help="Show the strongest assets or timeline segments from stored feedback scores.",
    )
    recommend_feedback_parser.add_argument(
        "--scope-type",
        choices=["asset", "timeline_segment"],
        default="timeline_segment",
    )
    recommend_feedback_parser.add_argument("--score-name", default="feedback_score_v1")
    recommend_feedback_parser.add_argument("--media-type", choices=["text", "audio", "video"], default=None)
    recommend_feedback_parser.add_argument("--limit", type=int, default=10)
    recommend_feedback_parser.add_argument("--min-score", type=float, default=None)
    recommend_feedback_parser.add_argument("--selection-status", choices=["included", "excluded", "unset"], default=None)
    recommend_feedback_parser.add_argument("--cohort", default=None)
    recommend_feedback_parser.add_argument("--subcohort", default=None)

    retrieval_benchmark_parser = subparsers.add_parser(
        "run-retrieval-benchmark",
        help="Evaluate retrieval Hit@1 / Hit@3 / MRR against a curated latest50 benchmark set.",
    )
    retrieval_benchmark_parser.add_argument("--benchmark", type=Path, default=None)
    retrieval_benchmark_parser.add_argument("--out", type=Path, default=None)
    retrieval_benchmark_parser.add_argument("--format", choices=["markdown", "json"], default="markdown")
    retrieval_benchmark_parser.add_argument("--namespace", default=None)
    retrieval_benchmark_parser.add_argument("--semantic-limit", type=int, default=None)
    retrieval_benchmark_parser.add_argument("--media-type", choices=["text", "audio", "video"], default=None)
    retrieval_benchmark_parser.add_argument(
        "--embedding-kind",
        choices=["asset", "text_chunk", "timeline_segment"],
        default=None,
    )
    retrieval_benchmark_parser.add_argument("--rerank-feedback", action="store_true")
    retrieval_benchmark_parser.add_argument("--feedback-weight", type=float, default=None)
    retrieval_benchmark_parser.add_argument("--candidate-k", type=int, default=None)
    retrieval_benchmark_parser.add_argument("--min-feedback-score", type=float, default=None)
    retrieval_benchmark_parser.add_argument("--cross-encoder-rerank", action="store_true")
    retrieval_benchmark_parser.add_argument("--cross-encoder-top-k", type=int, default=None)
    retrieval_benchmark_parser.add_argument("--selection-status", choices=["included", "excluded", "unset"], default=None)
    retrieval_benchmark_parser.add_argument("--cohort", default=None)
    retrieval_benchmark_parser.add_argument("--subcohort", default=None)
    retrieval_benchmark_parser.add_argument("--case", action="append", dest="case_ids", default=[])

    feedback_diagnostics_parser = subparsers.add_parser(
        "feedback-diagnostics",
        help="Inspect whether YouTube analytics and feedback scores are populated for the selected corpus window.",
    )
    feedback_diagnostics_parser.add_argument("--start-date", type=parse_date, required=True)
    feedback_diagnostics_parser.add_argument("--end-date", type=parse_date, required=True)
    feedback_diagnostics_parser.add_argument("--out", type=Path, default=None)
    feedback_diagnostics_parser.add_argument("--format", choices=["markdown", "json"], default="markdown")
    feedback_diagnostics_parser.add_argument("--media-type", choices=["text", "audio", "video"], default=None)
    feedback_diagnostics_parser.add_argument("--channel", default=None)
    feedback_diagnostics_parser.add_argument("--selection-status", choices=["included", "excluded", "unset"], default=None)
    feedback_diagnostics_parser.add_argument("--cohort", default=None)
    feedback_diagnostics_parser.add_argument("--subcohort", default=None)
    feedback_diagnostics_parser.add_argument("--require-tag", action="append", dest="require_tags", default=[])
    feedback_diagnostics_parser.add_argument("--exclude-tag", action="append", dest="exclude_tags", default=[])
    feedback_diagnostics_parser.add_argument("--title-contains", action="append", default=[])
    feedback_diagnostics_parser.add_argument("--source-url-contains", action="append", default=[])
    feedback_diagnostics_parser.add_argument("--limit", type=int, default=None)

    auto_assign_cohorts_parser = subparsers.add_parser(
        "auto-assign-cohorts",
        help="Infer cohort labels from Bankara titles and persist them into curation metadata.",
    )
    auto_assign_cohorts_parser.add_argument("--media-type", choices=["text", "audio", "video"], default=None)
    auto_assign_cohorts_parser.add_argument("--channel", default=None)
    auto_assign_cohorts_parser.add_argument("--selection-status", choices=["included", "excluded", "unset"], default=None)
    auto_assign_cohorts_parser.add_argument("--require-tag", action="append", dest="require_tags", default=[])
    auto_assign_cohorts_parser.add_argument("--exclude-tag", action="append", dest="exclude_tags", default=[])
    auto_assign_cohorts_parser.add_argument("--title-contains", action="append", default=[])
    auto_assign_cohorts_parser.add_argument("--source-url-contains", action="append", default=[])
    auto_assign_cohorts_parser.add_argument("--limit", type=int, default=None)
    auto_assign_cohorts_parser.add_argument("--dry-run", action="store_true")

    # ── Allow external callers to register extra sub-commands ────────────
    if extra_commands is not None:
        extra_commands(subparsers)

    return parser


def run(
    args: argparse.Namespace,
    *,
    fallback_dispatcher: "Callable[..., bool] | None" = None,
) -> None:
    """Dispatch a parsed CLI command.

    Parameters
    ----------
    fallback_dispatcher:
        Optional callback ``(args, *, brain) -> bool`` tried when
        no Brain command matches.  Used by ``bankara_brain_control_plane``
        to delegate consumer commands.
    """
    config = AppConfig.from_env()
    session_factory = init_db(config)
    blob_store = BlobStore(config.object_store_root)

    # Build a BankaraBrain facade for consumer (fallback) commands.
    from bankara_brain import BankaraBrain
    brain = BankaraBrain(config=config, session_factory=session_factory)

    if args.command == "init-db":
        print(f"Database ready: {config.database_url}")
        return

    if args.command == "stage-dataset":
        stage_dataset(
            session_factory=session_factory,
            blob_store=blob_store,
            dataset_dir=args.dir.expanduser().resolve(),
            recursive=args.recursive,
            copy_mode=args.copy_mode,
            force=args.force,
            limit=args.limit,
        )
        return

    if args.command == "run-ingest-pipeline":
        run_ingest_pipeline(
            session_factory=session_factory,
            blob_store=blob_store,
            dataset_dir=args.dir.expanduser().resolve(),
            output_dir=args.out_dir.expanduser().resolve(),
            recursive=args.recursive,
            copy_mode=args.copy_mode,
            force_stage=args.force_stage,
            replace_bootstrap=args.replace_bootstrap,
            skip_bootstrap_timeline=args.skip_bootstrap_timeline,
            max_segment_seconds=args.max_segment_seconds,
            min_segment_seconds=args.min_segment_seconds,
            gap_seconds=args.gap_seconds,
            target_chars=args.target_chars,
            limit=args.limit,
            namespace=args.namespace,
            only_missing_embeddings=args.only_missing_embeddings,
            use_files_api=args.use_files_api,
            allow_trim_long_media=not args.no_trim_long_media,
            embedding_python=args.embedding_python,
            dry_run=args.dry_run,
            channel=args.channel,
            require_tags=args.require_tags,
            exclude_tags=args.exclude_tags,
            title_contains=args.title_contains,
            source_url_contains=args.source_url_contains,
            selection_status=args.selection_status,
        )
        return

    if args.command == "run-maintenance-pipeline":
        run_maintenance_pipeline(
            session_factory=session_factory,
            blob_store=blob_store,
            output_dir=args.out_dir.expanduser().resolve(),
            asset_selector=args.asset,
            media_type=args.media_type,
            channel=args.channel,
            selection_status=args.selection_status,
            cohort=args.cohort,
            subcohort=args.subcohort,
            require_tags=args.require_tags,
            exclude_tags=args.exclude_tags,
            title_contains=args.title_contains,
            source_url_contains=args.source_url_contains,
            limit=args.limit,
            skip_duration_repair=args.skip_duration_repair,
            skip_transcribe=args.skip_transcribe,
            force_transcribe=args.force_transcribe,
            transcribe_script=args.transcribe_script,
            transcribe_language=args.transcribe_language,
            transcribe_model=args.transcribe_model,
            work_dir=args.work_dir,
            skip_bootstrap_timeline=args.skip_bootstrap_timeline,
            replace_timeline=args.replace_timeline,
            max_segment_seconds=args.max_segment_seconds,
            min_segment_seconds=args.min_segment_seconds,
            gap_seconds=args.gap_seconds,
            target_chars=args.target_chars,
            namespace=args.namespace,
            only_missing_embeddings=args.only_missing_embeddings,
            use_files_api=args.use_files_api,
            allow_trim_long_media=not args.no_trim_long_media,
            embedding_python=args.embedding_python,
            dry_run=args.dry_run,
        )
        return

    if args.command == "run-corpus-cycle":
        run_corpus_cycle(
            config=config,
            session_factory=session_factory,
            blob_store=blob_store,
            output_dir=args.out_dir.expanduser().resolve(),
            asset_selector=args.asset,
            media_type=args.media_type,
            channel=args.channel,
            selection_status=args.selection_status,
            require_tags=args.require_tags,
            exclude_tags=args.exclude_tags,
            title_contains=args.title_contains,
            source_url_contains=args.source_url_contains,
            limit=args.limit,
            quarantine_problem_filters=args.quarantine_problem_filters,
            quarantine_severity_filters=args.quarantine_severity_filters,
            quarantine_cohort=args.quarantine_cohort,
            quarantine_reason_prefix=args.quarantine_reason_prefix,
            skip_duration_repair=args.skip_duration_repair,
            skip_transcribe=args.skip_transcribe,
            force_transcribe=args.force_transcribe,
            transcribe_script=args.transcribe_script,
            transcribe_language=args.transcribe_language,
            transcribe_model=args.transcribe_model,
            work_dir=args.work_dir,
            skip_bootstrap_timeline=args.skip_bootstrap_timeline,
            replace_timeline=args.replace_timeline,
            max_segment_seconds=args.max_segment_seconds,
            min_segment_seconds=args.min_segment_seconds,
            gap_seconds=args.gap_seconds,
            target_chars=args.target_chars,
            namespace=args.namespace,
            only_missing_embeddings=args.only_missing_embeddings,
            use_files_api=args.use_files_api,
            allow_trim_long_media=not args.no_trim_long_media,
            embedding_python=args.embedding_python,
            feedback_start_date=args.feedback_start_date,
            feedback_end_date=args.feedback_end_date,
            overwrite_feedback=args.overwrite_feedback,
            skip_feedback_sync=args.skip_feedback_sync,
            require_feedback=args.require_feedback,
            skip_metadata_sync=args.skip_metadata_sync,
            auto_link_assets=args.auto_link_youtube_assets,
            dry_run=args.dry_run,
        )
        return

    if args.command == "list-assets":
        list_assets(
            session_factory=session_factory,
            media_type=args.media_type,
            limit=args.limit,
            channel=args.channel,
            require_tags=args.require_tags,
            exclude_tags=args.exclude_tags,
            title_contains=args.title_contains,
            source_url_contains=args.source_url_contains,
            selection_status=args.selection_status,
            cohort=args.cohort,
            subcohort=args.subcohort,
        )
        return

    if args.command == "corpus-status":
        corpus_status(
            session_factory=session_factory,
            channel=args.channel,
            selection_status=args.selection_status,
            cohort=args.cohort,
            subcohort=args.subcohort,
        )
        return

    if args.command == "curate-assets":
        curate_assets(
            session_factory=session_factory,
            selection_status=args.selection_status,
            media_type=args.media_type,
            channel=args.channel,
            require_tags=args.require_tags,
            exclude_tags=args.exclude_tags,
            title_contains=args.title_contains,
            source_url_contains=args.source_url_contains,
            cohort=args.cohort,
            reason=args.reason,
            limit=args.limit,
            dry_run=args.dry_run,
        )
        return

    if args.command == "auto-curate-bankara":
        auto_curate_bankara_assets(
            session_factory=session_factory,
            target_channel=args.target_channel,
            include_keywords=args.include_keywords or list(DEFAULT_COMEDY_INCLUDE_KEYWORDS),
            exclude_keywords=args.exclude_keywords or list(DEFAULT_COMEDY_EXCLUDE_KEYWORDS),
            include_threshold=args.include_threshold,
            exclude_threshold=args.exclude_threshold,
            media_type=args.media_type,
            selection_status=args.selection_status,
            cohort=args.cohort,
            reason_prefix=args.reason_prefix,
            limit=args.limit,
            dry_run=args.dry_run,
        )
        return

    if args.command == "audit-assets":
        audit_assets(
            session_factory=session_factory,
            media_type=args.media_type,
            channel=args.channel,
            selection_status=args.selection_status,
            cohort=args.cohort,
            subcohort=args.subcohort,
            require_tags=args.require_tags,
            exclude_tags=args.exclude_tags,
            title_contains=args.title_contains,
            source_url_contains=args.source_url_contains,
            only_problems=args.only_problems,
            only_blockers=args.only_blockers,
            only_warnings=args.only_warnings,
            limit=args.limit,
            json_output=args.json_output.expanduser().resolve() if args.json_output else None,
            summary_output=args.summary_output.expanduser().resolve() if args.summary_output else None,
        )
        return

    if args.command == "quarantine-assets":
        quarantine_assets(
            session_factory=session_factory,
            media_type=args.media_type,
            channel=args.channel,
            selection_status=args.selection_status,
            require_tags=args.require_tags,
            exclude_tags=args.exclude_tags,
            title_contains=args.title_contains,
            source_url_contains=args.source_url_contains,
            problem_filters=args.problem_filters,
            severity_filters=args.severity,
            cohort=args.cohort,
            reason_prefix=args.reason_prefix,
            limit=args.limit,
            dry_run=args.dry_run,
        )
        return

    if args.command == "purge-embeddings":
        purge_embeddings(
            session_factory=session_factory,
            asset_selector=args.asset,
            media_type=args.media_type,
            channel=args.channel,
            selection_status=args.selection_status,
            require_tags=args.require_tags,
            exclude_tags=args.exclude_tags,
            title_contains=args.title_contains,
            source_url_contains=args.source_url_contains,
            namespace=args.namespace,
            limit=args.limit,
            dry_run=args.dry_run,
            report_output=args.report_output.expanduser().resolve() if args.report_output else None,
        )
        return

    if args.command == "sync-embedding-metadata":
        sync_embedding_metadata(
            session_factory=session_factory,
            asset_selector=args.asset,
            media_type=args.media_type,
            channel=args.channel,
            selection_status=args.selection_status,
            cohort=args.cohort,
            subcohort=args.subcohort,
            require_tags=args.require_tags,
            exclude_tags=args.exclude_tags,
            title_contains=args.title_contains,
            source_url_contains=args.source_url_contains,
            namespace=args.namespace,
            limit=args.limit,
            dry_run=args.dry_run,
            report_output=args.report_output.expanduser().resolve() if args.report_output else None,
        )
        return

    if args.command == "doctor":
        doctor(
            config=config,
            json_output=args.json_output.expanduser().resolve() if args.json_output else None,
        )
        return

    if args.command == "auth-youtube":
        auth_youtube(
            config=config,
            force_reauth=args.force_reauth,
        )
        return

    if args.command == "youtube-whoami":
        youtube_whoami(
            config=config,
            json_output=args.json_output.expanduser().resolve() if args.json_output else None,
        )
        return

    if args.command == "list-youtube-videos":
        list_youtube_videos(
            config=config,
            limit=args.limit,
            title_contains=args.title_contains,
            json_output=args.json_output.expanduser().resolve() if args.json_output else None,
        )
        return

    if args.command == "link-youtube-assets":
        link_youtube_assets(
            config=config,
            session_factory=session_factory,
            asset_selector=args.asset,
            manual_video_id=args.video_id,
            media_type=args.media_type,
            channel=args.channel,
            selection_status=args.selection_status,
            cohort=args.cohort,
            require_tags=args.require_tags,
            exclude_tags=args.exclude_tags,
            title_contains=args.title_contains,
            source_url_contains=args.source_url_contains,
            asset_limit=args.limit,
            catalog_limit=args.catalog_limit,
            dry_run=args.dry_run,
            report_output=args.report_output.expanduser().resolve() if args.report_output else None,
        )
        return

    if args.command == "list-public-youtube-videos":
        list_public_youtube_videos(
            channel_url=args.channel_url,
            limit=args.limit,
            title_contains=args.title_contains,
            include_keywords=args.include_keywords,
            exclude_keywords=args.exclude_keywords,
            json_output=args.json_output.expanduser().resolve() if args.json_output else None,
        )
        return

    if args.command == "download-public-youtube-videos":
        download_public_youtube_videos(
            channel_url=args.channel_url,
            output_dir=args.out_dir.expanduser().resolve(),
            video_ids=args.video_ids,
            title_contains=args.title_contains,
            include_keywords=args.include_keywords,
            exclude_keywords=args.exclude_keywords,
            limit=args.limit,
            sub_langs=args.sub_langs,
            dry_run=args.dry_run,
            report_output=args.report_output.expanduser().resolve() if args.report_output else None,
        )
        return

    if args.command == "repair-assets":
        repair_assets(
            session_factory=session_factory,
            blob_store=blob_store,
            asset_selector=args.asset,
            media_type=args.media_type,
            channel=args.channel,
            selection_status=args.selection_status,
            require_tags=args.require_tags,
            exclude_tags=args.exclude_tags,
            title_contains=args.title_contains,
            source_url_contains=args.source_url_contains,
            limit=args.limit,
            skip_duration_repair=args.skip_duration_repair,
            skip_transcribe=args.skip_transcribe,
            force_transcribe=args.force_transcribe,
            transcribe_script=args.transcribe_script,
            transcribe_language=args.transcribe_language,
            transcribe_model=args.transcribe_model,
            work_dir=args.work_dir,
            skip_bootstrap_timeline=args.skip_bootstrap_timeline,
            replace_timeline=args.replace_timeline,
            max_segment_seconds=args.max_segment_seconds,
            min_segment_seconds=args.min_segment_seconds,
            gap_seconds=args.gap_seconds,
            target_chars=args.target_chars,
            dry_run=args.dry_run,
            report_output=args.report_output.expanduser().resolve() if args.report_output else None,
        )
        return

    if args.command == "enrich-structured-summaries":
        enrich_structured_summaries(
            session_factory=session_factory,
            asset_selector=args.asset,
            media_type=args.media_type,
            channel=args.channel,
            selection_status=args.selection_status,
            cohort=args.cohort,
            subcohort=args.subcohort,
            require_tags=args.require_tags,
            exclude_tags=args.exclude_tags,
            title_contains=args.title_contains,
            source_url_contains=args.source_url_contains,
            limit=args.limit,
            overwrite=args.overwrite,
            model_name=args.model,
            temperature=args.temperature,
            dry_run=args.dry_run,
            report_output=args.report_output.expanduser().resolve() if args.report_output else None,
        )
        return

    if args.command == "enrich-visual-audio-summaries":
        enrich_visual_audio_summaries(
            session_factory=session_factory,
            asset_selector=args.asset,
            media_type=args.media_type,
            channel=args.channel,
            selection_status=args.selection_status,
            cohort=args.cohort,
            subcohort=args.subcohort,
            require_tags=args.require_tags,
            exclude_tags=args.exclude_tags,
            title_contains=args.title_contains,
            source_url_contains=args.source_url_contains,
            limit=args.limit,
            overwrite=args.overwrite,
            model_name=args.model,
            temperature=args.temperature,
            dry_run=args.dry_run,
            scene_threshold=args.scene_threshold,
            report_output=args.report_output.expanduser().resolve() if args.report_output else None,
        )
        return

    if args.command == "export-embedding-manifest":
        export_embedding_manifest(
            session_factory=session_factory,
            output_path=args.out.expanduser().resolve(),
            namespace=args.namespace,
            limit=args.limit,
            only_missing_embeddings=args.only_missing_embeddings,
            channel=args.channel,
            require_tags=args.require_tags,
            exclude_tags=args.exclude_tags,
            title_contains=args.title_contains,
            source_url_contains=args.source_url_contains,
            selection_status=args.selection_status,
            cohort=args.cohort,
            subcohort=args.subcohort,
        )
        return

    if args.command == "import-embedding-results":
        import_embedding_results(session_factory=session_factory, results_path=args.results.expanduser().resolve())
        return

    if args.command == "import-shot-timeline":
        import_shot_timeline(
            session_factory=session_factory,
            asset_selector=args.asset,
            timeline_path=args.timeline.expanduser().resolve(),
            replace=not args.append,
        )
        return

    if args.command == "list-shot-timeline":
        list_timeline_segments(
            session_factory=session_factory,
            asset_selector=args.asset,
            limit=args.limit,
        )
        return

    if args.command == "bootstrap-shot-timeline":
        bootstrap_shot_timeline(
            session_factory=session_factory,
            asset_selector=args.asset,
            replace=args.replace,
            max_segment_seconds=args.max_segment_seconds,
            min_segment_seconds=args.min_segment_seconds,
            gap_seconds=args.gap_seconds,
            target_chars=args.target_chars,
        )
        return

    if args.command == "import-analytics-csv":
        import_analytics_csv(
            session_factory=session_factory,
            csv_path=args.csv.expanduser().resolve(),
            report_kind=args.report_kind,
            video_id=args.video_id,
            start_date=args.start_date,
            end_date=args.end_date,
        )
        return

    if args.command == "sync-youtube-analytics":
        sync_youtube_analytics(
            config=config,
            session_factory=session_factory,
            video_ids=args.video_ids,
            start_date=args.start_date,
            end_date=args.end_date,
            asset_selector=args.asset,
            channel=args.channel,
            require_tags=args.require_tags,
            exclude_tags=args.exclude_tags,
            title_contains=args.title_contains,
            source_url_contains=args.source_url_contains,
            selection_status=args.selection_status,
            cohort=args.cohort,
            subcohort=args.subcohort,
        )
        return

    if args.command == "sync-youtube-comments":
        from bankara_brain.youtube.comments import sync_youtube_comments
        sync_youtube_comments(
            config=config,
            session_factory=session_factory,
            video_ids=args.video_ids or None,
            asset_selector=args.asset,
            channel=args.channel,
            require_tags=args.require_tags,
            exclude_tags=args.exclude_tags,
            title_contains=args.title_contains,
            source_url_contains=args.source_url_contains,
            selection_status=args.selection_status,
            cohort=args.cohort,
            subcohort=args.subcohort,
        )
        return

    if args.command == "score-feedback":
        score_feedback(
            session_factory=session_factory,
            asset_selector=args.asset,
            start_date=args.start_date,
            end_date=args.end_date,
            overwrite=args.overwrite,
            channel=args.channel,
            require_tags=args.require_tags,
            exclude_tags=args.exclude_tags,
            title_contains=args.title_contains,
            source_url_contains=args.source_url_contains,
            selection_status=args.selection_status,
            cohort=args.cohort,
            subcohort=args.subcohort,
        )
        return

    if args.command == "run-feedback-pipeline":
        run_feedback_pipeline(
            config=config,
            session_factory=session_factory,
            asset_selector=args.asset,
            video_ids=args.video_ids,
            start_date=args.start_date,
            end_date=args.end_date,
            overwrite=args.overwrite,
            skip_sync=args.skip_sync,
            channel=args.channel,
            require_tags=args.require_tags,
            exclude_tags=args.exclude_tags,
            title_contains=args.title_contains,
            source_url_contains=args.source_url_contains,
            selection_status=args.selection_status,
            cohort=args.cohort,
            subcohort=args.subcohort,
            auto_link_assets=args.auto_link_youtube_assets,
        )
        return

    if args.command == "list-feedback":
        list_feedback_scores(
            session_factory=session_factory,
            asset_selector=args.asset,
            scope_type=args.scope_type,
            score_name=args.score_name,
            limit=args.limit,
        )
        return

    if args.command == "recommend-feedback":
        recommend_feedback_patterns(
            session_factory=session_factory,
            scope_type=args.scope_type,
            score_name=args.score_name,
            media_type=args.media_type,
            limit=args.limit,
            min_score=args.min_score,
            selection_status=args.selection_status,
            cohort=args.cohort,
            subcohort=args.subcohort,
        )
        return

    if fallback_dispatcher is not None and fallback_dispatcher(
        args, brain=brain
    ):
        return

    if args.command == "run-retrieval-benchmark":
        run_retrieval_benchmark(
            session_factory=session_factory,
            benchmark_path=args.benchmark.expanduser().resolve() if args.benchmark else None,
            output_path=args.out.expanduser().resolve() if args.out else None,
            output_format=args.format,
            namespace=args.namespace,
            semantic_limit=args.semantic_limit,
            media_type=args.media_type,
            embedding_kind=args.embedding_kind,
            rerank_feedback=True if args.rerank_feedback else None,
            feedback_weight=args.feedback_weight,
            candidate_k=args.candidate_k,
            min_feedback_score=args.min_feedback_score,
            cross_encoder_rerank=True if args.cross_encoder_rerank else None,
            cross_encoder_top_k=args.cross_encoder_top_k,
            selection_status=args.selection_status,
            cohort=args.cohort,
            subcohort=args.subcohort,
            case_ids=args.case_ids,
        )
        return

    if args.command == "feedback-diagnostics":
        feedback_diagnostics(
            session_factory=session_factory,
            start_date=args.start_date,
            end_date=args.end_date,
            output_path=args.out.expanduser().resolve() if args.out else None,
            output_format=args.format,
            media_type=args.media_type,
            channel=args.channel,
            selection_status=args.selection_status,
            cohort=args.cohort,
            subcohort=args.subcohort,
            require_tags=args.require_tags,
            exclude_tags=args.exclude_tags,
            title_contains=args.title_contains,
            source_url_contains=args.source_url_contains,
            limit=args.limit,
        )
        return

    if args.command == "auto-assign-cohorts":
        auto_assign_cohorts(
            session_factory=session_factory,
            media_type=args.media_type,
            channel=args.channel,
            selection_status=args.selection_status,
            require_tags=args.require_tags,
            exclude_tags=args.exclude_tags,
            title_contains=args.title_contains,
            source_url_contains=args.source_url_contains,
            limit=args.limit,
            dry_run=args.dry_run,
        )
        return

    raise RuntimeError(f"Unsupported command: {args.command}")


def main() -> int:
    setup_logging()
    logger = logging.getLogger(__name__)
    parser = build_parser()
    args = parser.parse_args()
    try:
        run(args)
        return 0
    except Exception as exc:
        logger.error("Command failed: %s", exc, exc_info=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
