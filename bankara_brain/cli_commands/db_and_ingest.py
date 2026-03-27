"""CLI commands: init-db, stage-dataset, run-ingest-pipeline, run-maintenance-pipeline."""
from __future__ import annotations

import argparse
import os
from pathlib import Path


def register(subparsers: "argparse._SubParsersAction") -> None:
    """Register database / ingest sub-commands."""

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


_COMMANDS = frozenset(["init-db", "stage-dataset", "run-ingest-pipeline", "run-maintenance-pipeline"])


def dispatch(args: argparse.Namespace, *, config, session_factory, blob_store, brain) -> bool:
    """Execute a db/ingest command. Return True if handled."""
    if args.command not in _COMMANDS:
        return False

    if args.command == "init-db":
        print(f"Database ready: {config.database_url}")
        return True

    if args.command == "stage-dataset":
        from bankara_brain.ingest.stage import stage_dataset
        stage_dataset(
            session_factory=session_factory,
            blob_store=blob_store,
            dataset_dir=args.dir.expanduser().resolve(),
            recursive=args.recursive,
            copy_mode=args.copy_mode,
            force=args.force,
            limit=args.limit,
        )
        return True

    if args.command == "run-ingest-pipeline":
        from bankara_brain.ingest.pipeline import run_ingest_pipeline
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
        return True

    if args.command == "run-maintenance-pipeline":
        from bankara_brain.pipelines import run_maintenance_pipeline
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
        return True

    return False
