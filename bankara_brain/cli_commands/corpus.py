"""CLI commands: run-corpus-cycle, list-assets, corpus-status, curate-assets,
auto-curate-bankara, audit-assets, quarantine-assets, auto-assign-cohorts."""
from __future__ import annotations

import argparse
import os
from pathlib import Path


def register(subparsers: "argparse._SubParsersAction") -> None:
    """Register corpus management sub-commands."""
    from bankara_brain.utils import parse_date_value as parse_date
    from bankara_brain.corpus.curation import (
        DEFAULT_BANKARA_CHANNEL,
    )

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


_COMMANDS = frozenset([
    "run-corpus-cycle", "list-assets", "corpus-status", "curate-assets",
    "auto-curate-bankara", "audit-assets", "quarantine-assets", "auto-assign-cohorts",
])


def dispatch(args: argparse.Namespace, *, config, session_factory, blob_store, brain) -> bool:
    """Execute a corpus command. Return True if handled."""
    if args.command not in _COMMANDS:
        return False

    if args.command == "run-corpus-cycle":
        from bankara_brain.pipelines import run_corpus_cycle
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
        return True

    if args.command == "list-assets":
        from bankara_brain.corpus.curation import list_assets
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
        return True

    if args.command == "corpus-status":
        from bankara_brain.corpus.curation import corpus_status
        corpus_status(
            session_factory=session_factory,
            channel=args.channel,
            selection_status=args.selection_status,
            cohort=args.cohort,
            subcohort=args.subcohort,
        )
        return True

    if args.command == "curate-assets":
        from bankara_brain.corpus.curation import curate_assets
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
        return True

    if args.command == "auto-curate-bankara":
        from bankara_brain.corpus.curation import (
            auto_curate_bankara_assets,
            DEFAULT_COMEDY_INCLUDE_KEYWORDS,
            DEFAULT_COMEDY_EXCLUDE_KEYWORDS,
        )
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
        return True

    if args.command == "audit-assets":
        from bankara_brain.corpus.curation import audit_assets
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
        return True

    if args.command == "quarantine-assets":
        from bankara_brain.corpus.curation import quarantine_assets
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
        return True

    if args.command == "auto-assign-cohorts":
        from bankara_brain.corpus.curation import auto_assign_cohorts
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
        return True

    return False
