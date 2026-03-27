"""CLI commands: score-feedback, run-feedback-pipeline, list-feedback,
recommend-feedback, feedback-diagnostics."""
from __future__ import annotations

import argparse
from pathlib import Path


def register(subparsers: "argparse._SubParsersAction") -> None:
    """Register feedback-related sub-commands."""
    from bankara_brain.utils import parse_date_value as parse_date

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


_COMMANDS = frozenset([
    "score-feedback", "run-feedback-pipeline", "list-feedback",
    "recommend-feedback", "feedback-diagnostics",
])


def dispatch(args: argparse.Namespace, *, config, session_factory, blob_store, brain) -> bool:
    """Execute a feedback command. Return True if handled."""
    if args.command not in _COMMANDS:
        return False

    if args.command == "score-feedback":
        from bankara_brain.analysis.scoring import score_feedback
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
        return True

    if args.command == "run-feedback-pipeline":
        from bankara_brain.analysis.scoring import run_feedback_pipeline
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
        return True

    if args.command == "list-feedback":
        from bankara_brain.analysis.scoring import list_feedback_scores
        list_feedback_scores(
            session_factory=session_factory,
            asset_selector=args.asset,
            scope_type=args.scope_type,
            score_name=args.score_name,
            limit=args.limit,
        )
        return True

    if args.command == "recommend-feedback":
        from bankara_brain.analysis.scoring import recommend_feedback_patterns
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
        return True

    if args.command == "feedback-diagnostics":
        from bankara_brain.analysis.scoring import feedback_diagnostics
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
        return True

    return False
