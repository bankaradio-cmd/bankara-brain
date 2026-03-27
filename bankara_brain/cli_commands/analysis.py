"""CLI commands: enrich-structured-summaries, enrich-visual-audio-summaries,
doctor, repair-assets."""
from __future__ import annotations

import argparse
import os
from pathlib import Path


def register(subparsers: "argparse._SubParsersAction") -> None:
    """Register analysis / enrichment sub-commands."""
    from bankara_brain.analysis.enrichment import DEFAULT_GENERATION_MODEL

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

    doctor_parser = subparsers.add_parser(
        "doctor",
        help="Check runtime/API/OAuth/transcription prerequisites.",
    )
    doctor_parser.add_argument("--json-output", type=Path, default=None)

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


_COMMANDS = frozenset([
    "enrich-structured-summaries", "enrich-visual-audio-summaries",
    "doctor", "repair-assets",
])


def dispatch(args: argparse.Namespace, *, config, session_factory, blob_store, brain) -> bool:
    """Execute an analysis command. Return True if handled."""
    if args.command not in _COMMANDS:
        return False

    if args.command == "enrich-structured-summaries":
        from bankara_brain.analysis.enrichment import enrich_structured_summaries
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
        return True

    if args.command == "enrich-visual-audio-summaries":
        from bankara_brain.analysis.enrichment import enrich_visual_audio_summaries
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
        return True

    if args.command == "doctor":
        from bankara_brain.maintenance import doctor
        doctor(
            config=config,
            json_output=args.json_output.expanduser().resolve() if args.json_output else None,
        )
        return True

    if args.command == "repair-assets":
        from bankara_brain.maintenance import repair_assets
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
        return True

    return False
