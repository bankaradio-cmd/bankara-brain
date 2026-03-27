"""CLI commands: import-shot-timeline, list-shot-timeline, bootstrap-shot-timeline."""
from __future__ import annotations

import argparse
from pathlib import Path


def register(subparsers: "argparse._SubParsersAction") -> None:
    """Register timeline sub-commands."""

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


_COMMANDS = frozenset(["import-shot-timeline", "list-shot-timeline", "bootstrap-shot-timeline"])


def dispatch(args: argparse.Namespace, *, config, session_factory, blob_store, brain) -> bool:
    """Execute a timeline command. Return True if handled."""
    if args.command not in _COMMANDS:
        return False

    if args.command == "import-shot-timeline":
        from bankara_brain.corpus.timeline import import_shot_timeline
        import_shot_timeline(
            session_factory=session_factory,
            asset_selector=args.asset,
            timeline_path=args.timeline.expanduser().resolve(),
            replace=not args.append,
        )
        return True

    if args.command == "list-shot-timeline":
        from bankara_brain.corpus.timeline import list_timeline_segments
        list_timeline_segments(
            session_factory=session_factory,
            asset_selector=args.asset,
            limit=args.limit,
        )
        return True

    if args.command == "bootstrap-shot-timeline":
        from bankara_brain.corpus.timeline import bootstrap_shot_timeline
        bootstrap_shot_timeline(
            session_factory=session_factory,
            asset_selector=args.asset,
            replace=args.replace,
            max_segment_seconds=args.max_segment_seconds,
            min_segment_seconds=args.min_segment_seconds,
            gap_seconds=args.gap_seconds,
            target_chars=args.target_chars,
        )
        return True

    return False
