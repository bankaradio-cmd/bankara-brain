"""CLI commands: auth-youtube, youtube-whoami, list-youtube-videos,
link-youtube-assets, list-public-youtube-videos, download-public-youtube-videos,
import-analytics-csv, sync-youtube-analytics, sync-youtube-comments."""
from __future__ import annotations

import argparse
from pathlib import Path


def register(subparsers: "argparse._SubParsersAction") -> None:
    """Register YouTube-related sub-commands."""
    from bankara_brain.utils import parse_date_value as parse_date
    from bankara_brain.youtube.public import (
        DEFAULT_BANKARA_PUBLIC_CHANNEL_URL,
        DEFAULT_BANKARA_PUBLIC_EXCLUDE_KEYWORDS,
    )

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


_COMMANDS = frozenset([
    "auth-youtube", "youtube-whoami", "list-youtube-videos", "link-youtube-assets",
    "list-public-youtube-videos", "download-public-youtube-videos",
    "import-analytics-csv", "sync-youtube-analytics", "sync-youtube-comments",
])


def dispatch(args: argparse.Namespace, *, config, session_factory, blob_store, brain) -> bool:
    """Execute a YouTube command. Return True if handled."""
    if args.command not in _COMMANDS:
        return False

    if args.command == "auth-youtube":
        from bankara_brain.youtube.auth import auth_youtube
        auth_youtube(
            config=config,
            force_reauth=args.force_reauth,
        )
        return True

    if args.command == "youtube-whoami":
        from bankara_brain.youtube.data_api import youtube_whoami
        youtube_whoami(
            config=config,
            json_output=args.json_output.expanduser().resolve() if args.json_output else None,
        )
        return True

    if args.command == "list-youtube-videos":
        from bankara_brain.youtube.linking import list_youtube_videos
        list_youtube_videos(
            config=config,
            limit=args.limit,
            title_contains=args.title_contains,
            json_output=args.json_output.expanduser().resolve() if args.json_output else None,
        )
        return True

    if args.command == "link-youtube-assets":
        from bankara_brain.youtube.linking import link_youtube_assets
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
        return True

    if args.command == "list-public-youtube-videos":
        from bankara_brain.youtube.public import list_public_youtube_videos
        list_public_youtube_videos(
            channel_url=args.channel_url,
            limit=args.limit,
            title_contains=args.title_contains,
            include_keywords=args.include_keywords,
            exclude_keywords=args.exclude_keywords,
            json_output=args.json_output.expanduser().resolve() if args.json_output else None,
        )
        return True

    if args.command == "download-public-youtube-videos":
        from bankara_brain.youtube.public import download_public_youtube_videos
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
        return True

    if args.command == "import-analytics-csv":
        from bankara_brain.youtube.sync import import_analytics_csv
        import_analytics_csv(
            session_factory=session_factory,
            csv_path=args.csv.expanduser().resolve(),
            report_kind=args.report_kind,
            video_id=args.video_id,
            start_date=args.start_date,
            end_date=args.end_date,
        )
        return True

    if args.command == "sync-youtube-analytics":
        from bankara_brain.youtube.sync import sync_youtube_analytics
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
        return True

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
        return True

    return False
