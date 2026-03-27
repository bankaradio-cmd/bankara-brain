"""YouTube analytics sync: CSV import and API-based sync."""

from __future__ import annotations

import csv
import json
import os
from datetime import date
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from bankara_brain.db import AppConfig
from bankara_brain.models import Asset, YoutubeDailyMetric, YoutubeRetentionPoint, now_utc
from bankara_brain.youtube.auth import build_youtube_analytics_service
from bankara_brain.youtube.data_api import ensure_expected_youtube_channel
from bankara_brain.youtube.analytics import fetch_youtube_daily_metrics, fetch_youtube_retention
from bankara_brain.youtube.helpers import first_present, is_valid_youtube_video_id, resolve_asset_id_for_video_id
from bankara_brain.analysis.scoring import resolve_feedback_assets_filtered
from bankara_brain.utils import parse_date_value as parse_date, parse_float, parse_int, safe_int


def import_analytics_csv(
    session_factory: sessionmaker[Session],
    csv_path: Path,
    report_kind: str,
    video_id: str | None,
    start_date: date | None,
    end_date: date | None,
) -> None:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)

    with session_factory() as session:
        if report_kind == "daily":
            imported = import_daily_metrics_rows(session, rows, video_id=video_id)
        else:
            if not start_date or not end_date:
                raise ValueError("--start-date and --end-date are required for retention CSV import")
            imported = import_retention_rows(session, rows, video_id=video_id, start_date=start_date, end_date=end_date)
        session.commit()

    print(f"Imported analytics rows: {imported}")


def import_daily_metrics_rows(session: Session, rows: list[dict[str, str]], video_id: str | None) -> int:
    imported = 0
    for row in rows:
        resolved_video_id = video_id or first_present(row, ["video_id", "video", "videoId"])
        if not resolved_video_id:
            raise ValueError("Daily analytics row is missing video_id/video/videoId")

        asset_id = resolve_asset_id_for_video_id(session, resolved_video_id)
        day = parse_date(first_present(row, ["day", "date"]))
        existing = session.scalar(
            select(YoutubeDailyMetric)
            .where(YoutubeDailyMetric.video_id == resolved_video_id)
            .where(YoutubeDailyMetric.day == day)
        )

        metric = existing or YoutubeDailyMetric(video_id=resolved_video_id, day=day)
        metric.asset_id = asset_id
        metric.views = parse_int(row.get("views"))
        metric.estimated_minutes_watched = parse_float(first_present(row, ["estimatedMinutesWatched", "estimated_minutes_watched"]))
        metric.average_view_duration_seconds = parse_float(first_present(row, ["averageViewDuration", "average_view_duration_seconds"]))
        metric.average_view_percentage = parse_float(first_present(row, ["averageViewPercentage", "average_view_percentage"]))
        metric.impressions = parse_int(row.get("impressions"))
        metric.impressions_ctr = parse_float(first_present(row, ["impressionClickThroughRate", "impressionsCtr", "impressions_ctr"]))
        metric.likes = parse_int(row.get("likes"))
        metric.comments = parse_int(row.get("comments"))
        metric.shares = parse_int(row.get("shares"))
        metric.subscribers_gained = parse_int(first_present(row, ["subscribersGained", "subscribers_gained"]))
        metric.subscribers_lost = parse_int(first_present(row, ["subscribersLost", "subscribers_lost"]))
        session.add(metric)
        imported += 1

    return imported


def import_retention_rows(
    session: Session,
    rows: list[dict[str, str]],
    video_id: str | None,
    start_date: date,
    end_date: date,
) -> int:
    imported = 0
    for row in rows:
        resolved_video_id = video_id or first_present(row, ["video_id", "video", "videoId"])
        if not resolved_video_id:
            raise ValueError("Retention analytics row is missing video_id/video/videoId")

        asset_id = resolve_asset_id_for_video_id(session, resolved_video_id)
        ratio = parse_float(first_present(row, ["elapsedVideoTimeRatio", "elapsed_video_time_ratio"]))
        existing = session.scalar(
            select(YoutubeRetentionPoint)
            .where(YoutubeRetentionPoint.video_id == resolved_video_id)
            .where(YoutubeRetentionPoint.start_date == start_date)
            .where(YoutubeRetentionPoint.end_date == end_date)
            .where(YoutubeRetentionPoint.elapsed_video_time_ratio == ratio)
        )

        point = existing or YoutubeRetentionPoint(
            video_id=resolved_video_id,
            start_date=start_date,
            end_date=end_date,
            elapsed_video_time_ratio=ratio,
        )
        point.asset_id = asset_id
        point.audience_watch_ratio = parse_float(first_present(row, ["audienceWatchRatio", "audience_watch_ratio"]))
        point.relative_retention_performance = parse_float(
            first_present(row, ["relativeRetentionPerformance", "relative_retention_performance"])
        )
        session.add(point)
        imported += 1

    return imported


def sync_youtube_analytics(
    config: AppConfig,
    session_factory: sessionmaker[Session],
    video_ids: list[str],
    start_date: date,
    end_date: date,
    asset_selector: str | None = None,
    channel: str | None = None,
    require_tags: list[str] | None = None,
    exclude_tags: list[str] | None = None,
    title_contains: list[str] | None = None,
    source_url_contains: list[str] | None = None,
    selection_status: str | None = None,
    cohort: str | None = None,
    subcohort: str | None = None,
) -> None:
    ensure_expected_youtube_channel(config)
    service = build_youtube_analytics_service(config)
    if service is None:
        raise RuntimeError("YouTube Analytics dependencies are not installed.")

    with session_factory() as session:
        if not video_ids:
            assets = resolve_feedback_assets_filtered(
                session=session,
                asset_selector=asset_selector,
                channel=channel,
                require_tags=require_tags,
                exclude_tags=exclude_tags,
                title_contains=title_contains,
                source_url_contains=source_url_contains,
                selection_status=selection_status,
                cohort=cohort,
                subcohort=subcohort,
            )
            valid_video_ids = sorted(
                {asset.youtube_video_id for asset in assets if is_valid_youtube_video_id(asset.youtube_video_id)}
            )
            invalid_assets = [
                asset.relative_path
                for asset in assets
                if asset.youtube_video_id and not is_valid_youtube_video_id(asset.youtube_video_id)
            ]
            for relative_path in invalid_assets:
                print(f"Skipping analytics sync for {relative_path}: invalid youtube_video_id")
            video_ids = valid_video_ids

        for video_id in video_ids:
            print(f"Syncing YouTube analytics: {video_id}")
            daily_rows = fetch_youtube_daily_metrics(service, video_id=video_id, start_date=start_date, end_date=end_date)
            import_daily_metrics_rows(session, daily_rows, video_id=video_id)
            retention_rows = fetch_youtube_retention(service, video_id=video_id, start_date=start_date, end_date=end_date)
            import_retention_rows(session, retention_rows, video_id=video_id, start_date=start_date, end_date=end_date)

        session.commit()

    print(f"Synced YouTube analytics for {len(video_ids)} video(s)")
