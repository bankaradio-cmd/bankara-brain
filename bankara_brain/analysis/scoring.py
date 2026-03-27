"""Feedback scoring, diagnostics, and pattern recommendation."""
from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
from typing import Any, Optional

from sqlalchemy import delete, select
from sqlalchemy.orm import Session, sessionmaker

from bankara_brain.models import (
    Asset,
    FeedbackScore,
    TimelineSegment,
    YoutubeDailyMetric,
    YoutubeRetentionPoint,
    now_utc,
)
from bankara_brain.utils import format_seconds_hms, safe_json_load
from bankara_brain.corpus.query import (
    asset_cohort,
    asset_selection_status,
    asset_subcohort,
    resolve_asset,
    select_assets_for_filters,
)
from bankara_brain.analysis.feedback import (
    build_feedback_inputs_from_aggregates,
    collect_feedback_pattern_rows,
    combine_feedback_score_v2,
    serialize_feedback_pattern,
)
from bankara_brain.youtube.helpers import is_valid_youtube_video_id


# ── Constants ────────────────────────────────────────────────────────────────

FEEDBACK_SCORE_FIELDS = (
    "feedback_score_v1",
    "feedback_score_v2",
    "watch_ratio_avg",
    "relative_retention_avg",
    "hook_watch_ratio_avg",
)


# ── Math helpers ─────────────────────────────────────────────────────────────


def average(values: list[float]) -> float:
    """Calculate the mean of a float list, returning 0.0 for empty lists."""
    if not values:
        return 0.0
    return float(sum(values) / len(values))


def clamp(value: float, lower: float, upper: float) -> float:
    """Clamp *value* between *lower* and *upper*."""
    return max(lower, min(upper, value))


def combine_feedback_score(avg_watch_ratio: float, avg_relative: float) -> float:
    """V1 composite feedback score from watch ratio and relative retention."""
    watch_component = clamp(avg_watch_ratio, 0.0, 1.5) / 1.5
    relative_component = clamp(avg_relative, 0.0, 1.5) / 1.5
    return round((watch_component * 0.7) + (relative_component * 0.3), 6)


def _parse_published_date(value: str | None) -> date | None:
    """Best-effort parse of the ``published_at`` string stored on assets."""
    if not value:
        return None
    try:
        return date.fromisoformat(value[:10])
    except (ValueError, TypeError):
        return None


# ── DB helpers ───────────────────────────────────────────────────────────────


def write_feedback_score(
    session: Session,
    asset_id: str,
    scope_type: str,
    scope_key: str,
    score_name: str,
    start_date: date,
    end_date: date,
    score_value: float,
    sample_count: int,
    details: dict[str, Any],
) -> None:
    """Upsert a FeedbackScore row."""
    existing = session.scalar(
        select(FeedbackScore)
        .where(FeedbackScore.asset_id == asset_id)
        .where(FeedbackScore.scope_type == scope_type)
        .where(FeedbackScore.scope_key == scope_key)
        .where(FeedbackScore.score_name == score_name)
        .where(FeedbackScore.start_date == start_date)
        .where(FeedbackScore.end_date == end_date)
    )
    record = existing or FeedbackScore(
        asset_id=asset_id,
        scope_type=scope_type,
        scope_key=scope_key,
        score_name=score_name,
        start_date=start_date,
        end_date=end_date,
        score_value=float(score_value),
        sample_count=sample_count,
    )
    record.score_value = float(score_value)
    record.sample_count = int(sample_count)
    record.details_json = json.dumps(details, ensure_ascii=False)
    session.add(record)


def load_feedback_summary_for_window(
    session: Session,
    asset_id: str,
    scope_type: str,
    scope_key: str,
    start_date: date,
    end_date: date,
) -> dict[str, Any]:
    """Get feedback stats for a specific time window."""
    rows = session.scalars(
        select(FeedbackScore)
        .where(FeedbackScore.asset_id == asset_id)
        .where(FeedbackScore.scope_type == scope_type)
        .where(FeedbackScore.scope_key == scope_key)
        .where(FeedbackScore.start_date == start_date)
        .where(FeedbackScore.end_date == end_date)
    ).all()
    if not rows:
        return {}
    summary = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }
    details: dict[str, Any] = {}
    for row in rows:
        summary[row.score_name] = row.score_value
        if not details:
            details = safe_json_load(row.details_json)
    if details:
        summary["details"] = details
    return summary


def load_latest_feedback_summary(
    session: Session,
    asset_id: str,
    scope_type: str,
    scope_key: str,
) -> dict[str, Any]:
    """Get the latest feedback summary for a given asset/scope."""
    latest_end_date = session.scalar(
        select(FeedbackScore.end_date)
        .where(FeedbackScore.asset_id == asset_id)
        .where(FeedbackScore.scope_type == scope_type)
        .where(FeedbackScore.scope_key == scope_key)
        .order_by(FeedbackScore.end_date.desc())
        .limit(1)
    )
    if latest_end_date is None:
        return {}

    rows = session.scalars(
        select(FeedbackScore)
        .where(FeedbackScore.asset_id == asset_id)
        .where(FeedbackScore.scope_type == scope_type)
        .where(FeedbackScore.scope_key == scope_key)
        .where(FeedbackScore.end_date == latest_end_date)
    ).all()
    summary = {"end_date": latest_end_date.isoformat()}
    for row in rows:
        summary[row.score_name] = row.score_value
    return summary


def load_latest_asset_feedback_summary(session: Session, asset_id: str) -> dict[str, Any]:
    """Shorthand for latest asset-level feedback summary."""
    return load_latest_feedback_summary(
        session=session,
        asset_id=asset_id,
        scope_type="asset",
        scope_key=asset_id,
    )


# ── Asset resolution for feedback ───────────────────────────────────────────


def resolve_feedback_assets(session: Session, asset_selector: Optional[str]) -> list[Asset]:
    """Return assets eligible for feedback (those with youtube_video_id)."""
    if asset_selector:
        return [resolve_asset(session, asset_selector)]
    return session.scalars(select(Asset).where(Asset.youtube_video_id.is_not(None)).order_by(Asset.relative_path)).all()


def resolve_feedback_assets_filtered(
    session: Session,
    asset_selector: Optional[str],
    channel: str | None,
    require_tags: list[str] | None,
    exclude_tags: list[str] | None,
    title_contains: list[str] | None,
    source_url_contains: list[str] | None,
    selection_status: str | None,
    cohort: str | None = None,
    subcohort: str | None = None,
) -> list[Asset]:
    """Return assets eligible for feedback with full filter support."""
    if asset_selector:
        return [resolve_asset(session, asset_selector)]
    return select_assets_for_filters(
        session=session,
        media_type=None,
        channel=channel,
        require_tags=require_tags,
        exclude_tags=exclude_tags,
        title_contains=title_contains,
        source_url_contains=source_url_contains,
        selection_status=selection_status,
        cohort=cohort,
        subcohort=subcohort,
        require_youtube_video_id=True,
    )


def feedback_filter_kwargs(args: argparse.Namespace) -> dict[str, Any]:
    """Extract feedback filter arguments from an argparse Namespace."""
    return {
        "asset_selector": getattr(args, "asset", None),
        "channel": getattr(args, "channel", None),
        "require_tags": getattr(args, "require_tags", None),
        "exclude_tags": getattr(args, "exclude_tags", None),
        "title_contains": getattr(args, "title_contains", None),
        "source_url_contains": getattr(args, "source_url_contains", None),
        "selection_status": getattr(args, "selection_status", None),
        "cohort": getattr(args, "cohort", None),
        "subcohort": getattr(args, "subcohort", None),
    }


# ── Retention mapping ───────────────────────────────────────────────────────


def retention_points_for_segment(
    retention_points: list[YoutubeRetentionPoint],
    duration_seconds: float,
    segment: TimelineSegment,
) -> list[YoutubeRetentionPoint]:
    """Map retention curve data to a timeline segment's timeframe."""
    start_ratio = max(0.0, segment.start_seconds / duration_seconds)
    end_ratio = min(1.0, segment.end_seconds / duration_seconds)
    points = [
        point
        for point in retention_points
        if start_ratio <= point.elapsed_video_time_ratio <= end_ratio
    ]
    if points:
        return points

    center_ratio = (start_ratio + end_ratio) / 2.0
    nearest = min(
        retention_points,
        key=lambda point: abs(point.elapsed_video_time_ratio - center_ratio),
    )
    return [nearest]


# ── Hook scoring ─────────────────────────────────────────────────────────────


def write_hook_score(
    session: Session,
    asset: Asset,
    retention_points: list[YoutubeRetentionPoint],
    start_date: date,
    end_date: date,
) -> None:
    """Calculate and write hook quality score (first 15% of video)."""
    hook_rows = [
        point.audience_watch_ratio
        for point in retention_points
        if point.audience_watch_ratio is not None and point.elapsed_video_time_ratio <= 0.15
    ]
    if not hook_rows:
        return
    hook_score = average(hook_rows)
    write_feedback_score(
        session=session,
        asset_id=asset.id,
        scope_type="asset",
        scope_key=asset.id,
        score_name="hook_watch_ratio_avg",
        start_date=start_date,
        end_date=end_date,
        score_value=hook_score,
        sample_count=len(hook_rows),
        details={"window_ratio_end": 0.15, "video_id": asset.youtube_video_id},
    )


# ── Asset-level scoring ─────────────────────────────────────────────────────


def score_asset_level_feedback(
    session: Session,
    asset: Asset,
    retention_points: list[YoutubeRetentionPoint],
    start_date: date,
    end_date: date,
    *,
    daily_rows: list[YoutubeDailyMetric] | None = None,
) -> None:
    """Calculate asset-level feedback scores from retention data."""
    watch_ratios = [point.audience_watch_ratio for point in retention_points if point.audience_watch_ratio is not None]
    relative_values = [
        point.relative_retention_performance
        for point in retention_points
        if point.relative_retention_performance is not None
    ]
    avg_watch_ratio = average(watch_ratios)
    avg_relative = average(relative_values)
    feedback_score_v1 = combine_feedback_score(avg_watch_ratio, avg_relative)

    write_feedback_score(
        session=session, asset_id=asset.id, scope_type="asset", scope_key=asset.id,
        score_name="watch_ratio_avg", start_date=start_date, end_date=end_date,
        score_value=avg_watch_ratio, sample_count=len(watch_ratios),
        details={"video_id": asset.youtube_video_id},
    )
    write_feedback_score(
        session=session, asset_id=asset.id, scope_type="asset", scope_key=asset.id,
        score_name="relative_retention_avg", start_date=start_date, end_date=end_date,
        score_value=avg_relative, sample_count=len(relative_values),
        details={"video_id": asset.youtube_video_id},
    )
    write_feedback_score(
        session=session, asset_id=asset.id, scope_type="asset", scope_key=asset.id,
        score_name="feedback_score_v1", start_date=start_date, end_date=end_date,
        score_value=feedback_score_v1, sample_count=len(retention_points),
        details={"video_id": asset.youtube_video_id, "watch_ratio_avg": avg_watch_ratio, "relative_retention_avg": avg_relative},
    )
    write_hook_score(session, asset, retention_points, start_date, end_date)

    # --- feedback_score_v2 ---
    hook_rows = [
        point.audience_watch_ratio
        for point in retention_points
        if point.audience_watch_ratio is not None and point.elapsed_video_time_ratio <= 0.15
    ]
    hook_avg = average(hook_rows)
    daily = daily_rows or []
    v2_inputs = build_feedback_inputs_from_aggregates(
        avg_watch_ratio=avg_watch_ratio,
        avg_relative_retention=avg_relative,
        hook_watch_ratio_avg=hook_avg,
        avg_impressions_ctr=average([float(r.impressions_ctr) for r in daily if r.impressions_ctr is not None]),
        total_views=sum(int(r.views or 0) for r in daily),
        total_likes=sum(int(r.likes or 0) for r in daily),
        total_comments=sum(int(r.comments or 0) for r in daily),
        total_shares=sum(int(r.shares or 0) for r in daily),
        published_date=_parse_published_date(asset.published_at),
        reference_date=end_date,
    )
    v2_result = combine_feedback_score_v2(v2_inputs)
    write_feedback_score(
        session=session, asset_id=asset.id, scope_type="asset", scope_key=asset.id,
        score_name="feedback_score_v2", start_date=start_date, end_date=end_date,
        score_value=v2_result.score, sample_count=len(retention_points),
        details={"video_id": asset.youtube_video_id, **v2_result.to_dict()},
    )


def score_asset_level_feedback_from_daily_metrics(
    session: Session,
    asset: Asset,
    daily_rows: list[YoutubeDailyMetric],
    start_date: date,
    end_date: date,
) -> None:
    """Calculate asset-level scores from daily metrics (fallback when retention unavailable)."""
    avg_view_percentages = [
        float(row.average_view_percentage) / 100.0
        for row in daily_rows
        if row.average_view_percentage is not None
    ]
    avg_duration_ratios = [
        float(row.average_view_duration_seconds) / float(asset.duration_seconds)
        for row in daily_rows
        if row.average_view_duration_seconds is not None and asset.duration_seconds and asset.duration_seconds > 0
    ]
    watch_ratio_proxy = average(avg_view_percentages or avg_duration_ratios)
    duration_ratio_proxy = average(avg_duration_ratios or avg_view_percentages)
    feedback_score_v1 = combine_feedback_score(watch_ratio_proxy, duration_ratio_proxy)
    details = {
        "video_id": asset.youtube_video_id,
        "fallback_source": "youtube_daily_metrics",
        "average_view_percentage_proxy": watch_ratio_proxy,
        "average_view_duration_ratio_proxy": duration_ratio_proxy,
        "views_total": sum(int(row.views or 0) for row in daily_rows),
        "days": len(daily_rows),
    }

    write_feedback_score(
        session=session, asset_id=asset.id, scope_type="asset", scope_key=asset.id,
        score_name="watch_ratio_avg", start_date=start_date, end_date=end_date,
        score_value=watch_ratio_proxy, sample_count=len(daily_rows), details=details,
    )
    write_feedback_score(
        session=session, asset_id=asset.id, scope_type="asset", scope_key=asset.id,
        score_name="relative_retention_avg", start_date=start_date, end_date=end_date,
        score_value=duration_ratio_proxy, sample_count=len(daily_rows), details=details,
    )
    write_feedback_score(
        session=session, asset_id=asset.id, scope_type="asset", scope_key=asset.id,
        score_name="feedback_score_v1", start_date=start_date, end_date=end_date,
        score_value=feedback_score_v1, sample_count=len(daily_rows),
        details={**details, "watch_ratio_avg": watch_ratio_proxy, "relative_retention_avg": duration_ratio_proxy},
    )

    # --- feedback_score_v2 from daily metrics ---
    v2_inputs = build_feedback_inputs_from_aggregates(
        avg_watch_ratio=watch_ratio_proxy,
        avg_relative_retention=duration_ratio_proxy,
        avg_impressions_ctr=average([float(r.impressions_ctr) for r in daily_rows if r.impressions_ctr is not None]),
        total_views=sum(int(r.views or 0) for r in daily_rows),
        total_likes=sum(int(r.likes or 0) for r in daily_rows),
        total_comments=sum(int(r.comments or 0) for r in daily_rows),
        total_shares=sum(int(r.shares or 0) for r in daily_rows),
        published_date=_parse_published_date(asset.published_at),
        reference_date=end_date,
    )
    v2_result = combine_feedback_score_v2(v2_inputs)
    write_feedback_score(
        session=session, asset_id=asset.id, scope_type="asset", scope_key=asset.id,
        score_name="feedback_score_v2", start_date=start_date, end_date=end_date,
        score_value=v2_result.score, sample_count=len(daily_rows),
        details={**details, **v2_result.to_dict()},
    )

    score_timeline_feedback_from_asset_proxy(
        session=session, asset=asset, start_date=start_date, end_date=end_date,
        avg_watch_ratio=watch_ratio_proxy, avg_relative=duration_ratio_proxy,
        feedback_score_v1=feedback_score_v1, feedback_score_v2=v2_result.score,
        sample_count=len(daily_rows), details=details,
    )


# ── Timeline-level scoring ──────────────────────────────────────────────────


def score_timeline_feedback_from_asset_proxy(
    session: Session,
    asset: Asset,
    start_date: date,
    end_date: date,
    *,
    avg_watch_ratio: float,
    avg_relative: float,
    feedback_score_v1: float,
    feedback_score_v2: float = 0.0,
    sample_count: int,
    details: dict[str, Any],
) -> None:
    """Generate timeline segment scores by interpolation from asset-level scores."""
    segments = session.scalars(
        select(TimelineSegment)
        .where(TimelineSegment.asset_id == asset.id)
        .order_by(TimelineSegment.segment_index)
    ).all()
    if not segments:
        return

    for segment in segments:
        scope_key = str(segment.id)
        base_details = {
            "segment_index": segment.segment_index,
            "segment_kind": segment.segment_kind,
            "label": segment.label,
            "start_seconds": segment.start_seconds,
            "end_seconds": segment.end_seconds,
            "video_id": asset.youtube_video_id,
            "proxy_scope": "asset",
            **details,
        }
        write_feedback_score(
            session=session, asset_id=asset.id, scope_type="timeline_segment", scope_key=scope_key,
            score_name="watch_ratio_avg", start_date=start_date, end_date=end_date,
            score_value=avg_watch_ratio, sample_count=sample_count, details=base_details,
        )
        write_feedback_score(
            session=session, asset_id=asset.id, scope_type="timeline_segment", scope_key=scope_key,
            score_name="relative_retention_avg", start_date=start_date, end_date=end_date,
            score_value=avg_relative, sample_count=sample_count, details=base_details,
        )
        write_feedback_score(
            session=session, asset_id=asset.id, scope_type="timeline_segment", scope_key=scope_key,
            score_name="feedback_score_v1", start_date=start_date, end_date=end_date,
            score_value=feedback_score_v1, sample_count=sample_count,
            details={**base_details, "watch_ratio_avg": avg_watch_ratio, "relative_retention_avg": avg_relative},
        )
        write_feedback_score(
            session=session, asset_id=asset.id, scope_type="timeline_segment", scope_key=scope_key,
            score_name="feedback_score_v2", start_date=start_date, end_date=end_date,
            score_value=feedback_score_v2, sample_count=sample_count,
            details={**base_details, "proxy_scope": "asset"},
        )


def score_timeline_feedback(
    session: Session,
    asset: Asset,
    retention_points: list[YoutubeRetentionPoint],
    start_date: date,
    end_date: date,
    *,
    daily_rows: list[YoutubeDailyMetric] | None = None,
) -> None:
    """Calculate timeline segment scores from retention data."""
    segments = session.scalars(
        select(TimelineSegment)
        .where(TimelineSegment.asset_id == asset.id)
        .order_by(TimelineSegment.segment_index)
    ).all()
    if not segments:
        return

    duration_seconds = float(asset.duration_seconds)
    for segment in segments:
        relevant_points = retention_points_for_segment(retention_points, duration_seconds, segment)
        if not relevant_points:
            continue

        watch_ratios = [point.audience_watch_ratio for point in relevant_points if point.audience_watch_ratio is not None]
        relative_values = [
            point.relative_retention_performance
            for point in relevant_points
            if point.relative_retention_performance is not None
        ]
        avg_watch_ratio = average(watch_ratios)
        avg_relative = average(relative_values)
        feedback_score_v1 = combine_feedback_score(avg_watch_ratio, avg_relative)
        scope_key = str(segment.id)
        base_details = {
            "segment_index": segment.segment_index,
            "segment_kind": segment.segment_kind,
            "label": segment.label,
            "start_seconds": segment.start_seconds,
            "end_seconds": segment.end_seconds,
            "video_id": asset.youtube_video_id,
        }
        write_feedback_score(
            session=session, asset_id=asset.id, scope_type="timeline_segment", scope_key=scope_key,
            score_name="watch_ratio_avg", start_date=start_date, end_date=end_date,
            score_value=avg_watch_ratio, sample_count=len(watch_ratios), details=base_details,
        )
        write_feedback_score(
            session=session, asset_id=asset.id, scope_type="timeline_segment", scope_key=scope_key,
            score_name="relative_retention_avg", start_date=start_date, end_date=end_date,
            score_value=avg_relative, sample_count=len(relative_values), details=base_details,
        )
        write_feedback_score(
            session=session, asset_id=asset.id, scope_type="timeline_segment", scope_key=scope_key,
            score_name="feedback_score_v1", start_date=start_date, end_date=end_date,
            score_value=feedback_score_v1, sample_count=len(relevant_points),
            details={**base_details, "watch_ratio_avg": avg_watch_ratio, "relative_retention_avg": avg_relative},
        )

        # --- feedback_score_v2 (segment level) ---
        seg_hook_rows = [
            p.audience_watch_ratio
            for p in relevant_points
            if p.audience_watch_ratio is not None and p.elapsed_video_time_ratio <= 0.15
        ]
        seg_hook_avg = average(seg_hook_rows) if seg_hook_rows else 0.0

        daily = daily_rows or []
        v2_inputs = build_feedback_inputs_from_aggregates(
            avg_watch_ratio=avg_watch_ratio,
            avg_relative_retention=avg_relative,
            hook_watch_ratio_avg=seg_hook_avg,
            avg_impressions_ctr=average([float(r.impressions_ctr) for r in daily if r.impressions_ctr is not None]),
            total_views=sum(int(r.views or 0) for r in daily),
            total_likes=sum(int(r.likes or 0) for r in daily),
            total_comments=sum(int(r.comments or 0) for r in daily),
            total_shares=sum(int(r.shares or 0) for r in daily),
            published_date=_parse_published_date(asset.published_at),
            reference_date=end_date,
        )
        v2_result = combine_feedback_score_v2(v2_inputs)
        write_feedback_score(
            session=session, asset_id=asset.id, scope_type="timeline_segment", scope_key=scope_key,
            score_name="feedback_score_v2", start_date=start_date, end_date=end_date,
            score_value=v2_result.score, sample_count=len(relevant_points),
            details={**base_details, **v2_result.to_dict()},
        )


# ── Main scoring orchestrator ───────────────────────────────────────────────


def score_feedback(
    session_factory: sessionmaker[Session],
    asset_selector: str | None,
    start_date: date,
    end_date: date,
    overwrite: bool,
    channel: str | None = None,
    require_tags: list[str] | None = None,
    exclude_tags: list[str] | None = None,
    title_contains: list[str] | None = None,
    source_url_contains: list[str] | None = None,
    selection_status: str | None = None,
    cohort: str | None = None,
    subcohort: str | None = None,
) -> None:
    """Main feedback scoring pipeline for assets."""
    scored_assets = 0
    with session_factory() as session:
        assets = resolve_feedback_assets_filtered(
            session=session, asset_selector=asset_selector,
            channel=channel, require_tags=require_tags, exclude_tags=exclude_tags,
            title_contains=title_contains, source_url_contains=source_url_contains,
            selection_status=selection_status, cohort=cohort, subcohort=subcohort,
        )
        for asset in assets:
            if not asset.youtube_video_id:
                print(f"Skipping {asset.relative_path}: missing youtube_video_id")
                continue
            if not is_valid_youtube_video_id(asset.youtube_video_id):
                print(f"Skipping {asset.relative_path}: invalid youtube_video_id={asset.youtube_video_id}")
                continue
            if not asset.duration_seconds or asset.duration_seconds <= 0:
                print(f"Skipping {asset.relative_path}: missing duration_seconds")
                continue

            retention_points = session.scalars(
                select(YoutubeRetentionPoint)
                .where(YoutubeRetentionPoint.video_id == asset.youtube_video_id)
                .where(YoutubeRetentionPoint.start_date == start_date)
                .where(YoutubeRetentionPoint.end_date == end_date)
                .order_by(YoutubeRetentionPoint.elapsed_video_time_ratio)
            ).all()

            if overwrite:
                session.execute(
                    delete(FeedbackScore)
                    .where(FeedbackScore.asset_id == asset.id)
                    .where(FeedbackScore.start_date == start_date)
                    .where(FeedbackScore.end_date == end_date)
                )

            daily_rows = session.scalars(
                select(YoutubeDailyMetric)
                .where(YoutubeDailyMetric.video_id == asset.youtube_video_id)
                .where(YoutubeDailyMetric.day >= start_date)
                .where(YoutubeDailyMetric.day <= end_date)
                .order_by(YoutubeDailyMetric.day)
            ).all()

            if retention_points:
                score_asset_level_feedback(session, asset, retention_points, start_date, end_date, daily_rows=daily_rows)
                score_timeline_feedback(session, asset, retention_points, start_date, end_date, daily_rows=daily_rows)
            else:
                if not daily_rows:
                    print(
                        f"Skipping {asset.relative_path}: no retention data and no daily metrics for "
                        f"{start_date.isoformat()}..{end_date.isoformat()}"
                    )
                    continue
                print(
                    f"Falling back to daily-metric asset scoring for {asset.relative_path}: "
                    f"retention unavailable in {start_date.isoformat()}..{end_date.isoformat()}"
                )
                score_asset_level_feedback_from_daily_metrics(session, asset, daily_rows, start_date, end_date)
            scored_assets += 1

        session.commit()

    print(f"Feedback scoring completed. assets={scored_assets}")


# ── Query / display ──────────────────────────────────────────────────────────


def list_feedback_scores(
    session_factory: sessionmaker[Session],
    asset_selector: str | None,
    scope_type: str | None,
    score_name: str | None,
    limit: int,
) -> None:
    """Display feedback scores for assets."""
    with session_factory() as session:
        stmt = select(FeedbackScore).order_by(
            FeedbackScore.end_date.desc(),
            FeedbackScore.scope_type,
            FeedbackScore.score_name,
            FeedbackScore.score_value.desc(),
        )
        if asset_selector:
            asset = resolve_asset(session, asset_selector)
            stmt = stmt.where(FeedbackScore.asset_id == asset.id)
        if scope_type:
            stmt = stmt.where(FeedbackScore.scope_type == scope_type)
        if score_name:
            stmt = stmt.where(FeedbackScore.score_name == score_name)

        rows = session.scalars(stmt.limit(limit)).all()
        if not rows:
            print("No feedback scores found.")
            return

        for row in rows:
            details = safe_json_load(row.details_json)
            extra = ""
            asset_label = row.asset.relative_path if row.asset else row.asset_id
            if row.scope_type == "timeline_segment":
                label = details.get("label") or ""
                segment_index = details.get("segment_index")
                start_seconds = details.get("start_seconds")
                end_seconds = details.get("end_seconds")
                time_range = ""
                if start_seconds is not None and end_seconds is not None:
                    time_range = f" time={format_seconds_hms(float(start_seconds))}-{format_seconds_hms(float(end_seconds))}"
                extra = f" asset={asset_label} segment={segment_index} label={label!r}{time_range}"
            else:
                extra = f" asset={asset_label}"
            print(
                f"{row.end_date.isoformat()} {row.scope_type:<15} {row.score_name:<24} "
                f"score={row.score_value:.4f} samples={row.sample_count}{extra}"
            )


# ── Pattern recommendation ──────────────────────────────────────────────────


def recommend_feedback_patterns(
    session_factory: sessionmaker[Session],
    scope_type: str,
    score_name: str,
    media_type: Optional[str],
    limit: int,
    min_score: Optional[float],
    selection_status: Optional[str] = None,
    cohort: Optional[str] = None,
    subcohort: Optional[str] = None,
) -> None:
    """Identify and display high-performing patterns by cohort."""
    with session_factory() as session:
        chosen = collect_feedback_pattern_rows(
            session=session, scope_type=scope_type, score_name=score_name,
            media_type=media_type, limit=limit, min_score=min_score,
            selection_status=selection_status, cohort=cohort, subcohort=subcohort,
        )
        if not chosen:
            print("No recommended feedback patterns found.")
            return

        print("\nRecommended patterns:")
        for rank, row in enumerate(chosen, start=1):
            pattern = serialize_feedback_pattern(session, row)
            print(
                f"{rank}. score={pattern['score_value']:.4f} end_date={pattern['end_date']} "
                f"asset={pattern['asset_relative_path'] or pattern['asset_id']}"
            )
            if scope_type == "timeline_segment":
                print_timeline_feedback_recommendation_from_pattern(pattern)
            else:
                print_asset_feedback_recommendation_from_pattern(pattern)


def print_timeline_feedback_recommendation_from_pattern(pattern: dict[str, Any]) -> None:
    """Format timeline pattern recommendation text."""
    print(
        f"   segment={pattern.get('segment_kind') or '-'} "
        f"label={pattern.get('segment_label', '')!r} "
        f"time={pattern.get('time_range') or '-'}"
    )
    if pattern.get("transcript"):
        print(f"   transcript={pattern['transcript']}")
    if pattern.get("notes"):
        print(f"   notes={pattern['notes']}")
    if pattern.get("asset_notes"):
        print(f"   asset_notes={pattern['asset_notes']}")
    if pattern.get("asset_summary_text"):
        print(f"   asset_summary={pattern['asset_summary_text']}")


def print_asset_feedback_recommendation_from_pattern(pattern: dict[str, Any]) -> None:
    """Format asset pattern recommendation text."""
    if pattern.get("asset_transcript_excerpt"):
        print(f"   transcript_excerpt={pattern['asset_transcript_excerpt']}")
    if pattern.get("asset_notes"):
        print(f"   notes={pattern['asset_notes']}")
    if pattern.get("asset_summary_text"):
        print(f"   summary={pattern['asset_summary_text']}")


# ── Feedback diagnostics ────────────────────────────────────────────────────


def render_feedback_diagnostics_markdown(payload: dict[str, Any]) -> str:
    """Format feedback diagnostics to markdown."""
    lines = [
        "# Feedback Diagnostics",
        "",
        f"- Window: {payload['start_date']} .. {payload['end_date']}",
        f"- Asset Count: {payload['asset_count']}",
        f"- Selection Status: {payload.get('selection_status') or '-'}",
        f"- Cohort: {payload.get('cohort') or '-'}",
        f"- Subcohort: {payload.get('subcohort') or '-'}",
        "",
        "## Coverage",
        "",
        f"- Assets with daily metrics: {payload['assets_with_daily_metrics']}",
        f"- Assets with retention: {payload['assets_with_retention']}",
        f"- Assets with asset feedback: {payload['assets_with_asset_feedback']}",
        f"- Assets with timeline feedback: {payload['assets_with_timeline_feedback']}",
        "",
        "## Signal Quality",
        "",
        f"- Total views in window: {payload['total_views']}",
        f"- Assets with non-zero views: {payload['assets_with_nonzero_views']}",
        f"- Assets with non-zero average view %: {payload['assets_with_nonzero_average_view_percentage']}",
        f"- Assets with non-zero feedback score (v1): {payload['assets_with_nonzero_feedback_score']}",
        f"- Assets with non-zero feedback score (v2): {payload.get('assets_with_nonzero_feedback_score_v2', 0)}",
        f"- Assets using daily metric fallback: {payload['assets_using_daily_metric_fallback']}",
        "",
        "## Recommendations",
        "",
    ]
    for item in payload.get("recommendations") or ["No major issues detected."]:
        lines.append(f"- {item}")

    if payload.get("cohort_breakdown"):
        lines.extend(["", "## Cohort Breakdown", ""])
        for row in payload["cohort_breakdown"]:
            lines.append(
                f"- {row['cohort'] or '(empty)'}: assets={row['asset_count']} "
                f"retention={row['assets_with_retention']} "
                f"daily={row['assets_with_daily_metrics']} "
                f"nonzero_feedback={row['assets_with_nonzero_feedback_score']}"
            )

    if payload.get("top_assets_by_views"):
        lines.extend(["", "## Top Assets By Views", ""])
        for row in payload["top_assets_by_views"]:
            lines.append(
                f"- {row['relative_path']}: views={row['views_total']} "
                f"avg_view_pct={row['average_view_percentage_mean']:.3f} "
                f"v1={row['feedback_score_v1']:.3f} "
                f"v2={row.get('feedback_score_v2', 0.0):.3f} "
                f"source={row['feedback_source']}"
            )

    if payload.get("problem_assets"):
        lines.extend(["", "## Problem Assets", ""])
        for row in payload["problem_assets"][:20]:
            lines.append(f"- {row['relative_path']}: {', '.join(row['issues'])}")

    return "\n".join(lines).strip() + "\n"


def feedback_diagnostics(
    session_factory: sessionmaker[Session],
    start_date: date,
    end_date: date,
    output_path: Path | None,
    output_format: str,
    media_type: str | None,
    channel: str | None,
    selection_status: str | None,
    cohort: str | None,
    subcohort: str | None,
    require_tags: list[str] | None,
    exclude_tags: list[str] | None,
    title_contains: list[str] | None,
    source_url_contains: list[str] | None,
    limit: int | None,
) -> None:
    """Generate a feedback diagnostics report."""
    with session_factory() as session:
        assets = select_assets_for_filters(
            session=session, media_type=media_type, channel=channel,
            require_tags=require_tags, exclude_tags=exclude_tags,
            title_contains=title_contains, source_url_contains=source_url_contains,
            selection_status=selection_status, cohort=cohort, subcohort=subcohort,
        )
        if limit is not None:
            assets = assets[:limit]

        rows: list[dict[str, Any]] = []
        for asset in assets:
            daily_rows = session.scalars(
                select(YoutubeDailyMetric)
                .where(YoutubeDailyMetric.video_id == asset.youtube_video_id)
                .where(YoutubeDailyMetric.day >= start_date)
                .where(YoutubeDailyMetric.day <= end_date)
                .order_by(YoutubeDailyMetric.day)
            ).all() if asset.youtube_video_id else []
            retention_rows = session.scalars(
                select(YoutubeRetentionPoint)
                .where(YoutubeRetentionPoint.video_id == asset.youtube_video_id)
                .where(YoutubeRetentionPoint.start_date == start_date)
                .where(YoutubeRetentionPoint.end_date == end_date)
                .order_by(YoutubeRetentionPoint.elapsed_video_time_ratio)
            ).all() if asset.youtube_video_id else []
            asset_feedback = load_feedback_summary_for_window(
                session=session, asset_id=asset.id, scope_type="asset",
                scope_key=asset.id, start_date=start_date, end_date=end_date,
            )
            timeline_feedback_count = len(
                session.scalars(
                    select(FeedbackScore)
                    .where(FeedbackScore.asset_id == asset.id)
                    .where(FeedbackScore.scope_type == "timeline_segment")
                    .where(FeedbackScore.score_name == "feedback_score_v1")
                    .where(FeedbackScore.start_date == start_date)
                    .where(FeedbackScore.end_date == end_date)
                ).all()
            )
            view_percentages = [
                float(row.average_view_percentage) / 100.0
                for row in daily_rows
                if row.average_view_percentage is not None
            ]
            duration_ratios = [
                float(row.average_view_duration_seconds) / float(asset.duration_seconds)
                for row in daily_rows
                if row.average_view_duration_seconds is not None and asset.duration_seconds and asset.duration_seconds > 0
            ]
            feedback_details = asset_feedback.get("details") if isinstance(asset_feedback.get("details"), dict) else {}
            issues: list[str] = []
            if not daily_rows:
                issues.append("missing_daily_metrics")
            if not retention_rows:
                issues.append("missing_retention")
            if daily_rows and sum(int(row.views or 0) for row in daily_rows) == 0:
                issues.append("zero_views")
            if asset_feedback and float(asset_feedback.get("feedback_score_v1", 0.0) or 0.0) == 0.0:
                issues.append("zero_feedback_score")

            rows.append(
                {
                    "asset_id": asset.id,
                    "relative_path": asset.relative_path,
                    "title": asset.title,
                    "cohort": asset_cohort(asset),
                    "subcohort": asset_subcohort(asset),
                    "selection_status": asset_selection_status(asset),
                    "video_id": asset.youtube_video_id or "",
                    "has_daily_metrics": bool(daily_rows),
                    "has_retention": bool(retention_rows),
                    "daily_row_count": len(daily_rows),
                    "retention_point_count": len(retention_rows),
                    "views_total": sum(int(row.views or 0) for row in daily_rows),
                    "average_view_percentage_mean": average(view_percentages),
                    "average_view_duration_ratio_mean": average(duration_ratios),
                    "feedback_score_v1": float(asset_feedback.get("feedback_score_v1", 0.0) or 0.0),
                    "feedback_score_v2": float(asset_feedback.get("feedback_score_v2", 0.0) or 0.0),
                    "feedback_source": feedback_details.get("fallback_source", "youtube_retention")
                    if asset_feedback
                    else "missing",
                    "timeline_feedback_count": timeline_feedback_count,
                    "issues": issues,
                }
            )

    cohort_groups: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        cohort_groups.setdefault(row["cohort"], []).append(row)

    recommendations: list[str] = []
    if rows and all(not row["has_retention"] for row in rows):
        recommendations.append("Retention が 0 件です。rerank は daily metrics proxy に依存しています。")
    if rows and all(row["feedback_score_v1"] == 0.0 for row in rows):
        recommendations.append("feedback_score_v1 が全 asset で 0.0 です。Analytics 側の実値を再点検してください。")
    if rows and all(row["feedback_score_v2"] == 0.0 for row in rows):
        recommendations.append("feedback_score_v2 が全 asset で 0.0 です。run-feedback-pipeline を再実行してください。")
    if rows and all(row["views_total"] == 0 for row in rows):
        recommendations.append("daily metrics の views が全 asset で 0 です。対象チャンネル/権限/期間を確認してください。")
    if rows and not recommendations:
        recommendations.append("Feedback 配管は正常です。次は cohort ごとの rerank 実測を見てください。")

    payload = {
        "generated_at": now_utc().isoformat(),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "asset_count": len(rows),
        "selection_status": selection_status,
        "cohort": cohort or "",
        "subcohort": subcohort or "",
        "assets_with_daily_metrics": sum(1 for row in rows if row["has_daily_metrics"]),
        "assets_with_retention": sum(1 for row in rows if row["has_retention"]),
        "assets_with_asset_feedback": sum(1 for row in rows if row["feedback_source"] != "missing"),
        "assets_with_timeline_feedback": sum(1 for row in rows if row["timeline_feedback_count"] > 0),
        "assets_using_daily_metric_fallback": sum(1 for row in rows if row["feedback_source"] == "youtube_daily_metrics"),
        "assets_with_nonzero_views": sum(1 for row in rows if row["views_total"] > 0),
        "assets_with_nonzero_average_view_percentage": sum(1 for row in rows if row["average_view_percentage_mean"] > 0.0),
        "assets_with_nonzero_feedback_score": sum(1 for row in rows if row["feedback_score_v1"] > 0.0),
        "assets_with_nonzero_feedback_score_v2": sum(1 for row in rows if row["feedback_score_v2"] > 0.0),
        "total_views": sum(row["views_total"] for row in rows),
        "recommendations": recommendations,
        "cohort_breakdown": [
            {
                "cohort": cohort_name,
                "asset_count": len(group_rows),
                "assets_with_daily_metrics": sum(1 for row in group_rows if row["has_daily_metrics"]),
                "assets_with_retention": sum(1 for row in group_rows if row["has_retention"]),
                "assets_with_nonzero_feedback_score": sum(1 for row in group_rows if row["feedback_score_v1"] > 0.0),
            }
            for cohort_name, group_rows in sorted(cohort_groups.items(), key=lambda item: (item[0] or "~", len(item[1])))
        ],
        "top_assets_by_views": sorted(rows, key=lambda row: (row["views_total"], row["average_view_percentage_mean"]), reverse=True)[:10],
        "problem_assets": [row for row in rows if row["issues"]],
        "assets": rows,
    }
    rendered = (
        json.dumps(payload, ensure_ascii=False, indent=2)
        if output_format == "json"
        else render_feedback_diagnostics_markdown(payload)
    )
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered, encoding="utf-8")
        print(f"Wrote feedback diagnostics: {output_path}")
        return

    print(rendered)


# ── Feedback pipeline orchestrator ──────────────────────────────────────────


def run_feedback_pipeline(
    config: "AppConfig",
    session_factory: sessionmaker[Session],
    asset_selector: str | None,
    video_ids: list[str],
    start_date: date,
    end_date: date,
    overwrite: bool,
    skip_sync: bool,
    channel: str | None,
    require_tags: list[str] | None,
    exclude_tags: list[str] | None,
    title_contains: list[str] | None,
    source_url_contains: list[str] | None,
    selection_status: str | None,
    cohort: str | None,
    subcohort: str | None = None,
    auto_link_assets: bool = False,
) -> None:
    """Full feedback pipeline: sync analytics + score feedback."""
    from bankara_brain.youtube.data_api import ensure_expected_youtube_channel

    ensure_expected_youtube_channel(config)
    if auto_link_assets:
        from bankara_brain.youtube.linking import link_youtube_assets

        print("Auto-linking YouTube assets with safe exact matches before feedback sync")
        link_youtube_assets(
            config=config, session_factory=session_factory,
            asset_selector=asset_selector, manual_video_id=None,
            media_type=None, channel=channel, selection_status=selection_status,
            cohort=cohort, subcohort=subcohort,
            require_tags=require_tags, exclude_tags=exclude_tags,
            title_contains=title_contains, source_url_contains=source_url_contains,
            asset_limit=None, catalog_limit=1000, dry_run=False, report_output=None,
        )

    with session_factory() as session:
        assets = resolve_feedback_assets_filtered(
            session=session, asset_selector=asset_selector,
            channel=channel, require_tags=require_tags, exclude_tags=exclude_tags,
            title_contains=title_contains, source_url_contains=source_url_contains,
            selection_status=selection_status, cohort=cohort, subcohort=subcohort,
        )
        inferred_video_ids = sorted(
            {asset.youtube_video_id for asset in assets if is_valid_youtube_video_id(asset.youtube_video_id)}
        )
        invalid_assets = [
            asset.relative_path
            for asset in assets
            if asset.youtube_video_id and not is_valid_youtube_video_id(asset.youtube_video_id)
        ]

    for relative_path in invalid_assets:
        print(f"Skipping feedback target {relative_path}: invalid youtube_video_id")

    effective_video_ids = sorted({video_id for video_id in (set(video_ids) | set(inferred_video_ids)) if is_valid_youtube_video_id(video_id)})
    print(
        f"Feedback pipeline targets: assets={len(assets)} "
        f"video_ids={len(effective_video_ids)} window={start_date.isoformat()}..{end_date.isoformat()}"
    )

    if not skip_sync:
        from bankara_brain.youtube.sync import sync_youtube_analytics

        sync_youtube_analytics(
            config=config, session_factory=session_factory,
            video_ids=effective_video_ids, start_date=start_date, end_date=end_date,
            asset_selector=asset_selector, channel=channel,
            require_tags=require_tags, exclude_tags=exclude_tags,
            title_contains=title_contains, source_url_contains=source_url_contains,
            selection_status=selection_status, cohort=cohort, subcohort=subcohort,
        )

    score_feedback(
        session_factory=session_factory, asset_selector=asset_selector,
        start_date=start_date, end_date=end_date, overwrite=overwrite,
        channel=channel, require_tags=require_tags, exclude_tags=exclude_tags,
        title_contains=title_contains, source_url_contains=source_url_contains,
        selection_status=selection_status, cohort=cohort, subcohort=subcohort,
    )
