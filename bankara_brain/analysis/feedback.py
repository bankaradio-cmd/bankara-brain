"""Feedback Score v2 — multi-axis scoring with recency decay.

Replaces the fixed 70/30 split of ``feedback_score_v1`` with a richer
composite that reflects watch ratio, relative retention, hook retention,
CTR, engagement rate, and publication recency.

``feedback_score_v1`` is preserved for backward compatibility.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session


# ---------------------------------------------------------------------------
# Default weight configuration
# ---------------------------------------------------------------------------

DEFAULT_WEIGHTS = {
    "watch": 0.35,
    "retention": 0.15,
    "hook": 0.20,
    "ctr": 0.15,
    "engagement": 0.15,
}

RECENCY_HALF_LIFE_DAYS = 180
RECENCY_FLOOR = 0.70
RECENCY_CEILING = 1.00


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class FeedbackInputs:
    """Raw metrics collected from YouTube Analytics / daily metrics."""

    watch_ratio: Optional[float] = None
    relative_retention: Optional[float] = None
    hook_watch_ratio: Optional[float] = None
    impressions_ctr: Optional[float] = None
    engagement_rate: Optional[float] = None
    published_date: Optional[date] = None
    reference_date: Optional[date] = None


@dataclass
class FeedbackScoreBreakdown:
    """Full breakdown of a v2 feedback score computation."""

    score: float
    watch_component: float
    retention_component: float
    hook_component: float
    ctr_component: float
    engagement_component: float
    recency_multiplier: float
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "score": self.score,
            "watch_component": self.watch_component,
            "retention_component": self.retention_component,
            "hook_component": self.hook_component,
            "ctr_component": self.ctr_component,
            "engagement_component": self.engagement_component,
            "recency_multiplier": self.recency_multiplier,
            "notes": self.notes,
        }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _normalize(
    value: Optional[float],
    ceiling: float,
    *,
    floor: float = 0.0,
) -> tuple[float, bool]:
    """Normalize *value* to ``[0, 1]``.

    Returns ``(normalized, was_present)`` so callers can redistribute
    weight when an input is missing.
    """
    if value is None:
        return 0.0, False
    return _clamp((value - floor) / (ceiling - floor), 0.0, 1.0), True


def compute_recency_multiplier(
    published_date: Optional[date],
    reference_date: Optional[date],
    *,
    half_life_days: int = RECENCY_HALF_LIFE_DAYS,
    floor: float = RECENCY_FLOOR,
    ceiling: float = RECENCY_CEILING,
) -> float:
    """Smooth recency multiplier in ``[floor, ceiling]``.

    Formula: ``floor + (ceiling - floor) / (1 + days_old / half_life_days)``
    """
    if published_date is None:
        return 1.0  # no date → neutral
    ref = reference_date or date.today()
    days_old = max(0, (ref - published_date).days)
    return round(floor + (ceiling - floor) / (1.0 + days_old / half_life_days), 6)


# ---------------------------------------------------------------------------
# Core scorer
# ---------------------------------------------------------------------------

# Normalization ceilings chosen to keep the [0, 1] range meaningful for
# Bankara Radio's typical metric ranges:
#   watch_ratio:       0-1.5  (can exceed 1.0 for re-watches)
#   relative_retention: 0-1.5  (YouTube's relative metric)
#   hook_watch_ratio:  0-1.5  (same unit as watch_ratio)
#   impressions_ctr:   0-0.15 (15% CTR is very high for YouTube)
#   engagement_rate:   0-0.10 (10% engagement is exceptional)

_CEILINGS = {
    "watch": 1.5,
    "retention": 1.5,
    "hook": 1.5,
    "ctr": 0.15,
    "engagement": 0.10,
}


def combine_feedback_score_v2(
    inputs: FeedbackInputs,
    *,
    weights: dict[str, float] | None = None,
) -> FeedbackScoreBreakdown:
    """Compute a composite feedback score from multi-axis inputs.

    Missing inputs degrade gracefully: their weight is redistributed
    proportionally across present inputs so the final score stays in
    ``[0, 1]`` regardless of data availability.
    """
    w = dict(DEFAULT_WEIGHTS if weights is None else weights)
    notes: list[str] = []

    # Normalize each axis -------------------------------------------------
    watch_norm, watch_ok = _normalize(inputs.watch_ratio, _CEILINGS["watch"])
    retention_norm, retention_ok = _normalize(inputs.relative_retention, _CEILINGS["retention"])
    hook_norm, hook_ok = _normalize(inputs.hook_watch_ratio, _CEILINGS["hook"])
    ctr_norm, ctr_ok = _normalize(inputs.impressions_ctr, _CEILINGS["ctr"])
    engagement_norm, engagement_ok = _normalize(inputs.engagement_rate, _CEILINGS["engagement"])

    # Presence map --------------------------------------------------------
    presence = {
        "watch": watch_ok,
        "retention": retention_ok,
        "hook": hook_ok,
        "ctr": ctr_ok,
        "engagement": engagement_ok,
    }
    present_total = sum(w[k] for k, ok in presence.items() if ok)
    missing_keys = [k for k, ok in presence.items() if not ok]

    if missing_keys:
        notes.append(f"missing: {', '.join(missing_keys)}")

    # Redistribute missing weight ----------------------------------------
    if present_total > 0 and present_total < 1.0:
        scale = 1.0 / present_total
        for k in w:
            if presence[k]:
                w[k] *= scale
            else:
                w[k] = 0.0
    elif present_total == 0:
        notes.append("no input data — score=0.0")
        return FeedbackScoreBreakdown(
            score=0.0,
            watch_component=0.0,
            retention_component=0.0,
            hook_component=0.0,
            ctr_component=0.0,
            engagement_component=0.0,
            recency_multiplier=1.0,
            notes=notes,
        )

    # Weighted sum --------------------------------------------------------
    watch_component = round(watch_norm * w["watch"], 6)
    retention_component = round(retention_norm * w["retention"], 6)
    hook_component = round(hook_norm * w["hook"], 6)
    ctr_component = round(ctr_norm * w["ctr"], 6)
    engagement_component = round(engagement_norm * w["engagement"], 6)

    raw = watch_component + retention_component + hook_component + ctr_component + engagement_component

    # Recency multiplier --------------------------------------------------
    recency = compute_recency_multiplier(inputs.published_date, inputs.reference_date)

    score = round(_clamp(raw * recency, 0.0, 1.0), 6)

    return FeedbackScoreBreakdown(
        score=score,
        watch_component=watch_component,
        retention_component=retention_component,
        hook_component=hook_component,
        ctr_component=ctr_component,
        engagement_component=engagement_component,
        recency_multiplier=recency,
        notes=notes,
    )


# ---------------------------------------------------------------------------
# Convenience: build FeedbackInputs from DB-level aggregates
# ---------------------------------------------------------------------------


def build_feedback_inputs_from_aggregates(
    *,
    avg_watch_ratio: float = 0.0,
    avg_relative_retention: float = 0.0,
    hook_watch_ratio_avg: float = 0.0,
    avg_impressions_ctr: float = 0.0,
    total_views: int = 0,
    total_likes: int = 0,
    total_comments: int = 0,
    total_shares: int = 0,
    published_date: Optional[date] = None,
    reference_date: Optional[date] = None,
) -> FeedbackInputs:
    """Create a ``FeedbackInputs`` from typical database aggregates.

    Engagement rate is derived as ``(likes + comments * 3 + shares * 2) / views``
    to weight comments and shares more heavily than passive likes.
    """
    engagement_rate: Optional[float] = None
    if total_views and total_views > 0:
        weighted = total_likes + total_comments * 3 + total_shares * 2
        engagement_rate = weighted / total_views

    return FeedbackInputs(
        watch_ratio=avg_watch_ratio if avg_watch_ratio else None,
        relative_retention=avg_relative_retention if avg_relative_retention else None,
        hook_watch_ratio=hook_watch_ratio_avg if hook_watch_ratio_avg else None,
        impressions_ctr=avg_impressions_ctr if avg_impressions_ctr else None,
        engagement_rate=engagement_rate,
        published_date=published_date,
        reference_date=reference_date,
    )


# ---------------------------------------------------------------------------
# Feedback pattern collection & serialisation (moved from control plane)
# ---------------------------------------------------------------------------


def collect_feedback_pattern_rows(
    session: Session,
    scope_type: str,
    score_name: str,
    media_type: Optional[str],
    limit: int,
    min_score: Optional[float],
    selection_status: Optional[str] = None,
    cohort: Optional[str] = None,
    subcohort: Optional[str] = None,
) -> list:
    """Collect top-scoring :class:`FeedbackScore` rows, deduplicated by scope key."""
    from bankara_brain.models import FeedbackScore
    from bankara_brain.corpus.query import (
        asset_cohort,
        asset_selection_status,
        asset_subcohort,
        normalize_cohort,
        normalize_selection_status,
        normalize_subcohort,
    )

    fetch_limit = max(limit * 20, 200)
    rows = session.scalars(
        select(FeedbackScore)
        .where(FeedbackScore.scope_type == scope_type)
        .where(FeedbackScore.score_name == score_name)
        .order_by(FeedbackScore.end_date.desc(), FeedbackScore.score_value.desc())
        .limit(fetch_limit)
    ).all()

    chosen: list = []
    seen_scope_keys: set[str] = set()
    for row in rows:
        if row.scope_key in seen_scope_keys:
            continue
        if row.asset is None:
            continue
        if media_type and row.asset.media_type != media_type:
            continue
        if selection_status and asset_selection_status(row.asset) != normalize_selection_status(selection_status):
            continue
        if cohort and asset_cohort(row.asset).casefold() != normalize_cohort(cohort):
            continue
        if subcohort and asset_subcohort(row.asset).casefold() != normalize_subcohort(subcohort):
            continue
        if min_score is not None and row.score_value < min_score:
            continue
        chosen.append(row)
        seen_scope_keys.add(row.scope_key)
        if len(chosen) >= limit:
            break
    return chosen


def serialize_feedback_pattern(session: Session, row: Any) -> dict[str, Any]:
    """Serialise a :class:`FeedbackScore` row into a dict suitable for briefs."""
    from bankara_brain.models import TimelineSegment
    from bankara_brain.utils import (
        format_seconds_hms,
        safe_int,
        safe_json_load,
        shorten_text,
    )
    from bankara_brain.analysis.structured_summary import extract_structured_summary_text
    from bankara_brain.corpus.query import asset_cohort, asset_subcohort

    details = safe_json_load(row.details_json)
    asset = row.asset
    asset_metadata = safe_json_load(asset.metadata_json) if asset else {}
    pattern: dict[str, Any] = {
        "scope_type": row.scope_type,
        "scope_key": row.scope_key,
        "score_name": row.score_name,
        "score_value": row.score_value,
        "sample_count": row.sample_count,
        "start_date": row.start_date.isoformat(),
        "end_date": row.end_date.isoformat(),
        "asset_id": row.asset_id,
        "asset_relative_path": asset.relative_path if asset else None,
        "asset_title": asset.title if asset else None,
        "asset_media_type": asset.media_type if asset else None,
        "asset_notes": shorten_text(asset.notes, 220) if asset and asset.notes else "",
        "asset_transcript_excerpt": shorten_text(asset.transcript_excerpt, 220)
        if asset and asset.transcript_excerpt
        else "",
        "asset_tags": asset_metadata.get("tags") or [],
        "asset_summary_text": extract_structured_summary_text(asset_metadata, compact=True),
        "curation_cohort": asset_cohort(asset) if asset else "",
        "curation_subcohort": asset_subcohort(asset, asset_metadata) if asset else "",
    }

    if row.scope_type == "timeline_segment":
        segment_id = safe_int(row.scope_key)
        segment = session.scalar(select(TimelineSegment).where(TimelineSegment.id == segment_id)) if segment_id else None
        start_seconds = details.get("start_seconds") if details.get("start_seconds") is not None else (segment.start_seconds if segment else None)
        end_seconds = details.get("end_seconds") if details.get("end_seconds") is not None else (segment.end_seconds if segment else None)
        pattern.update(
            {
                "segment_id": segment.id if segment else segment_id,
                "segment_index": details.get("segment_index") if details.get("segment_index") is not None else (segment.segment_index if segment else None),
                "segment_kind": details.get("segment_kind") or (segment.segment_kind if segment else ""),
                "segment_label": details.get("label") or (segment.label if segment else ""),
                "start_seconds": start_seconds,
                "end_seconds": end_seconds,
                "time_range": (
                    f"{format_seconds_hms(float(start_seconds))}-{format_seconds_hms(float(end_seconds))}"
                    if start_seconds is not None and end_seconds is not None
                    else ""
                ),
                "transcript": shorten_text(segment.transcript, 240) if segment and segment.transcript else "",
                "notes": shorten_text(segment.notes, 240) if segment and segment.notes else "",
            }
        )

    return pattern
