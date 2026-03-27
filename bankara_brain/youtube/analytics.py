"""YouTube Analytics API — daily metrics and retention curves."""
from __future__ import annotations

from datetime import date
from typing import Any


def fetch_youtube_daily_metrics(
    service: Any, video_id: str, start_date: date, end_date: date
) -> list[dict[str, Any]]:
    """Fetch daily metrics for a single video from YouTube Analytics API v2."""
    response = (
        service.reports()
        .query(
            ids="channel==MINE",
            startDate=start_date.isoformat(),
            endDate=end_date.isoformat(),
            dimensions="day",
            metrics=(
                "views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage,"
                "likes,comments,shares,subscribersGained,subscribersLost"
            ),
            filters=f"video=={video_id}",
            sort="day",
        )
        .execute()
    )
    return report_response_to_rows(response, extra={"video_id": video_id})


def fetch_youtube_retention(
    service: Any, video_id: str, start_date: date, end_date: date
) -> list[dict[str, Any]]:
    """Fetch audience retention curve for a single video."""
    response = (
        service.reports()
        .query(
            ids="channel==MINE",
            startDate=start_date.isoformat(),
            endDate=end_date.isoformat(),
            dimensions="elapsedVideoTimeRatio",
            metrics="audienceWatchRatio,relativeRetentionPerformance",
            filters=f"video=={video_id}",
            sort="elapsedVideoTimeRatio",
        )
        .execute()
    )
    return report_response_to_rows(response, extra={"video_id": video_id})


def report_response_to_rows(
    response: dict[str, Any], extra: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    """Convert YouTube Analytics API response into a list of flat dicts."""
    headers = [header["name"] for header in response.get("columnHeaders", [])]
    rows = response.get("rows", []) or []
    output = []
    for row in rows:
        payload = {key: value for key, value in zip(headers, row)}
        if extra:
            payload.update(extra)
        output.append(payload)
    return output
