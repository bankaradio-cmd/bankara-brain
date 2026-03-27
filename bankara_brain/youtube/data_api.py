"""YouTube Data API v3 — channel info, video catalog."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from bankara_brain.db import AppConfig
from bankara_brain.youtube.auth import build_youtube_data_service
from bankara_brain.youtube.helpers import is_valid_youtube_video_id


# ── Channel helpers ──────────────────────────────────────────────────────────

def fetch_authorized_channel_payload(service: Any) -> dict[str, Any]:
    """Return the raw channel resource for the authenticated user."""
    response = (
        service.channels()
        .list(part="id,snippet,statistics,contentDetails", mine=True, maxResults=1)
        .execute()
    )
    items = response.get("items", []) or []
    if not items:
        raise RuntimeError("No authorized YouTube channel returned by the OAuth token.")
    return items[0]


def summarize_authorized_youtube_channel(item: dict[str, Any]) -> dict[str, Any]:
    snippet = item.get("snippet", {}) or {}
    statistics = item.get("statistics", {}) or {}
    return {
        "channel_id": item.get("id"),
        "title": snippet.get("title"),
        "custom_url": snippet.get("customUrl"),
        "published_at": snippet.get("publishedAt"),
        "uploads_playlist_id": (
            ((item.get("contentDetails") or {}).get("relatedPlaylists") or {}).get("uploads")
        ),
        "view_count": statistics.get("viewCount"),
        "subscriber_count": statistics.get("subscriberCount"),
        "video_count": statistics.get("videoCount"),
    }


def check_expected_youtube_channel(config: AppConfig) -> dict[str, Any]:
    """Verify the OAuth token belongs to the expected channel."""
    expected_channel_id = (config.expected_youtube_channel_id or "").strip()
    if not expected_channel_id:
        return {
            "ok": True,
            "expected_channel_id": "",
            "authorized_channel_id": "",
            "detail": "No BANKARA_EXPECTED_YOUTUBE_CHANNEL_ID configured.",
        }

    service = build_youtube_data_service(config=config, force_reauth=False)
    payload = summarize_authorized_youtube_channel(fetch_authorized_channel_payload(service))
    authorized_channel_id = str(payload.get("channel_id") or "")
    ok = authorized_channel_id == expected_channel_id
    detail = (
        f"authorized={authorized_channel_id or '-'} expected={expected_channel_id}"
        f" title={payload.get('title') or '-'} custom_url={payload.get('custom_url') or '-'}"
    )
    return {
        "ok": ok,
        "expected_channel_id": expected_channel_id,
        "authorized_channel_id": authorized_channel_id,
        "payload": payload,
        "detail": detail,
    }


def ensure_expected_youtube_channel(config: AppConfig) -> None:
    """Raise RuntimeError if the OAuth token is for the wrong channel."""
    check = check_expected_youtube_channel(config)
    if not check["ok"]:
        raise RuntimeError(
            "Authorized YouTube OAuth token is pointing at the wrong channel. "
            f"{check['detail']}"
        )


def youtube_whoami(config: AppConfig, json_output: Path | None = None) -> None:
    """Print the authenticated YouTube channel identity."""
    service = build_youtube_data_service(config=config, force_reauth=False)
    payload = summarize_authorized_youtube_channel(fetch_authorized_channel_payload(service))

    if json_output:
        json_output.parent.mkdir(parents=True, exist_ok=True)
        json_output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Wrote YouTube identity report: {json_output}")

    print(
        f"YouTube channel: {payload['title'] or '-'} "
        f"(id={payload['channel_id'] or '-'} custom_url={payload['custom_url'] or '-'})"
    )
    print(
        f"Subscribers={payload['subscriber_count'] or '-'} "
        f"Videos={payload['video_count'] or '-'} "
        f"Views={payload['view_count'] or '-'}"
    )


# ── Video catalog ────────────────────────────────────────────────────────────

def _chunk_list(values: list[Any], size: int) -> list[list[Any]]:
    return [values[idx:idx + size] for idx in range(0, len(values), size)]


def _normalize_filter_values(values: list[str] | None) -> list[str]:
    if not values:
        return []
    return [str(v).strip().casefold() for v in values if str(v).strip()]


def _normalize_match_text(value: str | None) -> str:
    import re
    if not value:
        return ""
    from bankara_brain.utils import humanize_stem
    text = humanize_stem(str(value)).casefold()
    return re.sub(r"[^\w]+", "", text, flags=re.UNICODE)


def fetch_youtube_video_catalog(
    service: Any,
    limit: int | None,
    title_contains: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Fetch the full video catalog from the authorized channel's uploads playlist."""
    channel_item = fetch_authorized_channel_payload(service)
    channel_snippet = channel_item.get("snippet", {}) or {}
    uploads_playlist_id = (
        ((channel_item.get("contentDetails") or {}).get("relatedPlaylists") or {}).get("uploads")
    )
    if not uploads_playlist_id:
        raise RuntimeError("Authorized channel did not expose an uploads playlist.")

    playlist_rows: list[dict[str, Any]] = []
    page_token: str | None = None
    while True:
        batch_size = 50
        if limit is not None:
            remaining = limit - len(playlist_rows)
            if remaining <= 0:
                break
            batch_size = min(batch_size, remaining)

        response = (
            service.playlistItems()
            .list(
                part="snippet,contentDetails,status",
                playlistId=uploads_playlist_id,
                maxResults=batch_size,
                pageToken=page_token,
            )
            .execute()
        )
        items = response.get("items", []) or []
        for item in items:
            snippet = item.get("snippet", {}) or {}
            content_details = item.get("contentDetails", {}) or {}
            video_id = content_details.get("videoId")
            if not is_valid_youtube_video_id(video_id):
                continue
            playlist_rows.append(
                {
                    "video_id": video_id,
                    "title": snippet.get("title"),
                    "description": snippet.get("description"),
                    "published_at": snippet.get("publishedAt"),
                    "channel_id": snippet.get("channelId") or channel_item.get("id"),
                    "channel_title": snippet.get("channelTitle") or channel_snippet.get("title"),
                    "playlist_item_id": item.get("id"),
                    "position": snippet.get("position"),
                    "privacy_status": (item.get("status", {}) or {}).get("privacyStatus"),
                    "url": f"https://www.youtube.com/watch?v={video_id}",
                }
            )

        page_token = response.get("nextPageToken")
        if not page_token:
            break

    # Enrich with video details (statistics, contentDetails, status)
    details_by_id: dict[str, dict[str, Any]] = {}
    for batch in _chunk_list([row["video_id"] for row in playlist_rows], 50):
        response = (
            service.videos()
            .list(part="contentDetails,statistics,status,snippet", id=",".join(batch), maxResults=len(batch))
            .execute()
        )
        for item in response.get("items", []) or []:
            details_by_id[item.get("id")] = item

    normalized_title_filters = _normalize_filter_values(title_contains)
    catalog: list[dict[str, Any]] = []
    for row in playlist_rows:
        video_details = details_by_id.get(row["video_id"], {})
        statistics = video_details.get("statistics", {}) or {}
        content_details = video_details.get("contentDetails", {}) or {}
        status = video_details.get("status", {}) or {}
        snippet = video_details.get("snippet", {}) or {}
        payload = {
            **row,
            "duration": content_details.get("duration"),
            "view_count": statistics.get("viewCount"),
            "like_count": statistics.get("likeCount"),
            "comment_count": statistics.get("commentCount"),
            "video_privacy_status": status.get("privacyStatus"),
            "made_for_kids": status.get("madeForKids"),
            "default_language": snippet.get("defaultLanguage"),
            "normalized_title": _normalize_match_text(row.get("title")),
        }
        title_haystack = str(payload.get("title") or "").casefold()
        if normalized_title_filters and not all(needle in title_haystack for needle in normalized_title_filters):
            continue
        catalog.append(payload)

    catalog.sort(key=lambda row: row.get("published_at") or "", reverse=True)
    return catalog
