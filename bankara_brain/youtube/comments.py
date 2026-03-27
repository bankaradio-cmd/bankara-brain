"""YouTube comment fetching and storage."""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

from dotenv import load_dotenv
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from bankara_brain.db import AppConfig
from bankara_brain.models import Asset, YouTubeComment, now_utc
from bankara_brain.youtube.helpers import is_valid_youtube_video_id, resolve_asset_id_for_video_id


def _build_comments_service(config: AppConfig) -> Any:
    """Build a YouTube Data API service with comment-reading scope.

    Uses OAuth with youtube.force-ssl scope, which is required for
    commentThreads.list endpoint.
    """
    from bankara_brain.youtube.auth import build_youtube_data_service
    return build_youtube_data_service(config, force_reauth=False)


def _parse_youtube_datetime(value: str | None) -> datetime | None:
    """Parse ISO 8601 datetime string from YouTube API."""
    if not value:
        return None
    try:
        # YouTube returns e.g. "2024-01-15T12:34:56Z"
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def fetch_comment_threads(
    service: Any,
    video_id: str,
    max_results: int = 100,
) -> list[dict[str, Any]]:
    """Fetch all top-level comment threads for a video, including replies.

    Uses YouTube Data API v3 commentThreads.list endpoint.
    Handles pagination automatically.
    """
    all_threads: list[dict[str, Any]] = []
    page_token: str | None = None

    while True:
        try:
            response = (
                service.commentThreads()
                .list(
                    part="snippet,replies",
                    videoId=video_id,
                    maxResults=min(max_results, 100),
                    pageToken=page_token,
                    textFormat="plainText",
                    order="time",
                )
                .execute()
            )
        except Exception as exc:
            msg = str(exc).upper()
            # Extract just the API error reason, not the URL
            reason = ""
            if hasattr(exc, "reason"):
                reason = str(getattr(exc, "reason", "")).upper()
            # Comments disabled on this video
            if "HAS DISABLED COMMENTS" in msg or "COMMENTS ARE DISABLED" in msg:
                logger.info("Comments are disabled for video %s", video_id)
                return []
            if "COMMENTSdisabled" in msg.replace(" ", "").replace("_", ""):
                logger.info("Comments are disabled for video %s", video_id)
                return []
            if "NOT_FOUND" in msg or ("404" in msg and "COMMENT" not in msg):
                logger.warning("Video not found: %s", video_id)
                return []
            # Rate limit: wait and retry once
            if "429" in msg or "RATE_LIMIT" in msg or "QUOTA" in msg:
                logger.warning("Rate limited on video %s, waiting 30s...", video_id)
                time.sleep(30)
                try:
                    response = (
                        service.commentThreads()
                        .list(
                            part="snippet,replies",
                            videoId=video_id,
                            maxResults=min(max_results, 100),
                            pageToken=page_token,
                            textFormat="plainText",
                            order="time",
                        )
                        .execute()
                    )
                except Exception:
                    raise
            else:
                raise

        items = response.get("items", []) or []
        all_threads.extend(items)

        page_token = response.get("nextPageToken")
        if not page_token:
            break

        # Brief pause to avoid rate limits
        time.sleep(0.5)

    return all_threads


def _extract_comments_from_threads(
    threads: list[dict[str, Any]],
    video_id: str,
) -> list[dict[str, Any]]:
    """Extract flat list of comment records from API thread responses."""
    comments: list[dict[str, Any]] = []

    for thread in threads:
        snippet = thread.get("snippet", {}) or {}
        top_comment = snippet.get("topLevelComment", {}) or {}
        top_snippet = top_comment.get("snippet", {}) or {}

        comment_id = top_comment.get("id", "")
        if not comment_id:
            continue

        comments.append({
            "video_id": video_id,
            "comment_id": comment_id,
            "parent_comment_id": None,
            "author_display_name": top_snippet.get("authorDisplayName", ""),
            "author_channel_id": (
                (top_snippet.get("authorChannelId") or {}).get("value", "")
            ),
            "text_original": top_snippet.get("textOriginal", ""),
            "like_count": int(top_snippet.get("likeCount", 0)),
            "reply_count": int(snippet.get("totalReplyCount", 0)),
            "published_at": _parse_youtube_datetime(top_snippet.get("publishedAt")),
            "youtube_updated_at": _parse_youtube_datetime(top_snippet.get("updatedAt")),
        })

        # Extract replies
        replies = thread.get("replies", {}) or {}
        reply_comments = replies.get("comments", []) or []
        for reply in reply_comments:
            reply_snippet = reply.get("snippet", {}) or {}
            reply_id = reply.get("id", "")
            if not reply_id:
                continue

            comments.append({
                "video_id": video_id,
                "comment_id": reply_id,
                "parent_comment_id": comment_id,
                "author_display_name": reply_snippet.get("authorDisplayName", ""),
                "author_channel_id": (
                    (reply_snippet.get("authorChannelId") or {}).get("value", "")
                ),
                "text_original": reply_snippet.get("textOriginal", ""),
                "like_count": int(reply_snippet.get("likeCount", 0)),
                "reply_count": 0,
                "published_at": _parse_youtube_datetime(reply_snippet.get("publishedAt")),
                "youtube_updated_at": _parse_youtube_datetime(reply_snippet.get("updatedAt")),
            })

    return comments


def upsert_comments(
    session: Session,
    comments: list[dict[str, Any]],
    asset_id: str | None,
) -> tuple[int, int]:
    """Insert or update comments in the database.

    Returns (inserted, updated) counts.
    """
    inserted = 0
    updated = 0

    for comment_data in comments:
        existing = session.scalar(
            select(YouTubeComment)
            .where(YouTubeComment.comment_id == comment_data["comment_id"])
        )

        if existing:
            existing.text_original = comment_data["text_original"]
            existing.like_count = comment_data["like_count"]
            existing.reply_count = comment_data["reply_count"]
            existing.youtube_updated_at = comment_data["youtube_updated_at"]
            existing.asset_id = asset_id or existing.asset_id
            session.add(existing)
            updated += 1
        else:
            record = YouTubeComment(
                asset_id=asset_id,
                video_id=comment_data["video_id"],
                comment_id=comment_data["comment_id"],
                parent_comment_id=comment_data["parent_comment_id"],
                author_display_name=comment_data["author_display_name"],
                author_channel_id=comment_data["author_channel_id"] or None,
                text_original=comment_data["text_original"],
                like_count=comment_data["like_count"],
                reply_count=comment_data["reply_count"],
                published_at=comment_data["published_at"],
                youtube_updated_at=comment_data["youtube_updated_at"],
            )
            session.add(record)
            inserted += 1

    return inserted, updated


def sync_youtube_comments(
    config: AppConfig,
    session_factory: sessionmaker[Session],
    video_ids: list[str] | None = None,
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
    """Fetch and store YouTube comments for videos.

    If video_ids are provided, fetches for those specific videos.
    Otherwise, fetches for all video assets matching the filters.
    """
    service = _build_comments_service(config)

    with session_factory() as session:
        if video_ids:
            targets = [
                (vid, resolve_asset_id_for_video_id(session, vid))
                for vid in video_ids
                if is_valid_youtube_video_id(vid)
            ]
        else:
            from bankara_brain.analysis.scoring import resolve_feedback_assets_filtered
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
            targets = [
                (asset.youtube_video_id, asset.id)
                for asset in assets
                if is_valid_youtube_video_id(asset.youtube_video_id)
            ]

        total_inserted = 0
        total_updated = 0
        processed = 0

        for video_id, asset_id in targets:
            print(f"Fetching comments: {video_id} ({processed + 1}/{len(targets)})")
            try:
                threads = fetch_comment_threads(service, video_id)
            except Exception as exc:
                logger.error("Error fetching comments for %s: %s", video_id, exc)
                continue

            if not threads:
                print(f"  No comments found")
                processed += 1
                continue

            comments = _extract_comments_from_threads(threads, video_id)
            inserted, updated = upsert_comments(session, comments, asset_id)
            session.commit()
            total_inserted += inserted
            total_updated += updated
            processed += 1

            top_level = sum(1 for c in comments if not c["parent_comment_id"])
            replies = len(comments) - top_level
            print(f"  {top_level} comments + {replies} replies (new={inserted} updated={updated})")

            # Brief pause between videos to avoid rate limits
            time.sleep(1.0)

    print(
        f"\nComment sync complete: {processed} videos processed, "
        f"{total_inserted} new comments, {total_updated} updated"
    )
