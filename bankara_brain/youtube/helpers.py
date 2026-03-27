"""YouTube helper utilities — video ID parsing, validation, asset resolution."""
from __future__ import annotations

import re
from typing import Any
from urllib.parse import parse_qs, urlparse

from sqlalchemy import select
from sqlalchemy.orm import Session

from bankara_brain.models import Asset

YOUTUBE_VIDEO_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")


def is_valid_youtube_video_id(video_id: str | None) -> bool:
    if not video_id:
        return False
    return bool(YOUTUBE_VIDEO_ID_RE.fullmatch(video_id.strip()))


def extract_youtube_video_id(source_url: str | None) -> str | None:
    """Extract a YouTube video ID from a URL (youtu.be or youtube.com)."""
    if not source_url:
        return None
    parsed = urlparse(source_url)
    if parsed.netloc.endswith("youtu.be"):
        candidate = parsed.path.lstrip("/")
        return candidate if is_valid_youtube_video_id(candidate) else None
    if "youtube.com" in parsed.netloc:
        candidate = parse_qs(parsed.query).get("v", [None])[0]
        return candidate if is_valid_youtube_video_id(candidate) else None
    return None


def resolve_asset_id_for_video_id(session: Session, video_id: str) -> str | None:
    """Look up the Brain asset ID that corresponds to a YouTube video ID."""
    asset = session.scalar(select(Asset).where(Asset.youtube_video_id == video_id))
    return asset.id if asset else None


def first_present(row: dict[str, Any], keys: list[str]) -> Any:
    """Return the first non-empty value from *row* for the given *keys*."""
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None
