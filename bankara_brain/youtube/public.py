"""Public YouTube catalog: list and download videos via yt-dlp (no OAuth required)."""

from __future__ import annotations

import json
import logging
import shlex
import shutil
import subprocess
from collections import Counter
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

from bankara_brain.corpus.query import normalize_filter_values, normalize_match_text
from bankara_brain.youtube.helpers import is_valid_youtube_video_id


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_BANKARA_PUBLIC_CHANNEL_URL = "https://www.youtube.com/channel/UCT5BVYrrhS7gD5xzloZ8FhA/videos"
DEFAULT_BANKARA_PUBLIC_CHANNEL_ID = "UCT5BVYrrhS7gD5xzloZ8FhA"
DEFAULT_BANKARA_PUBLIC_EXCLUDE_KEYWORDS = (
    "ライブ",
    "生配信",
    "配信",
    "ルーティン",
    "替え歌",
    "mv",
    "music video",
    "shorts",
    "切り抜き",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def require_yt_dlp_path() -> str:
    yt_dlp_path = shutil.which("yt-dlp")
    if not yt_dlp_path:
        raise RuntimeError("yt-dlp is required for public YouTube download flows but was not found in PATH.")
    return yt_dlp_path


def run_subprocess_checked(command: list[str], cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command,
        cwd=str(cwd) if cwd else None,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        stdout = (result.stdout or "").strip()
        detail = stderr or stdout or f"exit={result.returncode}"
        raise RuntimeError(f"Command failed: {shlex.join(command)} :: {detail}")
    return result


def fetch_public_youtube_catalog(channel_url: str) -> list[dict[str, Any]]:
    yt_dlp_path = require_yt_dlp_path()
    command = [yt_dlp_path, "--flat-playlist", "--dump-single-json", channel_url]
    result = run_subprocess_checked(command)
    payload = json.loads(result.stdout)
    channel_title = payload.get("channel") or payload.get("title")
    channel_id = payload.get("id")

    rows: list[dict[str, Any]] = []
    for entry in payload.get("entries") or []:
        video_id = entry.get("id")
        if not is_valid_youtube_video_id(video_id):
            continue
        title = entry.get("title") or ""
        rows.append(
            {
                "video_id": video_id,
                "title": title,
                "normalized_title": normalize_match_text(title),
                "channel": entry.get("channel") or channel_title,
                "channel_id": entry.get("channel_id") or channel_id,
                "url": entry.get("url") or f"https://www.youtube.com/watch?v={video_id}",
                "ie_key": entry.get("ie_key"),
            }
        )
    return rows


def filter_public_youtube_catalog(
    entries: list[dict[str, Any]],
    video_ids: list[str] | None,
    title_contains: list[str] | None,
    include_keywords: list[str] | None,
    exclude_keywords: list[str] | None,
    limit: int | None,
) -> list[dict[str, Any]]:
    normalized_video_ids = {video_id.strip() for video_id in (video_ids or []) if video_id and video_id.strip()}
    normalized_title_filters = normalize_filter_values(title_contains)
    normalized_include = normalize_filter_values(include_keywords)
    normalized_exclude = normalize_filter_values(exclude_keywords)

    selected: list[dict[str, Any]] = []
    for entry in entries:
        if normalized_video_ids and entry["video_id"] not in normalized_video_ids:
            continue
        title_haystack = str(entry.get("title") or "").casefold()
        if normalized_title_filters and not all(needle in title_haystack for needle in normalized_title_filters):
            continue
        if normalized_include and not any(keyword in title_haystack for keyword in normalized_include):
            continue
        if normalized_exclude and any(keyword in title_haystack for keyword in normalized_exclude):
            continue
        selected.append(entry)
        if limit is not None and len(selected) >= limit:
            break
    return selected


def write_download_sidecar(media_path: Path, info_payload: dict[str, Any], channel_label: str | None) -> Path:
    from bankara_brain.utils import humanize_stem

    upload_date = info_payload.get("upload_date")
    published_at = None
    if isinstance(upload_date, str) and len(upload_date) == 8 and upload_date.isdigit():
        published_at = f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:8]}"

    metadata = {
        "title": info_payload.get("title") or humanize_stem(media_path.stem),
        "description": info_payload.get("description") or "",
        "notes": info_payload.get("description") or "",
        "channel": channel_label or info_payload.get("channel") or info_payload.get("uploader"),
        "published_at": published_at or info_payload.get("release_date") or info_payload.get("timestamp"),
        "video_id": info_payload.get("id"),
        "source_url": info_payload.get("webpage_url") or f"https://www.youtube.com/watch?v={info_payload.get('id')}",
        "tags": info_payload.get("tags") or info_payload.get("categories") or [],
    }
    sidecar_path = Path(str(media_path) + ".json")
    sidecar_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    return sidecar_path


def find_downloaded_media_path(output_dir: Path, video_id: str) -> Path | None:
    for candidate in sorted(output_dir.glob(f"*_{video_id}_*.mp4")):
        if candidate.is_file():
            return candidate
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def list_public_youtube_videos(
    channel_url: str,
    limit: int,
    title_contains: list[str] | None,
    include_keywords: list[str] | None,
    exclude_keywords: list[str] | None,
    json_output: Path | None = None,
) -> None:
    rows = fetch_public_youtube_catalog(channel_url)
    selected = filter_public_youtube_catalog(
        entries=rows,
        video_ids=[],
        title_contains=title_contains,
        include_keywords=include_keywords,
        exclude_keywords=exclude_keywords,
        limit=limit,
    )

    if json_output:
        json_output.parent.mkdir(parents=True, exist_ok=True)
        json_output.write_text(json.dumps(selected, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Wrote public YouTube catalog: {json_output}")

    if not selected:
        print("No public YouTube videos matched the requested filters.")
        return

    for row in selected:
        print(f"{row['video_id']} title={row['title']!r} channel={row.get('channel') or '-'}")


def download_public_youtube_videos(
    channel_url: str,
    output_dir: Path,
    video_ids: list[str] | None,
    title_contains: list[str] | None,
    include_keywords: list[str] | None,
    exclude_keywords: list[str] | None,
    limit: int | None,
    sub_langs: str,
    dry_run: bool,
    report_output: Path | None = None,
) -> None:
    from bankara_brain.corpus.curation import DEFAULT_BANKARA_CHANNEL
    from bankara_brain.utils import find_sidecar_text_file
    from bankara_brain.utils import safe_json_load

    output_dir.mkdir(parents=True, exist_ok=True)
    catalog_error: str | None = None
    try:
        catalog = fetch_public_youtube_catalog(channel_url)
    except Exception as exc:
        catalog = []
        catalog_error = str(exc)

    if catalog:
        selected = filter_public_youtube_catalog(
            entries=catalog,
            video_ids=video_ids,
            title_contains=title_contains,
            include_keywords=include_keywords,
            exclude_keywords=exclude_keywords,
            limit=limit,
        )
    else:
        normalized_video_ids = [video_id for video_id in (video_ids or []) if is_valid_youtube_video_id(video_id)]
        if catalog_error and normalized_video_ids:
            logger.warning("Public catalog fetch failed, falling back to explicit video ids only: %s", catalog_error)
        selected = [
            {
                "video_id": video_id,
                "title": f"youtube_{video_id}",
                "url": f"https://www.youtube.com/watch?v={video_id}",
                "channel": DEFAULT_BANKARA_CHANNEL,
                "channel_id": None,
            }
            for video_id in normalized_video_ids[: limit or None]
        ]

    if not selected:
        print("No public YouTube videos matched the requested download filters.")
        return

    yt_dlp_path = require_yt_dlp_path()
    rows: list[dict[str, Any]] = []
    channel_label = DEFAULT_BANKARA_CHANNEL if "UCT5BVYrrhS7gD5xzloZ8FhA" in channel_url else None

    for entry in selected:
        row: dict[str, Any] = {
            "video_id": entry["video_id"],
            "title": entry.get("title"),
            "url": entry.get("url"),
            "status": "pending",
        }
        if dry_run:
            row["status"] = "would_download"
            rows.append(row)
            print(f"download {entry['video_id']} title={entry.get('title')!r}")
            continue

        command = [
            yt_dlp_path,
            "--no-progress",
            "--write-info-json",
            "--write-auto-subs",
            "--sub-langs",
            sub_langs,
            "--convert-subs",
            "srt",
            "--merge-output-format",
            "mp4",
            "--format",
            "bestvideo[height<=720]+bestaudio/best[height<=720]/best",
            "--output",
            str(output_dir / "%(upload_date)s_%(id)s_%(title).180B.%(ext)s"),
            entry["url"],
        ]
        try:
            run_subprocess_checked(command)
        except Exception as exc:
            row["status"] = "download_failed"
            row["error"] = str(exc)
            rows.append(row)
            logger.error("Download failed: %s title=%r error=%s", entry['video_id'], entry.get('title'), exc)
            continue

        media_path = find_downloaded_media_path(output_dir, entry["video_id"])
        if media_path is None:
            row["status"] = "media_not_found"
            rows.append(row)
            print(f"failed  {entry['video_id']} title={entry.get('title')!r} error=media_not_found")
            continue

        info_path = media_path.with_suffix(".info.json")
        if info_path.exists():
            info_payload = safe_json_load(info_path.read_text(encoding="utf-8"))
            sidecar_path = write_download_sidecar(media_path, info_payload, channel_label=channel_label)
            row["sidecar_path"] = str(sidecar_path)
        transcript_path = find_sidecar_text_file(media_path)
        row["status"] = "downloaded"
        row["media_path"] = str(media_path)
        row["transcript_path"] = str(transcript_path) if transcript_path else None
        rows.append(row)
        print(
            f"saved   {entry['video_id']} file={media_path.name} "
            f"subs={'yes' if transcript_path else 'no'}"
        )

    if report_output:
        from bankara_brain.ingest.pipeline import write_jsonl_report_row

        report_output.parent.mkdir(parents=True, exist_ok=True)
        with report_output.open("w", encoding="utf-8") as handle:
            for row in rows:
                write_jsonl_report_row(handle, row)
        print(f"Wrote public download report: {report_output}")

    status_counts = Counter(row["status"] for row in rows)
    summary = " ".join(f"{key}={value}" for key, value in sorted(status_counts.items()))
    print(f"Public download summary: assets={len(rows)} {summary}".rstrip())
