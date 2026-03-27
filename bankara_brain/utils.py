"""Bankara Brain — Media utilities and common helpers.

Consolidated from the former standalone ``bankara_media_utils.py`` plus
small parser helpers that were scattered across the control plane.
"""
from __future__ import annotations

import json
import mimetypes
import re
import shutil
import subprocess
import uuid
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

# ── Suffix & chunking constants ──────────────────────────────────────────────

TEXT_SUFFIXES = {".txt", ".md", ".srt", ".vtt"}
AUDIO_SUFFIXES = {".mp3", ".wav"}
VIDEO_SUFFIXES = {".mp4", ".mov"}
SUPPORTED_SUFFIXES = TEXT_SUFFIXES | AUDIO_SUFFIXES | VIDEO_SUFFIXES
TEXT_CHUNK_TARGET_CHARS = 2400
TEXT_CHUNK_OVERLAP_CHARS = 300
SUBTITLE_CHUNK_TARGET_CHARS = 1800
SIDECAR_JSON_SUFFIX = ".json"
YT_DLP_INFO_JSON_SUFFIX = ".info.json"


# ── Data classes ─────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class TextChunk:
    text: str
    chunk_index: int
    chunk_count: int
    start_seconds: float | None = None
    end_seconds: float | None = None


# ── Media type inference ─────────────────────────────────────────────────────

def infer_media_type_and_mime(file_path: Path, declared_media_type: str | None) -> tuple[str, str]:
    suffix = file_path.suffix.lower()
    guessed_mime, _ = mimetypes.guess_type(file_path.name)

    if declared_media_type is None:
        if suffix in AUDIO_SUFFIXES:
            declared_media_type = "audio"
        elif suffix in VIDEO_SUFFIXES:
            declared_media_type = "video"
        elif guessed_mime and guessed_mime.startswith("audio/"):
            declared_media_type = "audio"
        elif guessed_mime and guessed_mime.startswith("video/"):
            declared_media_type = "video"
        else:
            raise ValueError("Could not infer media type. Pass --media-type audio or --media-type video.")

    if declared_media_type == "audio":
        if suffix == ".mp3":
            return "audio", "audio/mpeg"
        if suffix == ".wav":
            return "audio", "audio/wav"
        raise ValueError("Audio embedding supports .mp3 or .wav files in this script.")

    if declared_media_type == "video":
        if suffix == ".mp4":
            return "video", "video/mp4"
        if suffix == ".mov":
            return "video", "video/quicktime"
        raise ValueError("Video embedding supports .mp4 or .mov files in this script.")

    raise ValueError(f"Unsupported media type: {declared_media_type}")


def infer_record_kind(file_path: Path) -> str:
    suffix = file_path.suffix.lower()
    if suffix in TEXT_SUFFIXES:
        return "text"
    if suffix in AUDIO_SUFFIXES:
        return "audio"
    if suffix in VIDEO_SUFFIXES:
        return "video"
    raise ValueError(f"Unsupported file extension: {suffix}")


# ── ffprobe duration ─────────────────────────────────────────────────────────

def probe_media_duration(file_path: Path) -> float | None:
    ffprobe_path = shutil.which("ffprobe")
    if not ffprobe_path:
        return None

    result = subprocess.run(
        [
            ffprobe_path,
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(file_path),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None

    try:
        return float(result.stdout.strip())
    except ValueError:
        return None


# ── Text helpers ─────────────────────────────────────────────────────────────

def humanize_stem(stem: str) -> str:
    return re.sub(r"[_\-]+", " ", stem).strip()


def normalize_whitespace(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


def shorten_text(text: str, limit: int) -> str:
    text = normalize_whitespace(text)
    if len(text) <= limit:
        return text
    return f"{text[:limit - 3].rstrip()}..."


def load_text_file(file_path: Path) -> str:
    return file_path.read_text(encoding="utf-8", errors="replace").strip()


# ── Sidecar metadata ────────────────────────────────────────────────────────

def load_sidecar_metadata(file_path: Path) -> dict[str, Any]:
    stem_base = file_path.with_suffix("")
    candidates = [
        Path(str(file_path) + SIDECAR_JSON_SUFFIX),
        file_path.with_suffix(SIDECAR_JSON_SUFFIX),
        stem_base.with_suffix(YT_DLP_INFO_JSON_SUFFIX),
    ]
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            try:
                data = json.loads(candidate.read_text(encoding="utf-8"))
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid sidecar JSON: {candidate} ({exc})") from exc
            if not isinstance(data, dict):
                raise ValueError(f"Sidecar metadata must be a JSON object: {candidate}")
            return data
    return {}


def normalize_sidecar_metadata(raw: dict[str, Any]) -> dict[str, Any]:
    if not raw:
        return {}

    # yt-dlp writes rich `.info.json` files. Map the common fields into the
    # normalized sidecar schema so stage-dataset can ingest downloaded YouTube
    # videos without any extra manual metadata editing.
    if "video_id" not in raw and raw.get("id"):
        raw = dict(raw)
        raw["video_id"] = raw.get("id")
    if "source_url" not in raw and raw.get("webpage_url"):
        raw = dict(raw)
        raw["source_url"] = raw.get("webpage_url")
    if "published_at" not in raw:
        upload_date = raw.get("upload_date") or raw.get("release_date")
        if isinstance(upload_date, str) and len(upload_date) == 8 and upload_date.isdigit():
            raw = dict(raw)
            raw["published_at"] = f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:8]}"
        elif raw.get("timestamp"):
            raw = dict(raw)
            raw["published_at"] = raw.get("timestamp")
    if "channel" not in raw:
        channel = raw.get("channel") or raw.get("channel_name") or raw.get("uploader")
        if channel:
            raw = dict(raw)
            raw["channel"] = channel
    if "tags" not in raw:
        categories = raw.get("categories")
        if isinstance(categories, list) and categories:
            raw = dict(raw)
            raw["tags"] = categories

    metadata: dict[str, Any] = {}
    for key in [
        "title",
        "notes",
        "description",
        "channel",
        "published_at",
        "video_id",
        "episode_id",
        "speaker",
        "source_url",
    ]:
        value = raw.get(key)
        if value not in (None, ""):
            metadata[key] = value

    tags = raw.get("tags")
    if isinstance(tags, list) and all(isinstance(item, str) for item in tags):
        metadata["tags"] = tags
    elif isinstance(tags, str) and tags.strip():
        metadata["tags"] = [part.strip() for part in tags.split(",") if part.strip()]

    extra = {}
    for key, value in raw.items():
        if key == "tags":
            continue
        if key in {
            "title",
            "notes",
            "description",
            "channel",
            "published_at",
            "video_id",
            "episode_id",
            "speaker",
            "source_url",
        }:
            continue
        if value in (None, ""):
            continue
        extra[key] = value
    if extra:
        metadata["sidecar_json"] = extra

    return metadata


def find_sidecar_text_file(file_path: Path) -> Path | None:
    stem_base = file_path.with_suffix("")
    candidates = []
    for suffix in [".srt", ".vtt", ".txt", ".md"]:
        candidates.append(stem_base.with_suffix(suffix))
        candidates.append(Path(str(file_path) + suffix))
        candidates.extend(sorted(stem_base.parent.glob(f"{stem_base.name}.*{suffix}")))
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


# ── Text chunking ────────────────────────────────────────────────────────────

def build_text_chunks(file_path: Path) -> list[TextChunk]:
    raw_text = load_text_file(file_path)
    if not raw_text:
        return []

    suffix = file_path.suffix.lower()
    if suffix in {".srt", ".vtt"}:
        return build_subtitle_chunks(raw_text)
    return build_plain_text_chunks(raw_text)


def build_plain_text_chunks(raw_text: str) -> list[TextChunk]:
    text = normalize_whitespace(raw_text)
    if not text:
        return []

    paragraphs = [part.strip() for part in re.split(r"\n\s*\n", text) if part.strip()]
    if not paragraphs:
        paragraphs = [text]

    chunks: list[str] = []
    current = ""
    for paragraph in paragraphs:
        candidate = paragraph if not current else f"{current}\n\n{paragraph}"
        if len(candidate) <= TEXT_CHUNK_TARGET_CHARS:
            current = candidate
            continue

        if current:
            chunks.append(current.strip())
            overlap = current[-TEXT_CHUNK_OVERLAP_CHARS:].strip()
            current = f"{overlap}\n\n{paragraph}".strip() if overlap else paragraph
        else:
            chunks.extend(split_long_text(paragraph, TEXT_CHUNK_TARGET_CHARS, TEXT_CHUNK_OVERLAP_CHARS))
            current = ""

    if current:
        chunks.append(current.strip())

    final_chunks = []
    total = len(chunks)
    for index, chunk_text in enumerate(chunks):
        final_chunks.append(TextChunk(text=chunk_text, chunk_index=index, chunk_count=total))
    return final_chunks


def split_long_text(text: str, target_chars: int, overlap_chars: int) -> list[str]:
    text = text.strip()
    if len(text) <= target_chars:
        return [text]

    chunks: list[str] = []
    start = 0
    while start < len(text):
        end = min(len(text), start + target_chars)
        if end < len(text):
            soft_break = max(
                text.rfind("\n", start + int(target_chars * 0.6), end),
                text.rfind("。", start + int(target_chars * 0.6), end),
                text.rfind(".", start + int(target_chars * 0.6), end),
                text.rfind(" ", start + int(target_chars * 0.6), end),
            )
            if soft_break > start:
                end = soft_break + 1

        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        if end >= len(text):
            break
        start = max(end - overlap_chars, start + 1)

    return chunks


def build_subtitle_chunks(raw_text: str) -> list[TextChunk]:
    cues = parse_subtitle_cues(raw_text)
    if not cues:
        return build_plain_text_chunks(raw_text)

    groups: list[list[dict[str, Any]]] = []
    current_group: list[dict[str, Any]] = []
    current_size = 0

    for cue in cues:
        cue_length = len(cue["text"])
        if current_group and current_size + cue_length > SUBTITLE_CHUNK_TARGET_CHARS:
            groups.append(current_group)
            current_group = []
            current_size = 0
        current_group.append(cue)
        current_size += cue_length

    if current_group:
        groups.append(current_group)

    chunks: list[TextChunk] = []
    total = len(groups)
    for index, group in enumerate(groups):
        text = " ".join(cue["text"] for cue in group).strip()
        chunks.append(
            TextChunk(
                text=text,
                chunk_index=index,
                chunk_count=total,
                start_seconds=group[0]["start_seconds"],
                end_seconds=group[-1]["end_seconds"],
            )
        )
    return chunks


# ── Subtitle parsing ─────────────────────────────────────────────────────────

def parse_subtitle_cues(raw_text: str) -> list[dict[str, Any]]:
    raw_text = raw_text.replace("\r\n", "\n").replace("\r", "\n")
    blocks = re.split(r"\n\s*\n", raw_text)
    cues: list[dict[str, Any]] = []

    for block in blocks:
        lines = [line.strip() for line in block.split("\n") if line.strip()]
        if not lines:
            continue
        if lines[0].upper() == "WEBVTT":
            continue
        if lines[0].startswith(("NOTE", "STYLE", "REGION")):
            continue

        timing_index = next((idx for idx, line in enumerate(lines) if "-->" in line), None)
        if timing_index is None:
            continue

        start_seconds, end_seconds = parse_time_range(lines[timing_index])
        if start_seconds is None or end_seconds is None:
            continue

        text_lines = lines[timing_index + 1 :]
        if not text_lines:
            continue

        text = clean_subtitle_text(" ".join(text_lines))
        if not text:
            continue

        cues.append(
            {
                "start_seconds": start_seconds,
                "end_seconds": end_seconds,
                "text": text,
            }
        )

    return cues


def parse_time_range(line: str) -> tuple[float | None, float | None]:
    left, sep, right = line.partition("-->")
    if not sep:
        return None, None
    start_seconds = parse_timestamp(left.strip().split(" ")[0])
    right_token = right.strip().split(" ")[0]
    end_seconds = parse_timestamp(right_token)
    return start_seconds, end_seconds


def parse_timestamp(value: str) -> float | None:
    value = value.strip().replace(",", ".")
    if not value:
        return None
    parts = value.split(":")
    try:
        if len(parts) == 3:
            hours = int(parts[0])
            minutes = int(parts[1])
            seconds = float(parts[2])
            return hours * 3600 + minutes * 60 + seconds
        if len(parts) == 2:
            minutes = int(parts[0])
            seconds = float(parts[1])
            return minutes * 60 + seconds
    except ValueError:
        return None
    return None


def clean_subtitle_text(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\{[^}]+\}", " ", text)
    text = re.sub(r"\[[^\]]+\]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


# ── Manifest / record ID ────────────────────────────────────────────────────

def build_manifest_record_id(payload: dict[str, Any]) -> str:
    explicit_record_id = payload.get("record_id")
    if explicit_record_id:
        return explicit_record_id

    asset_id = payload.get("asset_id")
    entry_type = payload.get("entry_type")
    media_type = payload.get("media_type") or "item"
    if entry_type == "text_chunk" and asset_id is not None and payload.get("chunk_index") is not None:
        return f"text-{asset_id}-{int(payload['chunk_index']):04d}"
    if entry_type == "timeline_segment" and asset_id is not None and payload.get("segment_index") is not None:
        return f"{media_type}-segment-{asset_id}-{int(payload['segment_index']):04d}"
    if asset_id is not None:
        return f"{media_type}-{asset_id}"
    return f"{media_type}-{uuid.uuid4().hex}"


# ── Generic parse helpers (from control plane) ───────────────────────────────

def parse_date_value(value: str | None) -> date:
    """Parse an ISO-format date string, raising ValueError if empty."""
    if not value:
        raise ValueError("date value is required")
    return date.fromisoformat(str(value))


def parse_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(float(value))


def parse_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def safe_json_load(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        loaded = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return loaded if isinstance(loaded, dict) else {}


# ── Time / int helpers (moved from control plane) ────────────────────────────

def format_seconds_hms(value: float) -> str:
    """Format *value* seconds as ``HH:MM:SS.mmm``."""
    total_milliseconds = int(round(value * 1000))
    total_seconds, milliseconds = divmod(total_milliseconds, 1000)
    minutes, seconds = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}.{milliseconds:03d}"


def safe_int(value: Any) -> int | None:
    """Parse *value* as an ``int``, returning ``None`` on failure."""
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
