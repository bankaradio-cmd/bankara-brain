"""Media file preparation, trimming, and clip extraction for embedding."""

from __future__ import annotations

import hashlib
import re
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any

from google import genai

from bankara_brain.utils import SUPPORTED_SUFFIXES, infer_media_type_and_mime

from bankara_brain.embedding.config import (
    PreparedMedia,
    VIDEO_DURATION_LIMIT_SECONDS,
    AUDIO_DURATION_LIMIT_SECONDS,
    FILE_READY_TIMEOUT_SECONDS,
)
from bankara_brain.embedding.client import _get_attr


def wait_for_uploaded_file_ready(client: genai.Client, uploaded_file: Any) -> Any:
    name = _get_attr(uploaded_file, "name")
    if not name:
        return uploaded_file

    state_name = file_state_name(uploaded_file)
    if state_name in {None, "ACTIVE"}:
        return uploaded_file

    deadline = time.time() + FILE_READY_TIMEOUT_SECONDS
    while time.time() < deadline:
        current = client.files.get(name=name)
        state_name = file_state_name(current)
        if state_name == "ACTIVE":
            return current
        if state_name in {"FAILED", "ERROR"}:
            raise RuntimeError(f"Gemini Files API processing failed for {name}: state={state_name}")
        time.sleep(2)

    raise TimeoutError(f"Timed out waiting for Gemini uploaded file to become ACTIVE: {name}")


def file_state_name(file_obj: Any) -> str | None:
    state = _get_attr(file_obj, "state")
    if state is None:
        return None
    if isinstance(state, str):
        return state
    return getattr(state, "name", str(state))


def stable_file_id(prefix: str, file_path: Path, root_dir: Path) -> str:
    relative_path = str(file_path.relative_to(root_dir))
    digest = hashlib.sha1(relative_path.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}-{digest}"


def stable_chunk_id(prefix: str, file_path: Path, root_dir: Path, chunk_index: int) -> str:
    return f"{stable_file_id(prefix, file_path, root_dir)}-{chunk_index:04d}"


def iter_supported_files(root_dir: Path, recursive: bool) -> list[Path]:
    iterator = root_dir.rglob("*") if recursive else root_dir.glob("*")
    files = [path for path in iterator if path.is_file() and path.suffix.lower() in SUPPORTED_SUFFIXES]
    return sorted(files)


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


def prepare_media_for_embedding(
    file_path: Path,
    declared_media_type: str | None,
    allow_trim_long_media: bool,
    temp_dir: Path,
) -> PreparedMedia:
    media_type, mime_type = infer_media_type_and_mime(file_path, declared_media_type)
    duration_seconds = probe_media_duration(file_path)
    limit_seconds = VIDEO_DURATION_LIMIT_SECONDS if media_type == "video" else AUDIO_DURATION_LIMIT_SECONDS

    if duration_seconds is None or duration_seconds <= limit_seconds:
        return PreparedMedia(
            source_path=file_path,
            embed_path=file_path,
            media_type=media_type,
            mime_type=mime_type,
            source_duration_seconds=duration_seconds,
            embed_duration_seconds=duration_seconds,
            was_trimmed=False,
        )

    if not allow_trim_long_media:
        raise ValueError(
            f"{media_type} duration is {duration_seconds:.1f}s, exceeding the limit of "
            f"{limit_seconds:.1f}s and --no-trim-long-media is active."
        )

    embed_path = trim_media_to_limit(file_path, media_type, limit_seconds, temp_dir)
    _, embed_mime_type = infer_media_type_and_mime(embed_path, media_type)
    embed_duration_seconds = probe_media_duration(embed_path) or limit_seconds
    return PreparedMedia(
        source_path=file_path,
        embed_path=embed_path,
        media_type=media_type,
        mime_type=embed_mime_type,
        source_duration_seconds=duration_seconds,
        embed_duration_seconds=embed_duration_seconds,
        was_trimmed=True,
    )


def prepare_media_clips_for_full_embedding(
    file_path: Path,
    declared_media_type: str | None,
    temp_dir: Path,
) -> list[tuple[PreparedMedia, float, float]]:
    """Split a media file into clips covering the FULL duration for embedding.

    Short files (within the Gemini limit) return a single clip.
    Long files are split into sequential clips of at most 120s (video) or 80s (audio)
    so that the entire file is embedded — nothing is discarded.

    Returns a list of (PreparedMedia, clip_start_sec, clip_end_sec) tuples.
    """
    media_type, mime_type = infer_media_type_and_mime(file_path, declared_media_type)
    duration_seconds = probe_media_duration(file_path)
    limit_seconds = VIDEO_DURATION_LIMIT_SECONDS if media_type == "video" else AUDIO_DURATION_LIMIT_SECONDS

    # Short file or unknown duration: return as-is (single clip)
    if duration_seconds is None or duration_seconds <= limit_seconds:
        prepared = PreparedMedia(
            source_path=file_path,
            embed_path=file_path,
            media_type=media_type,
            mime_type=mime_type,
            source_duration_seconds=duration_seconds,
            embed_duration_seconds=duration_seconds,
            was_trimmed=False,
        )
        return [(prepared, 0.0, duration_seconds or 0.0)]

    # Long file: split into sequential clips covering the entire duration
    return extract_segment_clips(
        source_path=file_path,
        start_seconds=0.0,
        end_seconds=duration_seconds,
        temp_dir=temp_dir,
        media_type=media_type,
    )


def trim_media_to_limit(file_path: Path, media_type: str, limit_seconds: float, temp_dir: Path) -> Path:
    ffmpeg_path = shutil.which("ffmpeg")
    if not ffmpeg_path:
        raise RuntimeError(
            f"ffmpeg is required to trim long {media_type} files automatically, but it was not found."
        )

    suffix = file_path.suffix.lower()
    output_name = f"{file_path.stem}.embedclip{suffix}"
    output_path = temp_dir / output_name

    if media_type == "video":
        cmd = [
            ffmpeg_path,
            "-y",
            "-i",
            str(file_path),
            "-t",
            str(limit_seconds),
            "-c:v",
            "libx264",
            "-preset",
            "fast",
            "-crf",
            "23",
            "-c:a",
            "aac",
        ]
        if suffix == ".mp4":
            cmd.extend(["-movflags", "faststart"])
        cmd.append(str(output_path))
    else:
        cmd = [
            ffmpeg_path,
            "-y",
            "-i",
            str(file_path),
            "-t",
            str(limit_seconds),
        ]
        if suffix == ".mp3":
            cmd.extend(["-codec:a", "libmp3lame", "-q:a", "2"])
        else:
            cmd.extend(["-acodec", "pcm_s16le"])
        cmd.append(str(output_path))

    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise RuntimeError(f"ffmpeg trim failed for {file_path}: {result.stderr.strip()}")

    return output_path


def _extract_single_clip(
    ffmpeg_path: str,
    source_path: Path,
    start_seconds: float,
    duration_seconds: float,
    temp_dir: Path,
    media_type: str,
    mime_type: str,
    clip_index: int,
) -> PreparedMedia:
    """Internal helper: extract one clip from source via ffmpeg."""
    suffix = source_path.suffix.lower()
    safe_stem = re.sub(r"[^a-zA-Z0-9_\-]", "_", source_path.stem)[:40]
    clip_name = f"{safe_stem}_seg_{start_seconds:.1f}_c{clip_index}{suffix}"
    output_path = temp_dir / clip_name

    if media_type == "video":
        cmd = [
            ffmpeg_path, "-y",
            "-ss", str(start_seconds),
            "-i", str(source_path),
            "-t", str(duration_seconds),
            "-c:v", "libx264", "-preset", "fast", "-crf", "23",
            "-c:a", "aac",
        ]
        if suffix == ".mp4":
            cmd.extend(["-movflags", "faststart"])
    else:
        cmd = [
            ffmpeg_path, "-y",
            "-ss", str(start_seconds),
            "-i", str(source_path),
            "-t", str(duration_seconds),
        ]
        if suffix == ".mp3":
            cmd.extend(["-codec:a", "libmp3lame", "-q:a", "2"])
        else:
            cmd.extend(["-acodec", "pcm_s16le"])
    cmd.append(str(output_path))

    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise RuntimeError(
            f"ffmpeg clip extraction failed for {source_path} "
            f"[ss={start_seconds:.1f}s t={duration_seconds:.1f}s]: {result.stderr[:500]}"
        )

    embed_duration = probe_media_duration(output_path) or duration_seconds
    return PreparedMedia(
        source_path=source_path,
        embed_path=output_path,
        media_type=media_type,
        mime_type=mime_type,
        source_duration_seconds=duration_seconds,
        embed_duration_seconds=embed_duration,
        was_trimmed=False,
    )


def extract_segment_clips(
    source_path: Path,
    start_seconds: float,
    end_seconds: float,
    temp_dir: Path,
    media_type: str = "video",
) -> list[tuple[PreparedMedia, float, float]]:
    """Extract clips covering the full segment range.

    If the segment is longer than the Gemini Embedding 2 limit (120s for video,
    80s for audio), the segment is split into multiple sequential clips so that
    the **entire** segment is embedded — nothing is discarded.

    Returns a list of (PreparedMedia, clip_start_sec, clip_end_sec) tuples.
    """
    ffmpeg_path = shutil.which("ffmpeg")
    if not ffmpeg_path:
        raise RuntimeError("ffmpeg is required for segment clip extraction")

    raw_duration = end_seconds - start_seconds
    if raw_duration <= 0:
        raise ValueError(f"Invalid segment range: {start_seconds}s - {end_seconds}s")

    limit_seconds = VIDEO_DURATION_LIMIT_SECONDS if media_type == "video" else AUDIO_DURATION_LIMIT_SECONDS
    _, mime_type = infer_media_type_and_mime(source_path, media_type)

    # Split the segment into clips of at most limit_seconds each
    clips: list[tuple[PreparedMedia, float, float]] = []
    clip_start = start_seconds
    clip_index = 0
    while clip_start < end_seconds:
        clip_end = min(clip_start + limit_seconds, end_seconds)
        clip_duration = clip_end - clip_start
        # Skip very short trailing clips (< 2 seconds)
        if clip_duration < 2.0 and clip_index > 0:
            break

        prepared = _extract_single_clip(
            ffmpeg_path=ffmpeg_path,
            source_path=source_path,
            start_seconds=clip_start,
            duration_seconds=clip_duration,
            temp_dir=temp_dir,
            media_type=media_type,
            mime_type=mime_type,
            clip_index=clip_index,
        )
        clips.append((prepared, clip_start, clip_end))
        clip_start = clip_end
        clip_index += 1

    return clips
