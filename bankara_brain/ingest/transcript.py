"""Transcript loading, syncing, transcription, and synthetic transcript generation."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from bankara_brain.db import BlobStore
from bankara_brain.models import Asset, EmbeddingRecord, TextSegment, TimelineSegment
from bankara_brain.utils import find_sidecar_text_file, format_seconds_hms, safe_json_load, shorten_text
from bankara_brain.corpus.query import resolve_asset_media_path, resolve_existing_path, media_has_audio_stream
from bankara_brain.ingest.stage import build_transcript_excerpt, file_sha256, load_segments_into_db


# ── Transcript segment queries ──────────────────────────────────────────────


def load_transcript_segments(session: Session, asset_id: str) -> list[TextSegment]:
    """Load transcript TextSegments for an asset, ordered by chunk_index."""
    return session.scalars(
        select(TextSegment)
        .where(TextSegment.asset_id == asset_id)
        .where(TextSegment.segment_kind == "transcript")
        .order_by(TextSegment.chunk_index)
    ).all()


def load_transcript_window_text(
    transcript_segments: list[TextSegment],
    start_seconds: float,
    end_seconds: float,
) -> str:
    """Extract transcript text overlapping a time window."""
    overlapping = []
    untimed = []
    for segment in transcript_segments:
        if segment.start_seconds is None or segment.end_seconds is None:
            untimed.append(segment.text)
            continue
        if segment.end_seconds < start_seconds or segment.start_seconds > end_seconds:
            continue
        overlapping.append(segment.text)

    if overlapping:
        return shorten_text(" ".join(overlapping), 1200)
    if untimed:
        return shorten_text(" ".join(untimed[:2]), 1200)
    return ""


def load_existing_record_ids(session: Session, asset_id: str, namespace: str) -> set[str]:
    """Get already-embedded record IDs for an asset+namespace from the EmbeddingRecord table."""
    rows = session.scalars(
        select(EmbeddingRecord.record_id)
        .where(EmbeddingRecord.asset_id == asset_id)
        .where(EmbeddingRecord.namespace == namespace)
    ).all()
    return set(rows)


# ── Transcript path resolution ──────────────────────────────────────────────


def resolve_asset_transcript_path(asset: Asset) -> Path | None:
    """Find a transcript file — check storage_path then sidecar next to source media."""
    current_path = resolve_existing_path(asset.transcript_storage_path)
    if current_path:
        return current_path

    source_media_path = resolve_existing_path(asset.source_path)
    if source_media_path:
        sidecar_path = find_sidecar_text_file(source_media_path)
        if sidecar_path and sidecar_path.exists():
            return sidecar_path.resolve()

    return None


# ── Transcript segment DB mutations ─────────────────────────────────────────


def replace_transcript_segments(session: Session, asset_id: str) -> None:
    """Delete existing transcript TextSegments for an asset."""
    session.execute(
        delete(TextSegment)
        .where(TextSegment.asset_id == asset_id)
        .where(TextSegment.segment_kind == "transcript")
    )


def sync_asset_transcript(
    session: Session,
    blob_store: BlobStore,
    asset: Asset,
    transcript_path: Path,
) -> Path:
    """Load/parse a transcript file and save TextSegments into the database."""
    transcript_sha256 = file_sha256(transcript_path)
    staged_path = blob_store.stage_file(transcript_path, sha256_hex=transcript_sha256, copy_mode="copy")
    asset.transcript_storage_path = str(staged_path.resolve())
    asset.transcript_excerpt = build_transcript_excerpt(transcript_path)
    replace_transcript_segments(session, asset.id)
    media_path = resolve_asset_media_path(asset) or transcript_path
    load_segments_into_db(session, asset, media_path, transcript_path)
    session.add(asset)
    return staged_path.resolve()


# ── SRT formatting helpers ──────────────────────────────────────────────────


def format_seconds_srt(value: float) -> str:
    """Format a timestamp as SRT (HH:MM:SS,mmm)."""
    return format_seconds_hms(value).replace(".", ",")


# ── Synthetic transcript generation ─────────────────────────────────────────


def build_synthetic_transcript_line(asset: Asset, segment: TimelineSegment) -> str:
    """Generate a single synthetic transcript line from a timeline segment."""
    parts = [
        segment.transcript.strip(),
        segment.label.strip() if segment.label else "",
        segment.notes.strip() if segment.notes else "",
        segment.segment_kind.strip(),
    ]
    for part in parts:
        if part:
            return part
    return f"{asset.title} visual beat {segment.segment_index + 1}"


def build_synthetic_transcript_file(
    asset: Asset,
    output_root: Path,
    timeline_segments: list[TimelineSegment],
) -> Path:
    """Generate an SRT or TXT file from timeline segments when no real transcript exists."""
    asset_output_dir = output_root / asset.id
    asset_output_dir.mkdir(parents=True, exist_ok=True)

    timed_segments = [
        segment
        for segment in timeline_segments
        if segment.start_seconds is not None
        and segment.end_seconds is not None
        and segment.end_seconds > segment.start_seconds
    ]
    if timed_segments:
        transcript_path = asset_output_dir / f"{Path(asset.relative_path).stem}.synthetic.srt"
        lines: list[str] = []
        for cue_index, segment in enumerate(timed_segments, start=1):
            lines.extend(
                [
                    str(cue_index),
                    f"{format_seconds_srt(float(segment.start_seconds))} --> {format_seconds_srt(float(segment.end_seconds))}",
                    build_synthetic_transcript_line(asset, segment),
                    "",
                ]
            )
        transcript_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
        return transcript_path

    transcript_path = asset_output_dir / f"{Path(asset.relative_path).stem}.synthetic.txt"
    metadata = safe_json_load(asset.metadata_json)
    lines = [f"title: {asset.title}"]
    if asset.notes:
        lines.append(f"notes: {asset.notes}")
    if metadata.get("tags"):
        lines.append(f"tags: {', '.join(str(tag) for tag in metadata['tags'])}")
    if asset.channel:
        lines.append(f"channel: {asset.channel}")
    if asset.source_url:
        lines.append(f"source_url: {asset.source_url}")
    if timeline_segments:
        lines.append("")
        lines.append("visual beats:")
        for segment in timeline_segments:
            lines.append(f"- {build_synthetic_transcript_line(asset, segment)}")
    transcript_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    return transcript_path


# ── Transcription via faster-whisper ────────────────────────────────────────


def default_transcribe_script_path() -> Path:
    """Resolve the default faster-whisper transcription script path."""
    return Path.home() / ".agents" / "skills" / "faster-whisper" / "scripts" / "transcribe"


def transcribe_asset_with_faster_whisper(
    asset: Asset,
    transcribe_script: Path,
    output_root: Path,
    language: str | None,
    model_name: str | None,
) -> Path:
    """Run faster-whisper subprocess for audio-to-text transcription."""
    from bankara_brain.ingest.pipeline import run_logged_subprocess

    media_path = resolve_asset_media_path(asset)
    if media_path is None:
        raise FileNotFoundError(f"Media file not found for asset: {asset.relative_path}")
    if not transcribe_script.exists():
        raise FileNotFoundError(f"faster-whisper script not found: {transcribe_script}")

    asset_output_dir = output_root / asset.id
    asset_output_dir.mkdir(parents=True, exist_ok=True)
    transcribe_input_path = media_path
    if asset.media_type == "video":
        has_audio_stream = media_has_audio_stream(media_path)
        if has_audio_stream is False:
            raise ValueError(f"Video has no audio stream: {asset.relative_path}")
        extracted_audio_path = asset_output_dir / f"{media_path.stem}.wav"
        extract_command = [
            "ffmpeg",
            "-y",
            "-i",
            str(media_path),
            "-vn",
            "-ac",
            "1",
            "-ar",
            "16000",
            str(extracted_audio_path),
        ]
        run_logged_subprocess(extract_command, cwd=Path(__file__).resolve().parent)
        transcribe_input_path = extracted_audio_path

    command = [
        str(transcribe_script),
        str(transcribe_input_path),
        "--format",
        "srt",
        "-o",
        str(asset_output_dir),
        "--quiet",
    ]
    if language:
        command.extend(["--language", language])
    if model_name:
        command.extend(["--model", model_name])

    run_logged_subprocess(command, cwd=Path(__file__).resolve().parent)

    transcripts = sorted(asset_output_dir.glob("*.srt"))
    if len(transcripts) != 1:
        raise RuntimeError(
            f"Expected exactly one SRT output for {asset.relative_path}, found {len(transcripts)} in {asset_output_dir}"
        )
    return transcripts[0].resolve()
