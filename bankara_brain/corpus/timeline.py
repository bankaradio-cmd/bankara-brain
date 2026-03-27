"""Timeline bootstrap, import, and validation helpers."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Optional

from sqlalchemy import delete, select
from sqlalchemy.orm import Session, sessionmaker

from bankara_brain.models import Asset, TimelineSegment
from bankara_brain.corpus.query import resolve_asset, select_assets_for_filters
from bankara_brain.ingest.transcript import load_transcript_segments
from bankara_brain.utils import format_seconds_hms, parse_float, safe_json_load, shorten_text
from bankara_brain.youtube.helpers import first_present
from bankara_brain.utils import build_text_chunks


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def resolve_bootstrap_assets(session: Session, asset_selector: Optional[str]) -> list[Asset]:
    if asset_selector:
        return [resolve_asset(session, asset_selector)]
    return session.scalars(
        select(Asset)
        .where(Asset.media_type.in_(("audio", "video")))
        .order_by(Asset.relative_path)
    ).all()


def import_shot_timeline(
    session_factory: sessionmaker[Session],
    asset_selector: str,
    timeline_path: Path,
    replace: bool,
) -> None:
    segments = load_timeline_segments_file(timeline_path)
    if not segments:
        raise ValueError(f"No timeline segments found in {timeline_path}")

    with session_factory() as session:
        asset = resolve_asset(session, asset_selector)
        replace_timeline_segments(session, asset, segments, replace=replace)
        session.commit()

    print(f"Imported timeline segments: {len(segments)}")


def bootstrap_shot_timeline(
    session_factory: sessionmaker[Session],
    asset_selector: Optional[str],
    replace: bool,
    max_segment_seconds: float,
    min_segment_seconds: float,
    gap_seconds: float,
    target_chars: int,
) -> None:
    with session_factory() as session:
        assets = resolve_bootstrap_assets(session, asset_selector)
        created_assets = 0
        skipped_assets = 0
        created_segments = 0

        for asset in assets:
            existing_segments = session.scalars(
                select(TimelineSegment)
                .where(TimelineSegment.asset_id == asset.id)
                .order_by(TimelineSegment.segment_index)
            ).all()
            if existing_segments and not replace:
                print(f"Skipping existing timeline: {asset.relative_path}")
                skipped_assets += 1
                continue

            transcript_segments = load_transcript_segments(session, asset.id)
            segments = build_bootstrap_timeline_segments(
                asset=asset,
                transcript_segments=transcript_segments,
                max_segment_seconds=max_segment_seconds,
                min_segment_seconds=min_segment_seconds,
                gap_seconds=gap_seconds,
                target_chars=target_chars,
            )
            if not segments:
                print(f"Skipping without usable transcript timing: {asset.relative_path}")
                skipped_assets += 1
                continue

            replace_timeline_segments(session, asset, segments, replace=True)
            created_assets += 1
            created_segments += len(segments)
            print(f"Bootstrapped timeline: {asset.relative_path} segments={len(segments)}")

        session.commit()

    print(
        f"Bootstrap completed. assets={created_assets} segments={created_segments} skipped={skipped_assets}"
    )


def build_bootstrap_timeline_segments(
    asset: Asset,
    transcript_segments: list,
    max_segment_seconds: float,
    min_segment_seconds: float,
    gap_seconds: float,
    target_chars: int,
) -> list[dict[str, Any]]:
    cues, timing_mode = build_transcript_cues(
        asset,
        transcript_segments,
        fallback_segment_seconds=min(max_segment_seconds, 3.0),
    )
    if not cues:
        return []

    grouped: list[list[dict[str, Any]]] = []
    current_group: list[dict[str, Any]] = []
    current_chars = 0

    for cue in cues:
        if not current_group:
            current_group = [cue]
            current_chars = len(cue["text"])
            continue

        current_start = current_group[0]["start_seconds"]
        current_end = current_group[-1]["end_seconds"]
        proposed_end = max(current_end, cue["end_seconds"])
        proposed_chars = current_chars + len(cue["text"])
        current_duration = current_end - current_start
        proposed_duration = proposed_end - current_start
        gap = cue["start_seconds"] - current_end

        should_split = False
        if gap > gap_seconds:
            should_split = True
        elif proposed_duration > max_segment_seconds and current_duration >= min_segment_seconds:
            should_split = True
        elif proposed_chars > target_chars and current_duration >= min_segment_seconds:
            should_split = True

        if should_split:
            grouped.append(current_group)
            current_group = [cue]
            current_chars = len(cue["text"])
            continue

        current_group.append(cue)
        current_chars = proposed_chars

    if current_group:
        grouped.append(current_group)

    total_groups = len(grouped)
    segments = []
    for index, group in enumerate(grouped):
        start_seconds = group[0]["start_seconds"]
        end_seconds = group[-1]["end_seconds"]
        transcript = " ".join(item["text"] for item in group).strip()
        segment_kind = infer_bootstrap_segment_kind(index, total_groups)
        label = segment_kind if total_groups > 1 else "full"
        segments.append(
            {
                "segment_kind": segment_kind,
                "label": label,
                "start_seconds": start_seconds,
                "end_seconds": end_seconds,
                "transcript": transcript,
                "notes": "auto_bootstrap: transcript grouped",
                "metadata": {
                    "bootstrap_source": "transcript",
                    "timing_mode": timing_mode,
                    "cue_count": len(group),
                },
            }
        )

    validate_timeline_segments(segments)
    return segments


def build_transcript_cues(
    asset: Asset,
    transcript_segments: list,
    fallback_segment_seconds: float,
) -> tuple[list[dict[str, Any]], str]:
    timed_cues = []
    for segment in transcript_segments:
        if segment.start_seconds is None or segment.end_seconds is None:
            continue
        if segment.end_seconds <= segment.start_seconds:
            continue
        if not segment.text.strip():
            continue
        timed_cues.append(
            {
                "start_seconds": float(segment.start_seconds),
                "end_seconds": float(segment.end_seconds),
                "text": segment.text.strip(),
            }
        )
    if timed_cues:
        return timed_cues, "timed"

    if not transcript_segments:
        return [], "unavailable"

    if asset.duration_seconds and asset.duration_seconds > 0:
        synthetic_cues = []
        slice_duration = float(asset.duration_seconds) / len(transcript_segments)
        cursor = 0.0
        for segment in transcript_segments:
            text = segment.text.strip()
            if not text:
                continue
            start_seconds = cursor
            end_seconds = min(float(asset.duration_seconds), cursor + slice_duration)
            synthetic_cues.append(
                {
                    "start_seconds": start_seconds,
                    "end_seconds": end_seconds,
                    "text": text,
                }
            )
            cursor = end_seconds
        return synthetic_cues, "synthetic_even_split"

    estimated_cues = []
    cursor = 0.0
    for segment in transcript_segments:
        text = segment.text.strip()
        if not text:
            continue
        start_seconds = cursor
        end_seconds = cursor + fallback_segment_seconds
        estimated_cues.append(
            {
                "start_seconds": start_seconds,
                "end_seconds": end_seconds,
                "text": text,
            }
        )
        cursor = end_seconds
    return estimated_cues, "synthetic_estimated"


def infer_bootstrap_segment_kind(index: int, total_groups: int) -> str:
    if index == 0:
        return "hook"
    if index == total_groups - 1:
        return "payoff"
    return "beat"


def replace_timeline_segments(session: Session, asset: Asset, segments: list[dict[str, Any]], replace: bool) -> None:
    if replace:
        session.execute(delete(TimelineSegment).where(TimelineSegment.asset_id == asset.id))

    for index, segment in enumerate(segments):
        session.add(
            TimelineSegment(
                asset_id=asset.id,
                segment_index=index,
                segment_kind=segment["segment_kind"],
                label=segment["label"],
                start_seconds=segment["start_seconds"],
                end_seconds=segment["end_seconds"],
                transcript=segment["transcript"],
                notes=segment["notes"],
                metadata_json=json.dumps(segment["metadata"], ensure_ascii=False),
            )
        )


def load_timeline_segments_file(timeline_path: Path) -> list[dict[str, Any]]:
    if not timeline_path.exists():
        raise FileNotFoundError(f"Timeline file not found: {timeline_path}")

    suffix = timeline_path.suffix.lower()
    if suffix == ".json":
        payload = json.loads(timeline_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            rows = payload.get("segments", [])
        elif isinstance(payload, list):
            rows = payload
        else:
            raise ValueError("Timeline JSON must be an array or {\"segments\": [...]}")
    elif suffix == ".csv":
        with timeline_path.open("r", encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.DictReader(handle))
    else:
        raise ValueError("Timeline file must be .json or .csv")

    normalized = [normalize_timeline_segment_row(row, index) for index, row in enumerate(rows)]
    normalized.sort(key=lambda row: (row["start_seconds"], row["end_seconds"], row["segment_kind"]))
    validate_timeline_segments(normalized)
    return normalized


def normalize_timeline_segment_row(row: Any, index: int) -> dict[str, Any]:
    if not isinstance(row, dict):
        raise ValueError(f"Timeline segment at index {index} must be an object")

    start_seconds = parse_float(first_present(row, ["start_seconds", "start", "from"]))
    end_seconds = parse_float(first_present(row, ["end_seconds", "end", "to"]))
    if start_seconds is None or end_seconds is None:
        raise ValueError(f"Timeline segment at index {index} is missing start/end seconds")

    segment_kind = str(first_present(row, ["segment_kind", "kind", "type"]) or "shot")
    label = str(first_present(row, ["label", "name", "title"]) or "")
    transcript = str(first_present(row, ["transcript", "text", "caption"]) or "")
    notes = str(first_present(row, ["notes", "description"]) or "")

    metadata = {}
    for key, value in row.items():
        if key in {
            "start_seconds",
            "start",
            "from",
            "end_seconds",
            "end",
            "to",
            "segment_kind",
            "kind",
            "type",
            "label",
            "name",
            "title",
            "transcript",
            "text",
            "caption",
            "notes",
            "description",
        }:
            continue
        if value in (None, ""):
            continue
        metadata[key] = value

    return {
        "segment_kind": segment_kind,
        "label": label,
        "start_seconds": float(start_seconds),
        "end_seconds": float(end_seconds),
        "transcript": transcript,
        "notes": notes,
        "metadata": metadata,
    }


def validate_timeline_segments(segments: list[dict[str, Any]]) -> None:
    previous_end = 0.0
    for index, segment in enumerate(segments):
        start_seconds = segment["start_seconds"]
        end_seconds = segment["end_seconds"]
        if start_seconds < 0 or end_seconds <= start_seconds:
            raise ValueError(f"Invalid timeline segment at index {index}: start={start_seconds}, end={end_seconds}")
        if start_seconds < previous_end - 0.001:
            raise ValueError(
                f"Timeline segments overlap or are out of order at index {index}: "
                f"start={start_seconds}, previous_end={previous_end}"
            )
        previous_end = end_seconds


def list_timeline_segments(session_factory: sessionmaker[Session], asset_selector: str, limit: int) -> None:
    with session_factory() as session:
        asset = resolve_asset(session, asset_selector)
        segments = session.scalars(
            select(TimelineSegment)
            .where(TimelineSegment.asset_id == asset.id)
            .order_by(TimelineSegment.segment_index)
            .limit(limit)
        ).all()

        if not segments:
            print("No timeline segments found.")
            return

        for segment in segments:
            print(
                f"{segment.segment_index:03d} "
                f"{format_seconds_hms(segment.start_seconds)}-{format_seconds_hms(segment.end_seconds)} "
                f"{segment.segment_kind:<8} "
                f"label={segment.label!r} "
                f"notes={shorten_text(segment.notes or segment.transcript, 80)!r}"
            )
