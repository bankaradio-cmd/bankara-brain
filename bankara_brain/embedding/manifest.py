"""Embedding manifest building, import, and index helpers.

Extracted from ``bankara_brain_control_plane.py`` to keep the control-plane
CLI thin while making these utilities reusable from other modules.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from dotenv import load_dotenv

from bankara_brain.models import Asset, AssetCuration, EmbeddingRecord, TextSegment, TimelineSegment, now_utc
from bankara_brain.corpus.query import (
    asset_cohort,
    asset_matches_filters,
    asset_selection_status,
    asset_subcohort,
    effective_cohort_label,
    normalize_match_text,
    select_assets_for_filters,
)
from bankara_brain.ingest.transcript import load_transcript_segments, load_transcript_window_text, load_existing_record_ids
from bankara_brain.analysis.scoring import FEEDBACK_SCORE_FIELDS, load_latest_feedback_summary, load_latest_asset_feedback_summary
from bankara_brain.analysis.structured_summary import extract_structured_summary_text, render_structured_summary_text
from bankara_brain.utils import safe_json_load, format_seconds_hms, shorten_text

from bankara_brain.utils import build_manifest_record_id, humanize_stem, infer_media_type_and_mime, shorten_text as media_shorten_text


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BRAIN_VISUAL_AUDIO_SUMMARY_KEY = "brain_visual_audio_summary_v1"
BRAIN_SEARCHABLE_SUMMARY_V2_KEY = "brain_searchable_summary_v2"


# ---------------------------------------------------------------------------
# Manifest building
# ---------------------------------------------------------------------------


def export_embedding_manifest(
    session_factory: sessionmaker[Session],
    output_path: Path,
    namespace: str,
    limit: int | None,
    only_missing_embeddings: bool,
    channel: str | None,
    require_tags: list[str] | None,
    exclude_tags: list[str] | None,
    title_contains: list[str] | None,
    source_url_contains: list[str] | None,
    selection_status: str | None,
    cohort: str | None,
    subcohort: str | None = None,
) -> None:
    count = 0
    with session_factory() as session, output_path.open("w", encoding="utf-8") as handle:
        assets = session.scalars(select(Asset).order_by(Asset.relative_path)).all()
        for asset in assets:
            metadata = safe_json_load(asset.metadata_json)
            if not asset_matches_filters(
                asset=asset,
                metadata=metadata,
                channel=channel,
                require_tags=require_tags,
                exclude_tags=exclude_tags,
                title_contains=title_contains,
                source_url_contains=source_url_contains,
                selection_status=selection_status,
                cohort=cohort,
                subcohort=subcohort,
            ):
                continue
            asset_feedback_summary = load_latest_asset_feedback_summary(session, asset.id)
            existing_record_ids = load_existing_record_ids(session, asset.id, namespace) if only_missing_embeddings else set()
            if asset.media_type == "text":
                segments = session.scalars(
                    select(TextSegment)
                    .where(TextSegment.asset_id == asset.id)
                    .where(TextSegment.segment_kind == "source_text")
                    .order_by(TextSegment.chunk_index)
                ).all()
                for segment in segments:
                    manifest_metadata = enrich_curation_metadata(
                        {
                            **metadata,
                            "asset_id": asset.id,
                            "relative_path": asset.relative_path,
                            "source_path": asset.storage_path,
                            "chunk_start_seconds": segment.start_seconds,
                            "chunk_end_seconds": segment.end_seconds,
                            "embedding_kind": "text_chunk",
                        },
                        asset,
                    )
                    manifest_metadata = merge_feedback_metadata(
                        manifest_metadata,
                        asset_feedback_summary,
                        prefix="asset",
                        set_primary=True,
                    )
                    manifest_line = {
                        "entry_type": "text_chunk",
                        "asset_id": asset.id,
                        "title": asset.title,
                        "media_type": "text",
                        "text": segment.text,
                        "chunk_index": segment.chunk_index,
                        "chunk_count": segment.chunk_count,
                        "relative_path": asset.relative_path,
                        "source_path": asset.storage_path,
                        "namespace": namespace,
                        "metadata": manifest_metadata,
                    }
                    manifest_line["record_id"] = build_manifest_record_id(manifest_line)
                    if only_missing_embeddings and manifest_line["record_id"] in existing_record_ids:
                        continue
                    handle.write(json.dumps(manifest_line, ensure_ascii=False) + "\n")
                    count += 1
                    if limit is not None and count >= limit:
                        print(f"Wrote manifest entries: {count}")
                        return
            else:
                transcript_segments = load_transcript_segments(session, asset.id)
                timeline_segments = session.scalars(
                    select(TimelineSegment)
                    .where(TimelineSegment.asset_id == asset.id)
                    .order_by(TimelineSegment.segment_index)
                ).all()
                # Determine if we can use multimodal segment embedding
                # (requires the source video/audio file to exist on disk)
                source_file = Path(asset.storage_path).expanduser().resolve() if asset.storage_path else None
                use_multimodal_segments = (
                    asset.media_type in ("video", "audio")
                    and source_file is not None
                    and source_file.exists()
                )
                for segment in timeline_segments:
                    segment_text = build_timeline_segment_embedding_text(asset, metadata, segment, transcript_segments)
                    if not segment_text.strip():
                        continue
                    segment_feedback_summary = load_latest_feedback_summary(
                        session=session,
                        asset_id=asset.id,
                        scope_type="timeline_segment",
                        scope_key=str(segment.id),
                    )
                    manifest_metadata = enrich_curation_metadata(
                        {
                            **metadata,
                            "asset_id": asset.id,
                            "relative_path": asset.relative_path,
                            "source_path": asset.storage_path,
                            "transcript_storage_path": asset.transcript_storage_path,
                            "youtube_video_id": asset.youtube_video_id,
                            "source_url": asset.source_url,
                            "embedding_kind": "timeline_segment",
                            "timeline_segment_id": segment.id,
                            "timeline_segment_index": segment.segment_index,
                            "timeline_segment_kind": segment.segment_kind,
                            "timeline_label": segment.label,
                            "chunk_start_seconds": segment.start_seconds,
                            "chunk_end_seconds": segment.end_seconds,
                        },
                        asset,
                    )
                    manifest_metadata = merge_feedback_metadata(
                        manifest_metadata,
                        segment_feedback_summary,
                        prefix="segment",
                        set_primary=bool(segment_feedback_summary),
                    )
                    manifest_metadata = merge_feedback_metadata(
                        manifest_metadata,
                        asset_feedback_summary,
                        prefix="asset",
                        set_primary=not bool(segment_feedback_summary),
                    )

                    if use_multimodal_segments:
                        # Multimodal: Gemini Embedding 2 will see the actual
                        # video clip for this segment alongside text context
                        manifest_line = {
                            "entry_type": "segment_media",
                            "asset_id": asset.id,
                            "title": build_timeline_segment_title(asset.title, segment),
                            "media_type": asset.media_type,
                            "notes": segment_text,
                            "clip_start_seconds": segment.start_seconds,
                            "clip_end_seconds": segment.end_seconds,
                            "segment_index": segment.segment_index,
                            "relative_path": asset.relative_path,
                            "source_path": asset.storage_path,
                            "namespace": namespace,
                            "metadata": manifest_metadata,
                        }
                    else:
                        # Text-only fallback (no source file on disk)
                        manifest_line = {
                            "entry_type": "timeline_segment",
                            "asset_id": asset.id,
                            "title": build_timeline_segment_title(asset.title, segment),
                            "media_type": asset.media_type,
                            "text": segment_text,
                            "notes": build_timeline_segment_notes(segment),
                            "segment_index": segment.segment_index,
                            "relative_path": asset.relative_path,
                            "source_path": asset.storage_path,
                            "namespace": namespace,
                            "metadata": manifest_metadata,
                        }
                    manifest_line["record_id"] = build_manifest_record_id(manifest_line)
                    if only_missing_embeddings and manifest_line["record_id"] in existing_record_ids:
                        continue
                    handle.write(json.dumps(manifest_line, ensure_ascii=False) + "\n")
                    count += 1
                    if limit is not None and count >= limit:
                        print(f"Wrote manifest entries: {count}")
                        return

                notes_payload = build_media_manifest_notes(asset, metadata)
                manifest_metadata = enrich_curation_metadata(
                    {
                        **metadata,
                        "asset_id": asset.id,
                        "relative_path": asset.relative_path,
                        "source_path": asset.storage_path,
                        "transcript_storage_path": asset.transcript_storage_path,
                        "youtube_video_id": asset.youtube_video_id,
                        "source_url": asset.source_url,
                        "embedding_kind": "asset",
                        "timeline_segment_count": len(timeline_segments),
                    },
                    asset,
                )
                manifest_metadata = merge_feedback_metadata(
                    manifest_metadata,
                    asset_feedback_summary,
                    prefix="asset",
                    set_primary=True,
                )
                manifest_line = {
                    "entry_type": "media",
                    "asset_id": asset.id,
                    "title": asset.title,
                    "media_type": asset.media_type,
                    "relative_path": asset.relative_path,
                    "source_path": asset.storage_path,
                    "namespace": namespace,
                    "notes": notes_payload,
                    "metadata": manifest_metadata,
                }
                manifest_line["record_id"] = build_manifest_record_id(manifest_line)
                if only_missing_embeddings and manifest_line["record_id"] in existing_record_ids:
                    continue
                handle.write(json.dumps(manifest_line, ensure_ascii=False) + "\n")
                count += 1
                if limit is not None and count >= limit:
                    print(f"Wrote manifest entries: {count}")
                    return

    print(f"Wrote manifest entries: {count}")


def build_media_manifest_notes(asset: Asset, metadata: dict[str, Any]) -> str:
    lines = []
    structured_summary = extract_structured_summary_text(metadata)
    if structured_summary:
        lines.append(f"structured_summary:\n{structured_summary}")

    # Visual-audio enriched summary (captures telops, SE, editing patterns)
    vas_searchable = metadata.get(BRAIN_SEARCHABLE_SUMMARY_V2_KEY)
    if vas_searchable and isinstance(vas_searchable, str):
        lines.append(f"visual_audio_summary: {vas_searchable}")
    vas_data = metadata.get(BRAIN_VISUAL_AUDIO_SUMMARY_KEY)
    if isinstance(vas_data, dict):
        editing_patterns = vas_data.get("editing_patterns") or []
        if editing_patterns:
            lines.append(f"editing_patterns: {', '.join(editing_patterns)}")
        beats = vas_data.get("beats") or []
        # Include key visual events + telops for embedding richness
        key_visuals = []
        key_telops = []
        for b in beats[:10]:
            if b.get("visual_event"):
                key_visuals.append(b["visual_event"])
            key_telops.extend(b.get("telop_text") or [])
        if key_visuals:
            lines.append(f"key_visual_events: {' / '.join(key_visuals[:6])}")
        if key_telops:
            lines.append(f"telops: {', '.join(key_telops[:10])}")

    if asset.notes:
        lines.append(f"notes: {asset.notes}")
    if asset.transcript_excerpt:
        lines.append(f"transcript_excerpt: {asset.transcript_excerpt}")
    if metadata.get("tags"):
        lines.append(f"tags: {', '.join(metadata['tags'])}")
    if asset.published_at:
        lines.append(f"published_at: {asset.published_at}")
    if asset.source_url:
        lines.append(f"source_url: {asset.source_url}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Timeline segment helpers
# ---------------------------------------------------------------------------


def build_timeline_segment_title(asset_title: str, segment: TimelineSegment) -> str:
    label = segment.label.strip() if segment.label else ""
    suffix = label or segment.segment_kind
    return f"{asset_title} [{suffix} #{segment.segment_index + 1}]"


def build_timeline_segment_notes(segment: TimelineSegment) -> str:
    lines = []
    if segment.label:
        lines.append(f"label: {segment.label}")
    if segment.notes:
        lines.append(f"notes: {segment.notes}")
    return "\n".join(lines)


def build_timeline_segment_embedding_text(
    asset: Asset,
    metadata: dict[str, Any],
    segment: TimelineSegment,
    transcript_segments: list[TextSegment],
) -> str:
    transcript = segment.transcript.strip()
    if not transcript:
        transcript = load_transcript_window_text(transcript_segments, segment.start_seconds, segment.end_seconds)

    lines = [f"segment_kind: {segment.segment_kind}"]
    if segment.label:
        lines.append(f"label: {segment.label}")
    if transcript:
        lines.append(f"transcript: {transcript}")
    if segment.notes:
        lines.append(f"notes: {segment.notes}")

    # Visual-audio beat data overlapping this segment's time range
    vas_data = metadata.get(BRAIN_VISUAL_AUDIO_SUMMARY_KEY)
    if isinstance(vas_data, dict):
        beats = vas_data.get("beats") or []
        seg_start = float(segment.start_seconds) if segment.start_seconds is not None else 0.0
        seg_end = float(segment.end_seconds) if segment.end_seconds is not None else 0.0
        overlapping_beats = [
            b for b in beats
            if float(b.get("end_sec", 0)) > seg_start and float(b.get("start_sec", 0)) < seg_end
        ]
        if overlapping_beats:
            visual_events = [b.get("visual_event", "") for b in overlapping_beats if b.get("visual_event")]
            if visual_events:
                lines.append(f"visual_events: {' / '.join(visual_events[:3])}")
            all_telops = []
            for b in overlapping_beats:
                all_telops.extend(b.get("telop_text") or [])
            if all_telops:
                lines.append(f"telop: {', '.join(all_telops[:6])}")
            audio_events = []
            for b in overlapping_beats:
                audio_events.extend(b.get("audio_events") or [])
            if audio_events:
                lines.append(f"audio_events: {', '.join(audio_events[:4])}")
            pace_labels = [b.get("pace_label", "") for b in overlapping_beats if b.get("pace_label")]
            if pace_labels:
                lines.append(f"pace: {pace_labels[0]}")
            tension_labels = [b.get("tension_label", "") for b in overlapping_beats if b.get("tension_label")]
            if tension_labels:
                lines.append(f"tension: {tension_labels[0]}")

        editing_patterns = vas_data.get("editing_patterns") or []
        if editing_patterns:
            lines.append(f"editing_patterns: {', '.join(editing_patterns[:5])}")

    structured_summary = extract_structured_summary_text(metadata)
    if structured_summary:
        lines.append(f"asset_summary: {structured_summary.replace(chr(10), ' | ')}")

    # Visual-audio searchable summary (richer than text-only v1)
    vas_searchable = metadata.get(BRAIN_SEARCHABLE_SUMMARY_V2_KEY)
    if vas_searchable and isinstance(vas_searchable, str):
        lines.append(f"visual_audio_summary: {shorten_text(vas_searchable, 400)}")

    if asset.notes:
        lines.append(f"asset_notes: {shorten_text(asset.notes, 300)}")
    if not transcript and asset.transcript_excerpt:
        lines.append(f"asset_transcript_excerpt: {shorten_text(asset.transcript_excerpt, 600)}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Feedback metadata merging
# ---------------------------------------------------------------------------


def merge_feedback_metadata(
    metadata: dict[str, Any],
    feedback_summary: dict[str, Any],
    prefix: str,
    set_primary: bool,
) -> dict[str, Any]:
    if not feedback_summary:
        return metadata

    enriched = dict(metadata)
    enriched[f"{prefix}_feedback_summary"] = feedback_summary

    end_date = feedback_summary.get("end_date")
    if end_date:
        enriched[f"{prefix}_feedback_end_date"] = end_date
        if set_primary:
            enriched["feedback_end_date"] = end_date

    for field in FEEDBACK_SCORE_FIELDS:
        value = feedback_summary.get(field)
        if value is None:
            continue
        enriched[f"{prefix}_{field}"] = value
        if set_primary:
            enriched[field] = value

    return enriched


def enrich_curation_metadata(metadata: dict[str, Any], asset: Asset) -> dict[str, Any]:
    enriched = dict(metadata)
    enriched["selection_status"] = asset_selection_status(asset)
    enriched["curation_cohort"] = asset.curation.cohort if asset.curation else ""
    enriched["curation_subcohort"] = asset_subcohort(asset, metadata)
    enriched["curation_reason"] = asset.curation.reason if asset.curation else ""
    return enriched


# ---------------------------------------------------------------------------
# Sync metadata builder
# ---------------------------------------------------------------------------


def build_embedding_record_sync_metadata(
    session: Session,
    asset: Asset,
    record: EmbeddingRecord,
) -> dict[str, Any]:
    base_metadata = strip_dynamic_embedding_metadata(safe_json_load(record.metadata_json))
    asset_metadata = safe_json_load(asset.metadata_json)
    metadata = {**asset_metadata, **base_metadata}
    metadata.update(
        {
            "asset_id": asset.id,
            "title": asset.title,
            "media_type": record.media_type,
            "relative_path": asset.relative_path,
            "source_path": asset.storage_path,
            "transcript_storage_path": asset.transcript_storage_path,
            "channel": asset.channel,
            "published_at": asset.published_at,
            "youtube_video_id": asset.youtube_video_id,
            "source_url": asset.source_url,
            "chunk_index": record.chunk_index,
        }
    )
    metadata = enrich_curation_metadata(metadata, asset)

    embedding_kind = infer_embedding_kind_from_metadata(metadata, record.media_type, record.chunk_index)
    metadata["embedding_kind"] = embedding_kind
    asset_feedback_summary = load_latest_asset_feedback_summary(session, asset.id)

    if embedding_kind == "timeline_segment":
        timeline_segment_id = metadata.get("timeline_segment_id")
        segment = None
        if timeline_segment_id:
            segment = session.get(TimelineSegment, int(timeline_segment_id))
        if segment:
            metadata.update(
                {
                    "timeline_segment_id": segment.id,
                    "timeline_segment_index": segment.segment_index,
                    "timeline_segment_kind": segment.segment_kind,
                    "timeline_label": segment.label,
                    "chunk_start_seconds": segment.start_seconds,
                    "chunk_end_seconds": segment.end_seconds,
                }
            )
            if segment.notes:
                metadata["notes"] = build_timeline_segment_notes(segment)
        segment_feedback_summary = (
            load_latest_feedback_summary(
                session=session,
                asset_id=asset.id,
                scope_type="timeline_segment",
                scope_key=str(timeline_segment_id),
            )
            if timeline_segment_id
            else {}
        )
        metadata = merge_feedback_metadata(
            metadata,
            segment_feedback_summary,
            prefix="segment",
            set_primary=bool(segment_feedback_summary),
        )
        metadata = merge_feedback_metadata(
            metadata,
            asset_feedback_summary,
            prefix="asset",
            set_primary=not bool(segment_feedback_summary),
        )
    else:
        if embedding_kind == "asset":
            metadata["notes"] = build_media_manifest_notes(asset, asset_metadata)
            if asset.timeline_segments:
                metadata["timeline_segment_count"] = len(asset.timeline_segments)
        metadata = merge_feedback_metadata(
            metadata,
            asset_feedback_summary,
            prefix="asset",
            set_primary=True,
        )

    return metadata


# ---------------------------------------------------------------------------
# Import embedding results
# ---------------------------------------------------------------------------


def import_embedding_results(session_factory: sessionmaker[Session], results_path: Path) -> None:
    imported = 0
    with session_factory() as session, results_path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            payload = json.loads(raw_line)
            asset_id = payload["asset_id"]
            namespace = payload["namespace"]
            record_id = payload["record_id"]

            existing = session.scalar(
                select(EmbeddingRecord)
                .where(EmbeddingRecord.namespace == namespace)
                .where(EmbeddingRecord.record_id == record_id)
            )
            record = existing or EmbeddingRecord(
                asset_id=asset_id,
                namespace=namespace,
                record_id=record_id,
                media_type=payload.get("media_type", "unknown"),
                embedding_model=payload.get("embedding_model", ""),
            )
            record.asset_id = asset_id
            record.media_type = payload.get("media_type", record.media_type)
            record.embedding_model = payload.get("embedding_model", record.embedding_model)
            record.chunk_index = payload.get("chunk_index")
            metadata = payload.get("metadata", {}) or {}
            if not isinstance(metadata, dict):
                metadata = {}
            metadata.setdefault(
                "embedding_kind",
                infer_embedding_kind_from_metadata(metadata, record.media_type, record.chunk_index),
            )
            record.metadata_json = json.dumps(metadata, ensure_ascii=False)
            session.add(record)
            imported += 1

        session.commit()

    print(f"Imported embedding result rows: {imported}")


# ---------------------------------------------------------------------------
# Index metadata helpers
# ---------------------------------------------------------------------------


def normalize_index_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    clean: dict[str, Any] = {}
    for key, value in metadata.items():
        if value is None:
            continue
        if isinstance(value, Path):
            clean[key] = str(value)
        elif isinstance(value, (str, int, float, bool)):
            clean[key] = value
        elif isinstance(value, list) and all(isinstance(item, str) for item in value):
            clean[key] = value
        else:
            clean[key] = json.dumps(value, ensure_ascii=False)
    return clean


def strip_dynamic_embedding_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    dynamic_keys = {
        "selection_status",
        "curation_cohort",
        "curation_reason",
        "feedback_summary",
        "asset_feedback_summary",
        "segment_feedback_summary",
        "feedback_end_date",
        "asset_feedback_end_date",
        "segment_feedback_end_date",
    }
    dynamic_keys.update(FEEDBACK_SCORE_FIELDS)
    dynamic_keys.update(f"asset_{field}" for field in FEEDBACK_SCORE_FIELDS)
    dynamic_keys.update(f"segment_{field}" for field in FEEDBACK_SCORE_FIELDS)
    return {key: value for key, value in metadata.items() if key not in dynamic_keys}


# ---------------------------------------------------------------------------
# Pinecone loader
# ---------------------------------------------------------------------------


def load_pinecone_index_from_env() -> tuple[Any, str]:
    load_dotenv(override=False)
    api_key = os.getenv("PINECONE_API_KEY")
    index_name = os.getenv("PINECONE_INDEX_NAME", "bankara-brain-mvp")
    if not api_key:
        raise RuntimeError("Missing PINECONE_API_KEY")

    from pinecone import Pinecone

    pc = Pinecone(api_key=api_key)
    if not pc.has_index(index_name):
        raise RuntimeError(f"Pinecone index not found: {index_name}")

    description = pc.describe_index(index_name)
    host = attr_or_key(description, "host")
    if not host:
        raise RuntimeError(f"Could not resolve Pinecone host for index: {index_name}")
    return pc.Index(host=host), index_name


# ---------------------------------------------------------------------------
# Small utilities
# ---------------------------------------------------------------------------


def chunk_list(values: list[Any], size: int) -> list[list[Any]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def attr_or_key(value: Any, key: str, default: Any | None = None) -> Any:
    if isinstance(value, dict):
        return value.get(key, default)
    return getattr(value, key, default)


def infer_embedding_kind_from_metadata(metadata: dict[str, Any], media_type: str, chunk_index: int | None) -> str:
    embedding_kind = str(metadata.get("embedding_kind") or "").strip()
    if embedding_kind:
        return embedding_kind
    if metadata.get("timeline_segment_id") is not None:
        return "timeline_segment"
    if media_type == "text":
        return "text_chunk"
    if chunk_index is not None:
        return "timeline_segment"
    return "asset"
