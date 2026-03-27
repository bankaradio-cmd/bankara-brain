"""Enrichment pipelines: structured summaries and visual-audio summaries."""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

from dotenv import load_dotenv
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from bankara_brain.models import Asset, TextSegment, TimelineSegment, now_utc
from bankara_brain.corpus.query import (
    asset_cohort, asset_selection_status, asset_subcohort,
    resolve_asset, resolve_asset_media_path, select_assets_for_filters,
)
from bankara_brain.utils import safe_json_load
from bankara_brain.analysis.structured_summary import (
    BRAIN_SUMMARY_KEY, BRAIN_SUMMARY_MODEL_KEY, BRAIN_SUMMARY_TEXT_KEY,
    BRAIN_SUMMARY_UPDATED_AT_KEY, extract_structured_summary_payload,
    normalize_summary_list, normalize_summary_value_text,
    render_structured_summary_text,
)
from bankara_brain.ingest.transcript import load_transcript_segments, load_transcript_window_text

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_GENERATION_MODEL = "gemini-2.5-flash"
BRAIN_VISUAL_AUDIO_UPDATED_AT_KEY = "brain_visual_audio_updated_at"


# ---------------------------------------------------------------------------
# Structured Summary helpers
# ---------------------------------------------------------------------------


def build_asset_summary_source_text(
    asset: Asset,
    metadata: dict[str, Any],
    transcript_segments: list[TextSegment],
    timeline_segments: list[TimelineSegment],
) -> str:
    from bankara_brain.utils import format_seconds_hms
    from bankara_media_utils import shorten_text

    transcript_parts: list[str] = []
    for segment in transcript_segments[:10]:
        text = normalize_summary_value_text(segment.text, 260)
        if not text:
            continue
        if segment.start_seconds is not None and segment.end_seconds is not None:
            transcript_parts.append(
                f"{format_seconds_hms(float(segment.start_seconds))}-{format_seconds_hms(float(segment.end_seconds))}: {text}"
            )
        else:
            transcript_parts.append(text)

    timeline_parts: list[str] = []
    for segment in timeline_segments[:8]:
        pieces = [f"segment#{segment.segment_index + 1}", segment.segment_kind]
        if segment.label:
            pieces.append(f"label={normalize_summary_value_text(segment.label, 80)}")
        if segment.notes:
            pieces.append(f"notes={normalize_summary_value_text(segment.notes, 160)}")
        transcript = segment.transcript or load_transcript_window_text(
            transcript_segments,
            segment.start_seconds,
            segment.end_seconds,
        )
        if transcript:
            pieces.append(f"transcript={normalize_summary_value_text(transcript, 180)}")
        timeline_parts.append(" | ".join(piece for piece in pieces if piece))

    tags = metadata.get("tags") or []
    lines = [
        f"title: {asset.title}",
        f"media_type: {asset.media_type}",
        f"channel: {asset.channel or metadata.get('channel') or ''}",
        f"published_at: {asset.published_at or metadata.get('published_at') or ''}",
        f"selection_status: {asset_selection_status(asset)}",
        f"cohort: {asset_cohort(asset)}",
        f"subcohort: {asset_subcohort(asset, metadata)}",
    ]
    if tags:
        lines.append(f"tags: {', '.join(str(tag) for tag in tags if str(tag).strip())}")
    if asset.notes:
        lines.append(f"notes: {shorten_text(asset.notes, 1000)}")
    if asset.transcript_excerpt:
        lines.append(f"transcript_excerpt: {shorten_text(asset.transcript_excerpt, 1200)}")
    if transcript_parts:
        lines.extend(["transcript_samples:", *[f"- {part}" for part in transcript_parts]])
    if timeline_parts:
        lines.extend(["timeline_samples:", *[f"- {part}" for part in timeline_parts]])
    return "\n".join(line for line in lines if line.strip())


def normalize_structured_summary_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = {
        "premise": normalize_summary_value_text(payload.get("premise"), 220),
        "character_engine": normalize_summary_value_text(payload.get("character_engine"), 220),
        "authority_flip": normalize_summary_value_text(payload.get("authority_flip"), 220),
        "hook_pattern": normalize_summary_value_text(payload.get("hook_pattern"), 260),
        "escalation_pattern": normalize_summary_value_text(payload.get("escalation_pattern"), 320),
        "payoff_pattern": normalize_summary_value_text(payload.get("payoff_pattern"), 260),
        "setting": normalize_summary_value_text(payload.get("setting"), 140),
        "searchable_summary": normalize_summary_value_text(payload.get("searchable_summary"), 320),
        "tone_tags": normalize_summary_list(payload.get("tone_tags"), max_items=8, max_length=32),
        "novelty_guardrails": normalize_summary_list(payload.get("novelty_guardrails"), max_items=5, max_length=90),
    }
    return {key: value for key, value in normalized.items() if value not in ("", [], None)}


def render_structured_summary_prompt(asset: Asset, source_text: str) -> str:
    return (
        "あなたはバンカラブレインの検索精度改善エンジンです。\n"
        "1本の動画から、後続の企画検索と再利用に効く『構造化サマリー』を抽出してください。\n"
        "会話の表面ではなく、設定・役割・導入フック・エスカレーション・オチの型を抽象化すること。\n"
        "setting は具体店名や固有名だけでなく、広いカテゴリ語も含めること。例: 飲食店 / 小売店 / 学校イベント / 運動イベント / 教室イベント / 遠足イベント / 法執行 / 国家権力。\n"
        "authority_flip では『誰が誰を支配するか』『誰の権威を奪うか』を短く明示すること。\n"
        "tone_tags は短い再利用語に揃えること。例: 痛快 / 破天荒 / テンポが良い / 支配的 / 理不尽 / 戦略的。\n"
        "searchable_summary は将来の検索語に寄せた抽象語彙を優先し、1-2文にまとめること。\n"
        "出力は短く、検索に効く語に寄せること。日本語で返すこと。JSON オブジェクトだけを返すこと。\n"
        "形式: "
        "{premise: string, character_engine: string, authority_flip: string, hook_pattern: string, "
        "escalation_pattern: string, payoff_pattern: string, setting: string, tone_tags: string[], "
        "novelty_guardrails: string[], searchable_summary: string}\n\n"
        f"asset_title: {asset.title}\n"
        f"asset_media_type: {asset.media_type}\n"
        f"asset_cohort: {asset_cohort(asset) or 'unspecified'}\n\n"
        f"asset_subcohort: {asset_subcohort(asset) or 'unspecified'}\n\n"
        "source_context:\n"
        f"{source_text}\n"
    )


def run_gemini_structured_summary_generation(
    asset: Asset,
    source_text: str,
    model_name: str,
    temperature: float,
) -> tuple[dict[str, Any], str]:
    try:
        from google import genai
        from google.genai import types
    except ImportError as exc:
        raise RuntimeError("Structured summary generation requires google-genai to be installed.") from exc

    from bankara_script_assistant.gemini_helpers import parse_generated_json

    load_dotenv(override=False)
    api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY (or GOOGLE_API_KEY) for structured summary generation.")

    client = genai.Client(api_key=api_key)
    response = client.models.generate_content(
        model=model_name,
        contents=render_structured_summary_prompt(asset, source_text),
        config=types.GenerateContentConfig(
            temperature=temperature,
            response_mime_type="application/json",
        ),
    )
    text = response.text or ""
    if not text.strip():
        raise RuntimeError(f"Structured summary generation returned empty text for asset: {asset.relative_path}")

    parsed = parse_generated_json(text)
    normalized = normalize_structured_summary_payload(parsed)
    if not normalized:
        raise ValueError(f"Structured summary generation returned no usable fields for asset: {asset.relative_path}")
    return normalized, render_structured_summary_text(normalized)


def enrich_structured_summaries(
    session_factory: sessionmaker[Session],
    asset_selector: str | None,
    media_type: str | None,
    channel: str | None,
    selection_status: str | None,
    cohort: str | None,
    subcohort: str | None,
    require_tags: list[str] | None,
    exclude_tags: list[str] | None,
    title_contains: list[str] | None,
    source_url_contains: list[str] | None,
    limit: int | None,
    overwrite: bool,
    model_name: str,
    temperature: float,
    dry_run: bool,
    report_output: Path | None,
) -> None:
    from bankara_brain.ingest.pipeline import write_jsonl_report_row

    processed = 0
    generated = 0
    skipped_existing = 0

    report_handle = None
    if report_output:
        report_output.parent.mkdir(parents=True, exist_ok=True)
        report_handle = report_output.open("w", encoding="utf-8")

    try:
        with session_factory() as session:
            if asset_selector:
                assets = [resolve_asset(session, asset_selector)]
            else:
                assets = select_assets_for_filters(
                    session=session,
                    media_type=media_type,
                    channel=channel,
                    require_tags=require_tags,
                    exclude_tags=exclude_tags,
                    title_contains=title_contains,
                    source_url_contains=source_url_contains,
                    selection_status=selection_status,
                    cohort=cohort,
                    subcohort=subcohort,
                )

            if limit is not None:
                assets = assets[:limit]

            for asset in assets:
                metadata = safe_json_load(asset.metadata_json)
                existing_summary = extract_structured_summary_payload(metadata)
                if existing_summary and not overwrite:
                    skipped_existing += 1
                    row = {
                        "status": "skipped_existing",
                        "asset_id": asset.id,
                        "relative_path": asset.relative_path,
                        "title": asset.title,
                    }
                    if report_handle:
                        write_jsonl_report_row(report_handle, row)
                    continue

                transcript_segments = load_transcript_segments(session, asset.id)
                timeline_segments = session.scalars(
                    select(TimelineSegment)
                    .where(TimelineSegment.asset_id == asset.id)
                    .order_by(TimelineSegment.segment_index)
                ).all()
                source_text = build_asset_summary_source_text(
                    asset=asset,
                    metadata=metadata,
                    transcript_segments=transcript_segments,
                    timeline_segments=timeline_segments,
                )
                if not source_text.strip():
                    row = {
                        "status": "skipped_empty_source",
                        "asset_id": asset.id,
                        "relative_path": asset.relative_path,
                        "title": asset.title,
                    }
                    if report_handle:
                        write_jsonl_report_row(report_handle, row)
                    continue

                processed += 1
                if dry_run:
                    row = {
                        "status": "would_generate",
                        "asset_id": asset.id,
                        "relative_path": asset.relative_path,
                        "title": asset.title,
                        "source_chars": len(source_text),
                    }
                    if report_handle:
                        write_jsonl_report_row(report_handle, row)
                    continue

                structured_summary, structured_text = run_gemini_structured_summary_generation(
                    asset=asset,
                    source_text=source_text,
                    model_name=model_name,
                    temperature=temperature,
                )
                metadata[BRAIN_SUMMARY_KEY] = structured_summary
                metadata[BRAIN_SUMMARY_TEXT_KEY] = structured_text
                metadata[BRAIN_SUMMARY_MODEL_KEY] = model_name
                metadata[BRAIN_SUMMARY_UPDATED_AT_KEY] = now_utc().isoformat()
                asset.metadata_json = json.dumps(metadata, ensure_ascii=False)
                session.add(asset)
                session.commit()
                generated += 1

                row = {
                    "status": "generated",
                    "asset_id": asset.id,
                    "relative_path": asset.relative_path,
                    "title": asset.title,
                    "summary_text": structured_text,
                }
                if report_handle:
                    write_jsonl_report_row(report_handle, row)
    finally:
        if report_handle:
            report_handle.close()

    action = "would_generate" if dry_run else "generated"
    action_count = processed if dry_run else generated
    print(
        f"Structured summary enrichment: {action}={action_count} "
        f"skipped_existing={skipped_existing} candidates={processed + skipped_existing}"
    )


def enrich_visual_audio_summaries(
    session_factory: sessionmaker[Session],
    asset_selector: str | None,
    media_type: str | None,
    channel: str | None,
    selection_status: str | None,
    cohort: str | None,
    subcohort: str | None,
    require_tags: list[str] | None,
    exclude_tags: list[str] | None,
    title_contains: list[str] | None,
    source_url_contains: list[str] | None,
    limit: int | None,
    overwrite: bool,
    model_name: str,
    temperature: float,
    dry_run: bool,
    scene_threshold: float,
    report_output: Path | None,
) -> None:
    """Generate visual-audio beat summaries for video assets.

    Uses ffmpeg for shot detection + frame extraction + audio analysis,
    then calls Gemini with multimodal input to produce structured beat data.
    """
    from bankara_visual_audio_summary import generate_visual_audio_summary, render_searchable_text
    from bankara_media_utils import find_sidecar_text_file
    from bankara_brain.embedding.manifest import BRAIN_VISUAL_AUDIO_SUMMARY_KEY, BRAIN_SEARCHABLE_SUMMARY_V2_KEY
    from bankara_brain.ingest.pipeline import write_jsonl_report_row

    processed = 0
    generated = 0
    skipped_existing = 0
    skipped_non_video = 0

    report_handle = None
    if report_output:
        report_output.parent.mkdir(parents=True, exist_ok=True)
        report_handle = report_output.open("w", encoding="utf-8")

    try:
        with session_factory() as session:
            if asset_selector:
                assets = [resolve_asset(session, asset_selector)]
            else:
                assets = select_assets_for_filters(
                    session=session,
                    media_type=media_type or "video",
                    channel=channel,
                    require_tags=require_tags,
                    exclude_tags=exclude_tags,
                    title_contains=title_contains,
                    source_url_contains=source_url_contains,
                    selection_status=selection_status,
                    cohort=cohort,
                    subcohort=subcohort,
                )
            if limit is not None:
                assets = assets[:limit]

            for asset in assets:
                # Only process video assets
                if asset.media_type != "video":
                    skipped_non_video += 1
                    continue

                metadata = safe_json_load(asset.metadata_json)

                if not overwrite and metadata.get(BRAIN_VISUAL_AUDIO_SUMMARY_KEY):
                    skipped_existing += 1
                    continue

                processed += 1

                # Resolve video file path
                media_path = resolve_asset_media_path(asset)
                if media_path is None or not media_path.exists():
                    print(f"Skipping (no media file): {asset.relative_path}")
                    continue

                if dry_run:
                    print(f"Would generate visual-audio summary: {asset.relative_path}")
                    continue

                # Load transcript as subtitle cues
                transcript_text = None
                sidecar = find_sidecar_text_file(media_path)
                if sidecar:
                    transcript_text = sidecar.read_text(encoding="utf-8", errors="replace")

                # Also check TextSegment records
                if not transcript_text:
                    text_segments = session.scalars(
                        select(TextSegment)
                        .where(TextSegment.asset_id == asset.id)
                        .order_by(TextSegment.chunk_index)
                    ).all()
                    if text_segments:
                        transcript_text = "\n".join(seg.text for seg in text_segments)

                duration = float(asset.duration_seconds) if asset.duration_seconds else None

                print(f"Generating visual-audio summary: {asset.relative_path}")
                try:
                    summary = generate_visual_audio_summary(
                        video_path=media_path,
                        asset_id=asset.id,
                        title=asset.title or "",
                        cohort=asset_cohort(asset),
                        subcohort=asset_subcohort(asset, metadata),
                        transcript_text=transcript_text,
                        duration_seconds=duration,
                        model_name=model_name,
                        temperature=temperature,
                        scene_threshold=scene_threshold,
                    )
                except Exception as exc:
                    logger.error("Error generating visual-audio summary for %s: %s", asset.relative_path, exc)
                    continue

                # Store in metadata
                metadata[BRAIN_VISUAL_AUDIO_SUMMARY_KEY] = summary.to_dict()
                searchable_text = render_searchable_text(summary)
                if searchable_text:
                    metadata[BRAIN_SEARCHABLE_SUMMARY_V2_KEY] = searchable_text
                metadata[BRAIN_VISUAL_AUDIO_UPDATED_AT_KEY] = now_utc().isoformat()
                asset.metadata_json = json.dumps(metadata, ensure_ascii=False)
                session.add(asset)
                session.commit()
                generated += 1

                print(
                    f"  beats={len(summary.beats)} shots={summary.shot_count} "
                    f"frames={summary.frame_count} patterns={summary.editing_patterns}"
                )

                # Pause between API calls to avoid rate limits
                time.sleep(2.0)

                row = {
                    "status": "generated",
                    "asset_id": asset.id,
                    "relative_path": asset.relative_path,
                    "title": asset.title,
                    "beat_count": len(summary.beats),
                    "shot_count": summary.shot_count,
                    "frame_count": summary.frame_count,
                    "editing_patterns": summary.editing_patterns,
                    "searchable_text": searchable_text[:200] if searchable_text else "",
                    "notes": summary.notes,
                }
                if report_handle:
                    write_jsonl_report_row(report_handle, row)
    finally:
        if report_handle:
            report_handle.close()

    action = "would_generate" if dry_run else "generated"
    action_count = processed if dry_run else generated
    print(
        f"Visual-audio summary enrichment: {action}={action_count} "
        f"skipped_existing={skipped_existing} skipped_non_video={skipped_non_video} "
        f"candidates={processed + skipped_existing + skipped_non_video}"
    )
