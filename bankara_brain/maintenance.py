from __future__ import annotations

import json
import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

from dotenv import load_dotenv
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from bankara_brain.db import AppConfig, BlobStore
from bankara_brain.models import Asset, TimelineSegment
from bankara_brain.corpus.query import (
    asset_selection_status,
    resolve_asset,
    resolve_asset_media_path,
    select_assets_for_filters,
)
from bankara_brain.ingest.transcript import (
    build_synthetic_transcript_file,
    default_transcribe_script_path,
    load_transcript_segments,
    resolve_asset_transcript_path,
    sync_asset_transcript,
    transcribe_asset_with_faster_whisper,
)
from bankara_brain.corpus.timeline import (
    build_bootstrap_timeline_segments,
    replace_timeline_segments,
)
from bankara_brain.ingest.pipeline import (
    default_embedding_python_path,
    write_jsonl_report_row,
)


def repair_assets(
    session_factory: sessionmaker[Session],
    blob_store: BlobStore,
    asset_selector: str | None,
    media_type: str | None,
    channel: str | None,
    selection_status: str | None,
    require_tags: list[str] | None,
    exclude_tags: list[str] | None,
    title_contains: list[str] | None,
    source_url_contains: list[str] | None,
    limit: int | None,
    skip_duration_repair: bool,
    skip_transcribe: bool,
    force_transcribe: bool,
    transcribe_script: Path | None,
    transcribe_language: str | None,
    transcribe_model: str | None,
    work_dir: Path | None,
    skip_bootstrap_timeline: bool,
    replace_timeline: bool,
    max_segment_seconds: float,
    min_segment_seconds: float,
    gap_seconds: float,
    target_chars: int,
    dry_run: bool,
    report_output: Path | None,
) -> None:
    from bankara_brain.utils import probe_media_duration

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
            )

    if limit is not None:
        assets = assets[:limit]

    if not assets:
        print("No assets matched repair filters.")
        return

    repair_work_dir = work_dir.expanduser().resolve() if work_dir else Path(tempfile.mkdtemp(prefix="bankara_repair_"))
    transcript_script_path = (
        transcribe_script.expanduser().resolve() if transcribe_script else default_transcribe_script_path()
    )

    repaired_assets = 0
    reviewed_assets = 0
    report_handle = None
    if report_output:
        report_output.parent.mkdir(parents=True, exist_ok=True)
        report_handle = report_output.open("w", encoding="utf-8")

    try:
        with session_factory() as session:
            for asset in assets:
                reviewed_assets += 1
                row: dict[str, Any] = {
                    "asset_id": asset.id,
                    "relative_path": asset.relative_path,
                    "media_type": asset.media_type,
                    "selection_status": None,
                    "actions": [],
                    "warnings": [],
                    "status": "ok",
                }
                try:
                    live_asset = resolve_asset(session, asset.id)
                    row["selection_status"] = asset_selection_status(live_asset)

                    if live_asset.media_type not in {"audio", "video"}:
                        row["warnings"].append("repair_not_applicable_for_text_asset")
                        print(f"Skipping {live_asset.relative_path}: text asset")
                        if report_handle:
                            write_jsonl_report_row(report_handle, row)
                        continue

                    media_path = resolve_asset_media_path(live_asset)
                    if media_path is None:
                        raise FileNotFoundError(f"Media file not found for asset: {live_asset.relative_path}")

                    if not skip_duration_repair and (not live_asset.duration_seconds or live_asset.duration_seconds <= 0):
                        probed_duration = probe_media_duration(media_path)
                        if probed_duration and probed_duration > 0:
                            if dry_run:
                                row["actions"].append(f"would_update_duration:{round(probed_duration, 3)}")
                            else:
                                live_asset.duration_seconds = probed_duration
                                session.add(live_asset)
                                row["actions"].append(f"updated_duration:{round(probed_duration, 3)}")
                        else:
                            row["warnings"].append("duration_probe_failed")

                    transcript_segments = load_transcript_segments(session, live_asset.id)
                    transcript_path = resolve_asset_transcript_path(live_asset)
                    transcript_needed = force_transcribe or transcript_path is None or not transcript_segments

                    if not skip_transcribe and transcript_needed:
                        if dry_run:
                            action = "would_transcribe"
                            if not force_transcribe and transcript_path:
                                action = f"would_stage_transcript:{transcript_path.name}"
                            row["actions"].append(action)
                        else:
                            if not force_transcribe and transcript_path:
                                synced_path = sync_asset_transcript(session, blob_store, live_asset, transcript_path)
                                row["actions"].append(f"staged_transcript:{synced_path.name}")
                            else:
                                try:
                                    generated_path = transcribe_asset_with_faster_whisper(
                                        asset=live_asset,
                                        transcribe_script=transcript_script_path,
                                        output_root=repair_work_dir,
                                        language=transcribe_language,
                                        model_name=transcribe_model,
                                    )
                                except ValueError as exc:
                                    if "no audio stream" not in str(exc).casefold():
                                        raise
                                    row["warnings"].append("video_has_no_audio_stream")
                                    synthetic_transcript_path = build_synthetic_transcript_file(
                                        asset=live_asset,
                                        output_root=repair_work_dir,
                                        timeline_segments=session.scalars(
                                            select(TimelineSegment)
                                            .where(TimelineSegment.asset_id == live_asset.id)
                                            .order_by(TimelineSegment.segment_index)
                                        ).all(),
                                    )
                                    synced_path = sync_asset_transcript(
                                        session=session,
                                        blob_store=blob_store,
                                        asset=live_asset,
                                        transcript_path=synthetic_transcript_path,
                                    )
                                    row["actions"].append(f"synthetic_transcript:{synced_path.name}")
                                else:
                                    synced_path = sync_asset_transcript(session, blob_store, live_asset, generated_path)
                                    row["actions"].append(f"transcribed:{synced_path.name}")

                    transcript_segments = load_transcript_segments(session, live_asset.id)
                    existing_timeline_segments = session.scalars(
                        select(TimelineSegment)
                        .where(TimelineSegment.asset_id == live_asset.id)
                        .order_by(TimelineSegment.segment_index)
                    ).all()
                    should_rebuild_timeline = not skip_bootstrap_timeline and (replace_timeline or not existing_timeline_segments)
                    if should_rebuild_timeline:
                        segments = build_bootstrap_timeline_segments(
                            asset=live_asset,
                            transcript_segments=transcript_segments,
                            max_segment_seconds=max_segment_seconds,
                            min_segment_seconds=min_segment_seconds,
                            gap_seconds=gap_seconds,
                            target_chars=target_chars,
                        )
                        if segments:
                            if dry_run:
                                row["actions"].append(f"would_bootstrap_timeline:{len(segments)}")
                            else:
                                replace_timeline_segments(session, live_asset, segments, replace=True)
                                row["actions"].append(f"bootstrapped_timeline:{len(segments)}")
                        else:
                            row["warnings"].append("no_transcript_available_for_timeline")

                    if dry_run:
                        print(
                            f"DRY-RUN {live_asset.relative_path} "
                            f"actions={','.join(row['actions']) or '-'} "
                            f"warnings={','.join(row['warnings']) or '-'}"
                        )
                    else:
                        session.commit()
                        repaired_assets += 1
                        print(
                            f"Repaired {live_asset.relative_path} "
                            f"actions={','.join(row['actions']) or '-'} "
                            f"warnings={','.join(row['warnings']) or '-'}"
                        )

                except Exception as exc:
                    session.rollback()
                    row["status"] = "error"
                    row["error"] = str(exc)
                    logger.error("Repair failed for %s: %s", asset.relative_path, exc)

                if report_handle:
                    write_jsonl_report_row(report_handle, row)
    finally:
        if report_handle:
            report_handle.close()

    mode = "dry-run reviewed" if dry_run else "repaired"
    total = reviewed_assets if dry_run else repaired_assets
    print(f"Repair completed. {mode}={total} work_dir={repair_work_dir}")


def doctor(config: AppConfig, json_output: Path | None = None) -> None:
    from bankara_brain.youtube.data_api import check_expected_youtube_channel
    from bankara_brain.utils import probe_media_duration  # noqa: F401 — kept for completeness

    load_dotenv(override=False)
    checks: list[dict[str, Any]] = []

    def add_check(name: str, ok: bool, detail: str) -> None:
        checks.append({"name": name, "ok": ok, "detail": detail})

    gemini_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    pinecone_key = os.getenv("PINECONE_API_KEY")
    add_check("gemini_api_key", bool(gemini_key), "configured" if gemini_key else "missing GEMINI_API_KEY / GOOGLE_API_KEY")
    add_check("pinecone_api_key", bool(pinecone_key), "configured" if pinecone_key else "missing PINECONE_API_KEY")

    embedding_python = default_embedding_python_path()
    add_check("embedding_python", embedding_python.exists(), str(embedding_python))

    ffmpeg_path = shutil.which("ffmpeg")
    ffprobe_path = shutil.which("ffprobe")
    add_check("ffmpeg", bool(ffmpeg_path), ffmpeg_path or "not found in PATH")
    add_check("ffprobe", bool(ffprobe_path), ffprobe_path or "not found in PATH")

    transcribe_script = default_transcribe_script_path()
    transcribe_venv_python = transcribe_script.parent.parent / ".venv" / "bin" / "python"
    add_check("faster_whisper_script", transcribe_script.exists(), str(transcribe_script))
    add_check("faster_whisper_venv", transcribe_venv_python.exists(), str(transcribe_venv_python))

    add_check(
        "youtube_client_secrets",
        config.youtube_client_secrets_file.exists(),
        str(config.youtube_client_secrets_file),
    )
    add_check(
        "youtube_oauth_token",
        config.youtube_token_file.exists(),
        str(config.youtube_token_file),
    )
    if config.youtube_token_file.exists():
        try:
            channel_check = check_expected_youtube_channel(config)
            add_check("youtube_oauth_channel", bool(channel_check["ok"]), channel_check["detail"])
        except Exception as exc:
            add_check("youtube_oauth_channel", False, f"failed to inspect authorized channel: {exc}")
    else:
        add_check(
            "youtube_oauth_channel",
            False,
            "OAuth token missing; cannot verify authorized channel against BANKARA_EXPECTED_YOUTUBE_CHANNEL_ID",
        )

    database_detail = config.database_url
    if config.database_url.startswith("sqlite:///"):
        sqlite_path = Path(config.database_url.replace("sqlite:///", "", 1)).expanduser()
        database_detail = f"{config.database_url} ({'exists' if sqlite_path.exists() else 'will be created'})"
    add_check("database", True, database_detail)
    add_check(
        "object_store_root",
        config.object_store_root.exists(),
        str(config.object_store_root),
    )

    if json_output:
        json_output.parent.mkdir(parents=True, exist_ok=True)
        json_output.write_text(json.dumps(checks, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Wrote doctor report: {json_output}")

    passed = sum(1 for row in checks if row["ok"])
    for row in checks:
        status = "OK" if row["ok"] else "NG"
        print(f"{status:<2} {row['name']}: {row['detail']}")
    print(f"Doctor summary: ok={passed}/{len(checks)}")
