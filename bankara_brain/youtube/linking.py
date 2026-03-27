"""YouTube asset linking: list catalog videos and match them to corpus assets."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session, sessionmaker

from bankara_brain.db import AppConfig
from bankara_brain.models import Asset
from bankara_brain.corpus.query import (
    asset_selection_status,
    normalize_match_text,
    resolve_asset,
    select_assets_for_filters,
)
from bankara_brain.utils import safe_json_load
from bankara_brain.youtube.data_api import build_youtube_data_service, fetch_youtube_video_catalog
from bankara_brain.youtube.helpers import extract_youtube_video_id, is_valid_youtube_video_id


def list_youtube_videos(
    config: AppConfig,
    limit: int,
    title_contains: list[str] | None,
    json_output: Path | None = None,
) -> None:
    service = build_youtube_data_service(config=config, force_reauth=False)
    catalog = fetch_youtube_video_catalog(service=service, limit=limit, title_contains=title_contains)

    if json_output:
        json_output.parent.mkdir(parents=True, exist_ok=True)
        json_output.write_text(json.dumps(catalog, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Wrote YouTube video catalog: {json_output}")

    if not catalog:
        print("No YouTube videos matched the requested filters.")
        return

    for row in catalog:
        print(
            f"{row['video_id']} "
            f"title={row.get('title')!r} "
            f"published={row.get('published_at') or '-'} "
            f"privacy={row.get('video_privacy_status') or row.get('privacy_status') or '-'} "
            f"views={row.get('view_count') or '-'}"
        )


def link_youtube_assets(
    config: AppConfig,
    session_factory: sessionmaker[Session],
    asset_selector: str | None,
    manual_video_id: str | None,
    media_type: str | None,
    channel: str | None,
    selection_status: str | None,
    cohort: str | None,
    subcohort: str | None,
    require_tags: list[str] | None,
    exclude_tags: list[str] | None,
    title_contains: list[str] | None,
    source_url_contains: list[str] | None,
    asset_limit: int | None,
    catalog_limit: int | None,
    dry_run: bool,
    report_output: Path | None,
) -> None:
    service = build_youtube_data_service(config=config, force_reauth=False)
    catalog = fetch_youtube_video_catalog(service=service, limit=catalog_limit, title_contains=None)
    catalog_by_id = {row["video_id"]: row for row in catalog}
    title_index: dict[str, list[dict[str, Any]]] = {}
    for row in catalog:
        normalized_title = row.get("normalized_title") or ""
        if normalized_title:
            title_index.setdefault(normalized_title, []).append(row)

    rows: list[dict[str, Any]] = []
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

        if asset_limit is not None:
            assets = assets[:asset_limit]

        if manual_video_id and not asset_selector:
            raise ValueError("--video-id requires --asset so the target asset is unambiguous.")

        if manual_video_id and not is_valid_youtube_video_id(manual_video_id):
            raise ValueError(f"Invalid YouTube video id: {manual_video_id}")

        changed = 0
        for asset in assets:
            row: dict[str, Any] = {
                "asset_id": asset.id,
                "relative_path": asset.relative_path,
                "asset_title": asset.title,
                "selection_status": asset_selection_status(asset),
                "previous_video_id": asset.youtube_video_id,
                "status": "pending",
                "match_reason": "",
                "video_id": None,
                "video_title": None,
                "video_url": None,
            }

            chosen: dict[str, Any] | None = None
            if manual_video_id:
                chosen = catalog_by_id.get(manual_video_id)
                if not chosen:
                    row["status"] = "manual_video_not_found"
                    rows.append(row)
                    print(f"skip    {asset.relative_path} manual_video_id={manual_video_id} not found in authorized channel")
                    continue
                row["match_reason"] = "manual_video_id"
            else:
                source_url_video_id = extract_youtube_video_id(asset.source_url)
                if source_url_video_id and source_url_video_id in catalog_by_id:
                    chosen = catalog_by_id[source_url_video_id]
                    row["match_reason"] = "source_url_exact"
                elif is_valid_youtube_video_id(asset.youtube_video_id) and asset.youtube_video_id in catalog_by_id:
                    chosen = catalog_by_id[asset.youtube_video_id]
                    row["match_reason"] = "current_video_id"
                else:
                    title_matches = title_index.get(normalize_match_text(asset.title), [])
                    stem_matches = title_index.get(normalize_match_text(Path(asset.relative_path).stem), [])
                    if len(title_matches) == 1:
                        chosen = title_matches[0]
                        row["match_reason"] = "title_exact"
                    elif len(stem_matches) == 1:
                        chosen = stem_matches[0]
                        row["match_reason"] = "stem_exact"
                    elif len(title_matches) > 1:
                        row["status"] = "ambiguous_title_match"
                        row["candidate_video_ids"] = [match["video_id"] for match in title_matches[:5]]
                    elif len(stem_matches) > 1:
                        row["status"] = "ambiguous_stem_match"
                        row["candidate_video_ids"] = [match["video_id"] for match in stem_matches[:5]]
                    else:
                        row["status"] = "unmatched"

            if not chosen:
                if row["status"] == "pending":
                    row["status"] = "unmatched"
                rows.append(row)
                print(
                    f"skip    {asset.relative_path} status={row['status']} "
                    f"title={asset.title!r} current_video_id={asset.youtube_video_id or '-'}"
                )
                continue

            row["video_id"] = chosen["video_id"]
            row["video_title"] = chosen.get("title")
            row["video_url"] = chosen.get("url")

            already_linked = asset.youtube_video_id == chosen["video_id"] and asset.source_url == chosen.get("url")
            if already_linked:
                row["status"] = "already_linked"
                rows.append(row)
                print(
                    f"keep    {asset.relative_path} video_id={chosen['video_id']} "
                    f"reason={row['match_reason']}"
                )
                continue

            if dry_run:
                row["status"] = "would_link"
                rows.append(row)
                print(
                    f"link    {asset.relative_path} -> {chosen['video_id']} "
                    f"reason={row['match_reason']} title={chosen.get('title')!r}"
                )
                continue

            asset.youtube_video_id = chosen["video_id"]
            asset.source_url = chosen.get("url")
            if not asset.channel:
                asset.channel = chosen.get("channel_title")
            if not asset.published_at:
                asset.published_at = chosen.get("published_at")
            metadata = safe_json_load(asset.metadata_json)
            metadata["video_id"] = chosen["video_id"]
            metadata["source_url"] = chosen.get("url")
            if chosen.get("published_at") and not metadata.get("published_at"):
                metadata["published_at"] = chosen.get("published_at")
            if chosen.get("channel_title") and not metadata.get("channel"):
                metadata["channel"] = chosen.get("channel_title")
            asset.metadata_json = json.dumps(metadata, ensure_ascii=False)
            session.add(asset)
            changed += 1
            row["status"] = "linked"
            rows.append(row)
            print(
                f"linked  {asset.relative_path} -> {chosen['video_id']} "
                f"reason={row['match_reason']} title={chosen.get('title')!r}"
            )

        if not dry_run and changed:
            session.commit()

    if report_output:
        from bankara_brain.ingest.pipeline import write_jsonl_report_row

        report_output.parent.mkdir(parents=True, exist_ok=True)
        with report_output.open("w", encoding="utf-8") as handle:
            for row in rows:
                write_jsonl_report_row(handle, row)
        print(f"Wrote YouTube link report: {report_output}")

    status_counts = Counter(row["status"] for row in rows)
    summary = " ".join(f"{key}={value}" for key, value in sorted(status_counts.items()))
    print(f"YouTube link summary: assets={len(rows)} {summary}".rstrip())
