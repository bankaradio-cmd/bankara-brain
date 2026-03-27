"""Asset curation, auditing, cohort assignment, and quarantine helpers."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from bankara_brain.models import Asset, AssetCuration
from bankara_brain.corpus.query import (
    asset_cohort,
    asset_matches_filters,
    asset_selection_status,
    asset_subcohort,
    normalize_filter_values,
    normalize_selection_status,
    resolve_asset,
    resolve_asset_media_path,
    select_assets_for_filters,
    media_has_audio_stream,
)
from bankara_brain.utils import safe_json_load
from bankara_brain.youtube.helpers import is_valid_youtube_video_id
from bankara_brain.embedding.manifest import infer_embedding_kind_from_metadata  # noqa: F401 — re-exported for backward compat


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_BANKARA_CHANNEL = "バンカラジオ"
DEFAULT_COMEDY_INCLUDE_KEYWORDS = (
    "コメディ",
    "コント",
    "寸劇",
    "茶番",
    "ドッキリ",
    " prank",
    "prank",
    "sketch",
    "comedy",
    "ギャグ",
    "ボケ",
    "ツッコミ",
)
DEFAULT_COMEDY_EXCLUDE_KEYWORDS = (
    "vlog",
    "メイキング",
    "making",
    "behind the scenes",
    "インタビュー",
    "interview",
    "切り抜き",
    "shorts",
)
WARNING_PROBLEMS = {
    "video_has_no_audio_stream",
    "missing_feedback_scores",
    "missing_youtube_video_id",
    "invalid_youtube_video_id",
    "uncurated",
}
BLOCKER_PROBLEMS = {
    "missing_media_file",
    "missing_duration",
    "missing_transcript",
    "missing_timeline",
    "missing_text_embedding",
    "missing_asset_embedding",
    "missing_timeline_embedding",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def asset_text_haystacks(asset: Asset, metadata: dict[str, Any]) -> list[str]:
    values = [
        asset.title,
        asset.notes,
        asset.transcript_excerpt,
        asset.channel or "",
        asset.source_url or "",
        " ".join(str(tag) for tag in (metadata.get("tags") or [])),
    ]
    return [value.casefold() for value in values if value]


def classify_bankara_comedy_asset(
    asset: Asset,
    metadata: dict[str, Any],
    target_channel: str,
    include_keywords: list[str],
    exclude_keywords: list[str],
    include_threshold: float,
    exclude_threshold: float,
) -> dict[str, Any]:
    reasons: list[str] = []
    score = 0.0
    asset_channel = (asset.channel or metadata.get("channel") or "").strip()
    normalized_target_channel = target_channel.strip().casefold()
    normalized_asset_channel = asset_channel.casefold()

    if normalized_target_channel and normalized_asset_channel == normalized_target_channel:
        score += 3.0
        reasons.append(f"channel matched {target_channel}")
    elif normalized_asset_channel:
        score -= 3.0
        reasons.append(f"channel mismatched ({asset_channel})")

    if asset.media_type in {"video", "audio"}:
        score += 0.75
        reasons.append(f"media_type={asset.media_type}")

    haystacks = asset_text_haystacks(asset, metadata)
    include_hits = [
        keyword
        for keyword in normalize_filter_values(include_keywords)
        if any(keyword in haystack for haystack in haystacks)
    ]
    exclude_hits = [
        keyword
        for keyword in normalize_filter_values(exclude_keywords)
        if any(keyword in haystack for haystack in haystacks)
    ]

    if include_hits:
        score += min(2.5, 0.8 * len(set(include_hits)))
        reasons.append(f"include keywords: {', '.join(sorted(set(include_hits)))}")
    if exclude_hits:
        score -= min(2.5, 0.8 * len(set(exclude_hits)))
        reasons.append(f"exclude keywords: {', '.join(sorted(set(exclude_hits)))}")

    if asset.youtube_video_id:
        score += 0.25
        reasons.append("has youtube_video_id")
    if asset.source_url and "youtube.com" in asset.source_url:
        score += 0.25
        reasons.append("source_url is youtube")
    if asset.media_type in {"audio", "video"} and not asset.duration_seconds:
        score -= 1.0
        reasons.append("missing media duration")

    if score >= include_threshold:
        recommendation = "included"
    elif score <= exclude_threshold:
        recommendation = "excluded"
    else:
        recommendation = "unset"

    return {
        "recommendation": recommendation,
        "score": round(score, 3),
        "confidence": round(min(1.0, abs(score) / max(abs(include_threshold), abs(exclude_threshold), 1.0)), 3),
        "reasons": reasons,
        "title": asset.title,
        "relative_path": asset.relative_path,
        "media_type": asset.media_type,
        "channel": asset_channel,
    }


def problem_severity(problem: str) -> str:
    return "warning" if problem in WARNING_PROBLEMS else "blocker"


def split_asset_problems(problems: list[str]) -> tuple[list[str], list[str]]:
    blockers: list[str] = []
    warnings: list[str] = []
    for problem in problems:
        if problem_severity(problem) == "warning":
            warnings.append(problem)
        else:
            blockers.append(problem)
    return blockers, warnings


def detect_asset_problems(asset: Asset) -> list[str]:
    metadata = safe_json_load(asset.metadata_json)
    current_selection_status = asset_selection_status(asset)
    embedding_kinds = [
        infer_embedding_kind_from_metadata(
            safe_json_load(record.metadata_json),
            record.media_type,
            record.chunk_index,
        )
        for record in asset.embedding_records
    ]
    problems: list[str] = []
    media_path = resolve_asset_media_path(asset)
    if asset.media_type in {"audio", "video"} and media_path is None:
        problems.append("missing_media_file")
    if asset.media_type in {"audio", "video"} and not asset.duration_seconds:
        problems.append("missing_duration")
    if asset.media_type == "video" and media_path is not None:
        has_audio_stream = media_has_audio_stream(media_path)
        if has_audio_stream is False:
            problems.append("video_has_no_audio_stream")
    if asset.media_type in {"audio", "video"} and not asset.transcript_storage_path:
        problems.append("missing_transcript")
    if asset.media_type in {"audio", "video"} and not asset.timeline_segments:
        problems.append("missing_timeline")
    if current_selection_status != "excluded" and asset.media_type == "text" and "text_chunk" not in embedding_kinds:
        problems.append("missing_text_embedding")
    if current_selection_status != "excluded" and asset.media_type in {"audio", "video"} and "asset" not in embedding_kinds:
        problems.append("missing_asset_embedding")
    if current_selection_status != "excluded" and asset.media_type in {"audio", "video"} and asset.timeline_segments:
        segment_ids = {segment.id for segment in asset.timeline_segments}
        embedded_segment_ids = {
            safe_json_load(record.metadata_json).get("timeline_segment_id")
            for record in asset.embedding_records
            if infer_embedding_kind_from_metadata(
                safe_json_load(record.metadata_json),
                record.media_type,
                record.chunk_index,
            )
            == "timeline_segment"
        }
        if not segment_ids.issubset(embedded_segment_ids):
            problems.append("missing_timeline_embedding")
    if current_selection_status != "excluded" and asset.youtube_video_id and not asset.feedback_scores:
        problems.append("missing_feedback_scores")
    if current_selection_status != "excluded" and asset.youtube_video_id and not is_valid_youtube_video_id(asset.youtube_video_id):
        problems.append("invalid_youtube_video_id")
    if current_selection_status != "excluded" and not asset.youtube_video_id:
        problems.append("missing_youtube_video_id")
    if current_selection_status == "unset":
        problems.append("uncurated")
    return problems


def build_audit_summary(rows: list[dict[str, Any]], emitted_rows: int) -> dict[str, Any]:
    blocker_counts = Counter(problem for row in rows for problem in row["blockers"])
    warning_counts = Counter(problem for row in rows for problem in row["warnings"])
    return {
        "assets_scanned": len(rows),
        "rows_emitted": emitted_rows,
        "assets_with_blockers": sum(1 for row in rows if row["blockers"]),
        "assets_with_warnings": sum(1 for row in rows if row["warnings"]),
        "blocker_counts": dict(sorted(blocker_counts.items())),
        "warning_counts": dict(sorted(warning_counts.items())),
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def list_assets(
    session_factory: sessionmaker[Session],
    media_type: str | None,
    limit: int,
    channel: str | None,
    require_tags: list[str] | None,
    exclude_tags: list[str] | None,
    title_contains: list[str] | None,
    source_url_contains: list[str] | None,
    selection_status: str | None,
    cohort: str | None,
    subcohort: str | None,
) -> None:
    with session_factory() as session:
        filtered_assets = select_assets_for_filters(
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
        )[:limit]

        if not filtered_assets:
            print("No assets found.")
            return

        for asset in filtered_assets:
            print(
                f"{asset.media_type:<5} "
                f"{asset.relative_path} "
                f"title={asset.title!r} "
                f"selection={asset_selection_status(asset)} "
                f"subcohort={asset_subcohort(asset) or '-'} "
                f"video_id={asset.youtube_video_id or '-'} "
                f"segments={len(asset.text_segments)} "
                f"timeline={len(asset.timeline_segments)} "
                f"embeddings={len(asset.embedding_records)} "
                f"feedback={len(asset.feedback_scores)}"
            )


def curate_assets(
    session_factory: sessionmaker[Session],
    selection_status: str,
    media_type: str | None,
    channel: str | None,
    require_tags: list[str] | None,
    exclude_tags: list[str] | None,
    title_contains: list[str] | None,
    source_url_contains: list[str] | None,
    cohort: str,
    reason: str,
    limit: int | None,
    dry_run: bool,
) -> None:
    normalized_selection_status = normalize_selection_status(selection_status)
    if normalized_selection_status is None:
        raise ValueError("--selection-status is required")

    updated = 0
    with session_factory() as session:
        stmt = select(Asset).order_by(Asset.relative_path)
        if media_type:
            stmt = stmt.where(Asset.media_type == media_type)
        assets = session.scalars(stmt).all()

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
            ):
                continue

            print(
                f"{normalized_selection_status:<8} {asset.relative_path} "
                f"title={asset.title!r} channel={asset.channel or metadata.get('channel') or '-'}"
            )
            updated += 1
            if dry_run:
                if limit is not None and updated >= limit:
                    break
                continue

            curation = asset.curation or AssetCuration(asset_id=asset.id)
            curation.selection_status = normalized_selection_status
            curation.cohort = cohort
            curation.reason = reason
            session.add(curation)
            if limit is not None and updated >= limit:
                break

        if not dry_run:
            session.commit()

    action = "matched" if dry_run else "updated"
    print(f"Curation {action}: {updated}")


def corpus_status(
    session_factory: sessionmaker[Session],
    channel: str | None,
    selection_status: str | None,
    cohort: str | None,
    subcohort: str | None,
) -> None:
    with session_factory() as session:
        assets = session.scalars(select(Asset).order_by(Asset.relative_path)).all()
        filtered_assets = [
            asset
            for asset in assets
            if asset_matches_filters(
                asset=asset,
                metadata=safe_json_load(asset.metadata_json),
                channel=channel,
                require_tags=None,
                exclude_tags=None,
                title_contains=None,
                source_url_contains=None,
                selection_status=selection_status,
                cohort=cohort,
                subcohort=subcohort,
            )
        ]
        if not filtered_assets:
            print("No assets matched the requested status scope.")
            return

        total_assets = len(filtered_assets)
        total_by_media_type = {"text": 0, "audio": 0, "video": 0}
        selection_counts = {"included": 0, "excluded": 0, "unset": 0}
        transcript_assets = 0
        asset_embedding_assets = 0
        timeline_assets = 0
        feedback_assets = 0
        text_chunk_embeddings = 0
        timeline_segment_total = 0
        timeline_segment_embedded = 0
        timeline_segment_feedback = 0

        for asset in filtered_assets:
            total_by_media_type[asset.media_type] = total_by_media_type.get(asset.media_type, 0) + 1
            selection_counts[asset_selection_status(asset)] += 1
            if asset.transcript_storage_path:
                transcript_assets += 1
            if any(
                infer_embedding_kind_from_metadata(
                    safe_json_load(record.metadata_json),
                    record.media_type,
                    record.chunk_index,
                )
                == "asset"
                for record in asset.embedding_records
            ):
                asset_embedding_assets += 1
            if asset.feedback_scores:
                feedback_assets += 1

            if asset.media_type == "text":
                text_chunk_embeddings += sum(
                    1
                    for record in asset.embedding_records
                    if infer_embedding_kind_from_metadata(
                        safe_json_load(record.metadata_json),
                        record.media_type,
                        record.chunk_index,
                    )
                    == "text_chunk"
                )
                continue

            if asset.timeline_segments:
                timeline_assets += 1
            timeline_segment_total += len(asset.timeline_segments)
            for segment in asset.timeline_segments:
                if any(
                    (
                        safe_json_load(record.metadata_json).get("timeline_segment_id") == segment.id
                        or (
                            infer_embedding_kind_from_metadata(
                                safe_json_load(record.metadata_json),
                                record.media_type,
                                record.chunk_index,
                            )
                            == "timeline_segment"
                            and record.chunk_index == segment.segment_index
                        )
                    )
                    for record in asset.embedding_records
                ):
                    timeline_segment_embedded += 1
                if any(score.scope_type == "timeline_segment" and score.scope_key == str(segment.id) for score in asset.feedback_scores):
                    timeline_segment_feedback += 1

        print("Corpus status:")
        print(f"  assets={total_assets} text={total_by_media_type['text']} audio={total_by_media_type['audio']} video={total_by_media_type['video']}")
        print(
            "  selection="
            f"included:{selection_counts['included']} "
            f"excluded:{selection_counts['excluded']} "
            f"unset:{selection_counts['unset']}"
        )
        print(
            "  coverage="
            f"transcripts:{transcript_assets}/{total_assets} "
            f"asset_embeddings:{asset_embedding_assets}/{total_assets} "
            f"feedback_assets:{feedback_assets}/{total_assets}"
        )
        print(
            "  timeline="
            f"assets_with_timeline:{timeline_assets} "
            f"segments:{timeline_segment_total} "
            f"embedded:{timeline_segment_embedded} "
            f"feedback_scored:{timeline_segment_feedback}"
        )
        print(f"  text_chunk_embeddings={text_chunk_embeddings}")


def auto_curate_bankara_assets(
    session_factory: sessionmaker[Session],
    target_channel: str,
    include_keywords: list[str],
    exclude_keywords: list[str],
    include_threshold: float,
    exclude_threshold: float,
    media_type: str | None,
    selection_status: str | None,
    cohort: str,
    reason_prefix: str,
    limit: int | None,
    dry_run: bool,
) -> None:
    reviewed = 0
    applied = 0
    with session_factory() as session:
        assets = select_assets_for_filters(
            session=session,
            media_type=media_type,
            selection_status=selection_status,
        )
        for asset in assets:
            metadata = safe_json_load(asset.metadata_json)
            recommendation = classify_bankara_comedy_asset(
                asset=asset,
                metadata=metadata,
                target_channel=target_channel,
                include_keywords=include_keywords,
                exclude_keywords=exclude_keywords,
                include_threshold=include_threshold,
                exclude_threshold=exclude_threshold,
            )
            print(
                f"{recommendation['recommendation']:<8} score={recommendation['score']:+.2f} "
                f"{asset.relative_path} title={asset.title!r}"
            )
            if recommendation["reasons"]:
                print(f"  reasons={'; '.join(recommendation['reasons'])}")
            reviewed += 1

            if recommendation["recommendation"] == "unset" or dry_run:
                if limit is not None and reviewed >= limit:
                    break
                continue

            curation = asset.curation or AssetCuration(asset_id=asset.id)
            curation.selection_status = recommendation["recommendation"]
            curation.cohort = cohort
            curation.reason = f"{reason_prefix} | score={recommendation['score']} | " + "; ".join(recommendation["reasons"])
            session.add(curation)
            applied += 1
            if limit is not None and reviewed >= limit:
                break

        if not dry_run:
            session.commit()

    action = "reviewed" if dry_run else "applied"
    print(f"Auto-curation {action}: reviewed={reviewed} changed={applied}")


def infer_bankara_asset_cohort(asset: Asset, metadata: dict[str, Any]) -> dict[str, Any]:
    title_haystack = asset.title.casefold()

    character = "ensemble"
    character_reason = "character:ensemble"
    for keyword, label in (
        ("最恐の母", "mother"),
        ("母親", "mother"),
        ("天才幼稚園生", "kindergarten-kid"),
        ("天才小学生", "genius-kid"),
        ("うど潤", "udojun"),
        ("小学生", "school-kid"),
    ):
        if keyword.casefold() in title_haystack:
            character = label
            character_reason = f"character:{keyword}"
            break

    format_label = "scenario"
    format_reason = "format:scenario"
    if any(keyword.casefold() in title_haystack for keyword in ("鬼滅", "トトロ", "コナン", "のび太", "スマブラ", "マイクラ")):
        format_label = "parody"
        format_reason = "format:parody"
    elif any(keyword.casefold() in title_haystack for keyword in ("逃走中", "人狼", "ロブロックス", "8番出口", "バトルロワイヤル")):
        format_label = "game-world"
        format_reason = "format:game-world"
    elif any(keyword.casefold() in title_haystack for keyword in ("殺し屋", "強盗犯", "スパイ")):
        format_label = "crime"
        format_reason = "format:crime"
    elif any(keyword.casefold() in title_haystack for keyword in ("夏休み", "運動会", "遠足", "マラソン大会", "調理実習")):
        format_label = "school-event"
        format_reason = "format:school-event"
    elif any(keyword.casefold() in title_haystack for keyword in ("ヒーロー", "レンジャー")):
        format_label = "hero"
        format_reason = "format:hero"
    elif any(keyword.casefold() in title_haystack for keyword in ("開いたら", "店長", "コンビニ", "スーパー", "ラーメン屋", "寿司屋", "ハンバーガー屋", "焼肉屋", "中華料理屋", "おもちゃ屋")):
        format_label = "shop"
        format_reason = "format:shop"
    elif "なったら" in title_haystack or any(
        keyword.casefold() in title_haystack
        for keyword in ("消防士", "警察官", "医者", "教師", "総理大臣", "先生")
    ):
        format_label = "profession"
        format_reason = "format:profession"

    subcohort = ""
    subcohort_reasons: list[str] = []
    if character == "mother" and format_label == "profession":
        if any(keyword.casefold() in title_haystack for keyword in ("教師", "先生", "校長", "保育士", "幼稚園")):
            subcohort = "mother-profession-school-authority"
            subcohort_reasons.append("subcohort:school-authority")
        elif any(keyword.casefold() in title_haystack for keyword in ("警察官", "警官", "刑事", "裁判官", "検事", "弁護士")):
            subcohort = "mother-profession-law-authority"
            subcohort_reasons.append("subcohort:law-authority")
        elif any(keyword.casefold() in title_haystack for keyword in ("消防士", "医者", "看護師", "救急", "病院", "院長")):
            subcohort = "mother-profession-emergency-authority"
            subcohort_reasons.append("subcohort:emergency-authority")
        elif any(keyword.casefold() in title_haystack for keyword in ("総理大臣", "大統領", "知事", "市長", "政治家", "議員")):
            subcohort = "mother-profession-national-authority"
            subcohort_reasons.append("subcohort:national-authority")
    elif character == "genius-kid" and format_label == "shop":
        if any(
            keyword.casefold() in title_haystack
            for keyword in ("寿司屋", "焼肉屋", "ハンバーガー屋", "ラーメン屋", "中華料理屋", "うどん屋")
        ):
            subcohort = "genius-kid-shop-food-retail"
            subcohort_reasons.append("subcohort:food-retail")
        elif any(
            keyword.casefold() in title_haystack
            for keyword in ("コンビニ", "スーパー", "おもちゃ屋", "店長", "雑貨屋", "文房具屋")
        ):
            subcohort = "genius-kid-shop-general-retail"
            subcohort_reasons.append("subcohort:general-retail")
    elif character == "genius-kid" and format_label == "school-event":
        if any(keyword.casefold() in title_haystack for keyword in ("運動会", "マラソン大会", "体育祭", "リレー")):
            subcohort = "genius-kid-school-event-athletic"
            subcohort_reasons.append("subcohort:athletic")
        elif any(keyword.casefold() in title_haystack for keyword in ("遠足", "夏休み", "修学旅行", "林間学校")):
            subcohort = "genius-kid-school-event-outing"
            subcohort_reasons.append("subcohort:outing")
        elif any(keyword.casefold() in title_haystack for keyword in ("調理実習", "授業参観", "学芸会", "文化祭")):
            subcohort = "genius-kid-school-event-classroom"
            subcohort_reasons.append("subcohort:classroom")
    elif character == "school-kid" and format_label == "parody":
        if any(keyword.casefold() in title_haystack for keyword in ("コナン", "名探偵", "探偵", "推理")):
            subcohort = "school-kid-parody-detective"
            subcohort_reasons.append("subcohort:detective")
        elif any(keyword.casefold() in title_haystack for keyword in ("スマブラ", "マイクラ", "マインクラフト", "大乱闘")):
            subcohort = "school-kid-parody-game-world"
            subcohort_reasons.append("subcohort:game-world")

    return {
        "cohort": f"{character}-{format_label}",
        "subcohort": subcohort,
        "character": character,
        "format": format_label,
        "reasons": [character_reason, format_reason, *subcohort_reasons],
        "tags": metadata.get("tags") or [],
    }


def auto_assign_cohorts(
    session_factory: sessionmaker[Session],
    media_type: str | None,
    channel: str | None,
    selection_status: str | None,
    require_tags: list[str] | None,
    exclude_tags: list[str] | None,
    title_contains: list[str] | None,
    source_url_contains: list[str] | None,
    limit: int | None,
    dry_run: bool,
) -> None:
    reviewed = 0
    changed = 0
    counts: Counter[str] = Counter()
    subcohort_counts: Counter[str] = Counter()
    with session_factory() as session:
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

        for asset in assets:
            inferred = infer_bankara_asset_cohort(asset, safe_json_load(asset.metadata_json))
            counts[inferred["cohort"]] += 1
            if inferred["subcohort"]:
                subcohort_counts[inferred["subcohort"]] += 1
            print(
                f"{inferred['cohort']:<28} {asset.relative_path} "
                f"subcohort={inferred['subcohort'] or '-'} "
                f"title={asset.title!r} reasons={', '.join(inferred['reasons'])}"
            )
            reviewed += 1
            if dry_run:
                continue

            curation = asset.curation or AssetCuration(asset_id=asset.id)
            curation.selection_status = asset_selection_status(asset)
            curation.cohort = inferred["cohort"]
            session.add(curation)
            metadata = safe_json_load(asset.metadata_json)
            metadata["curation_character"] = inferred["character"]
            metadata["curation_format"] = inferred["format"]
            metadata["curation_subcohort"] = inferred["subcohort"]
            metadata["curation_inference_reasons"] = inferred["reasons"]
            asset.metadata_json = json.dumps(metadata, ensure_ascii=False)
            session.add(asset)
            changed += 1

        if not dry_run:
            session.commit()

    summary = " ".join(f"{cohort}={count}" for cohort, count in sorted(counts.items()))
    subcohort_summary = " ".join(
        f"{subcohort}={count}"
        for subcohort, count in sorted(subcohort_counts.items())
    )
    action = "reviewed" if dry_run else "updated"
    print(
        f"Auto cohort assignment {action}: assets={reviewed} changed={changed} {summary}".rstrip()
    )
    if subcohort_summary:
        print(f"Subcohort breakdown: {subcohort_summary}")


def audit_assets(
    session_factory: sessionmaker[Session],
    media_type: str | None,
    channel: str | None,
    selection_status: str | None,
    cohort: str | None,
    subcohort: str | None,
    require_tags: list[str] | None,
    exclude_tags: list[str] | None,
    title_contains: list[str] | None,
    source_url_contains: list[str] | None,
    only_problems: bool,
    only_blockers: bool,
    only_warnings: bool,
    limit: int,
    json_output: Path | None,
    summary_output: Path | None,
) -> dict[str, Any]:
    if only_blockers and only_warnings:
        raise ValueError("audit-assets cannot combine --only-blockers and --only-warnings.")
    rows: list[dict[str, Any]] = []
    emitted_rows: list[dict[str, Any]] = []
    with session_factory() as session:
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
        for asset in assets[:limit]:
            metadata = safe_json_load(asset.metadata_json)
            problems = detect_asset_problems(asset)
            blockers, warnings = split_asset_problems(problems)

            row = {
                "relative_path": asset.relative_path,
                "title": asset.title,
                "media_type": asset.media_type,
                "selection_status": asset_selection_status(asset),
                "subcohort": asset_subcohort(asset, metadata),
                "channel": asset.channel or metadata.get("channel"),
                "youtube_video_id": asset.youtube_video_id,
                "timeline_segments": len(asset.timeline_segments),
                "embedding_records": len(asset.embedding_records),
                "feedback_scores": len(asset.feedback_scores),
                "problems": problems,
                "blockers": blockers,
                "warnings": warnings,
                "has_blockers": bool(blockers),
                "has_warnings": bool(warnings),
            }
            rows.append(row)

            if only_problems and not problems:
                continue
            if only_blockers and not blockers:
                continue
            if only_warnings and not warnings:
                continue
            emitted_rows.append(row)

    summary = build_audit_summary(rows, emitted_rows=len(emitted_rows))

    if json_output:
        json_output.parent.mkdir(parents=True, exist_ok=True)
        json_output.write_text(json.dumps(emitted_rows, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Wrote audit report: {json_output}")
    if summary_output:
        summary_output.parent.mkdir(parents=True, exist_ok=True)
        summary_output.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Wrote audit summary: {summary_output}")

    if not emitted_rows:
        print("No audit rows matched.")
        print(
            f"Audit summary: scanned={summary['assets_scanned']} "
            f"blocker_assets={summary['assets_with_blockers']} "
            f"warning_assets={summary['assets_with_warnings']}"
        )
        return summary

    for row in emitted_rows:
        print(
            f"{row['media_type']:<5} {row['relative_path']} "
            f"selection={row['selection_status']} "
            f"subcohort={row['subcohort'] or '-'} "
            f"blockers={','.join(row['blockers']) or '-'} "
            f"warnings={','.join(row['warnings']) or '-'}"
        )
    print(
        f"Audit summary: scanned={summary['assets_scanned']} "
        f"blocker_assets={summary['assets_with_blockers']} "
        f"warning_assets={summary['assets_with_warnings']}"
    )
    return summary


def quarantine_assets(
    session_factory: sessionmaker[Session],
    media_type: str | None,
    channel: str | None,
    selection_status: str | None,
    require_tags: list[str] | None,
    exclude_tags: list[str] | None,
    title_contains: list[str] | None,
    source_url_contains: list[str] | None,
    problem_filters: list[str] | None,
    severity_filters: list[str] | None,
    cohort: str,
    reason_prefix: str,
    limit: int | None,
    dry_run: bool,
) -> None:
    reviewed = 0
    changed = 0
    normalized_problem_filters = {problem.strip() for problem in (problem_filters or []) if problem.strip()}
    normalized_severity_filters = {
        severity.strip().lower() for severity in (severity_filters or []) if severity and severity.strip()
    }
    if not normalized_problem_filters and not normalized_severity_filters:
        raise ValueError("quarantine-assets requires at least one --problem or --severity filter.")

    with session_factory() as session:
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
        for asset in assets:
            problems = detect_asset_problems(asset)
            blockers, warnings = split_asset_problems(problems)
            matched_problems = set(normalized_problem_filters).intersection(problems)
            if "blocker" in normalized_severity_filters:
                matched_problems.update(blockers)
            if "warning" in normalized_severity_filters:
                matched_problems.update(warnings)
            if not matched_problems:
                continue

            reviewed += 1
            print(
                f"exclude  {asset.relative_path} "
                f"selection={asset_selection_status(asset)} "
                f"blockers={','.join(blockers) or '-'} "
                f"warnings={','.join(warnings) or '-'}"
            )

            if dry_run:
                if limit is not None and reviewed >= limit:
                    break
                continue

            curation = asset.curation or AssetCuration(asset_id=asset.id)
            curation.selection_status = "excluded"
            curation.cohort = cohort
            curation.reason = f"{reason_prefix} | matched={','.join(sorted(matched_problems)) or '-'}"
            session.add(curation)
            changed += 1
            if limit is not None and reviewed >= limit:
                break

        if not dry_run:
            session.commit()

    action = "reviewed" if dry_run else "excluded"
    print(f"Quarantine {action}: reviewed={reviewed} changed={changed}")
