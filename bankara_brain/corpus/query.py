"""Asset querying, filtering, and selection helpers."""
from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from bankara_brain.models import Asset
from bankara_brain.utils import humanize_stem, safe_json_load


# ── Filter normalization ─────────────────────────────────────────────────────

def normalize_filter_values(values: list[str] | None) -> list[str]:
    if not values:
        return []
    normalized = []
    for value in values:
        text = str(value).strip()
        if text:
            normalized.append(text.casefold())
    return normalized


def normalize_match_text(value: str | None) -> str:
    if not value:
        return ""
    text = humanize_stem(str(value)).casefold()
    return re.sub(r"[^\w]+", "", text, flags=re.UNICODE)


def normalize_selection_status(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip().casefold()
    if not normalized:
        return None
    if normalized not in {"included", "excluded", "unset"}:
        raise ValueError(f"Unsupported selection status: {value}")
    return normalized


def normalize_cohort(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    return normalized.casefold()


def normalize_subcohort(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    return normalized.casefold()


# ── Asset attribute accessors ────────────────────────────────────────────────

def asset_selection_status(asset: Asset) -> str:
    if asset.curation and asset.curation.selection_status:
        return asset.curation.selection_status
    return "unset"


def asset_cohort(asset: Asset) -> str:
    if asset.curation and asset.curation.cohort:
        return asset.curation.cohort.strip()
    return ""


def asset_subcohort(asset: Asset, metadata: dict[str, Any] | None = None) -> str:
    resolved_metadata = metadata if metadata is not None else safe_json_load(asset.metadata_json)
    for key in ("curation_subcohort", "bankara_subcohort", "subcohort"):
        value = str(resolved_metadata.get(key) or "").strip()
        if value:
            return value
    return ""


def effective_cohort_label(cohort: str | None, subcohort: str | None = None) -> str:
    return (subcohort or cohort or "").strip()


# ── Querying ─────────────────────────────────────────────────────────────────

def select_assets_for_filters(
    session: Session,
    media_type: str | None = None,
    channel: str | None = None,
    require_tags: list[str] | None = None,
    exclude_tags: list[str] | None = None,
    title_contains: list[str] | None = None,
    source_url_contains: list[str] | None = None,
    selection_status: str | None = None,
    cohort: str | None = None,
    subcohort: str | None = None,
    require_youtube_video_id: bool = False,
) -> list[Asset]:
    """Select assets matching a set of filter criteria."""
    stmt = select(Asset).order_by(Asset.relative_path)
    if media_type:
        stmt = stmt.where(Asset.media_type == media_type)
    if require_youtube_video_id:
        stmt = stmt.where(Asset.youtube_video_id.is_not(None))

    assets = session.scalars(stmt).all()
    return [
        asset
        for asset in assets
        if asset_matches_filters(
            asset=asset,
            metadata=safe_json_load(asset.metadata_json),
            channel=channel,
            require_tags=require_tags,
            exclude_tags=exclude_tags,
            title_contains=title_contains,
            source_url_contains=source_url_contains,
            selection_status=selection_status,
            cohort=cohort,
            subcohort=subcohort,
        )
    ]


def asset_matches_filters(
    asset: Asset,
    metadata: dict[str, Any],
    channel: str | None,
    require_tags: list[str] | None,
    exclude_tags: list[str] | None,
    title_contains: list[str] | None,
    source_url_contains: list[str] | None,
    selection_status: str | None = None,
    cohort: str | None = None,
    subcohort: str | None = None,
) -> bool:
    """Return True if *asset* passes all filter criteria."""
    normalized_selection_status = normalize_selection_status(selection_status)
    if normalized_selection_status and asset_selection_status(asset) != normalized_selection_status:
        return False
    normalized_cohort = normalize_cohort(cohort)
    if normalized_cohort and asset_cohort(asset).casefold() != normalized_cohort:
        return False
    normalized_subcohort = normalize_subcohort(subcohort)
    if normalized_subcohort and asset_subcohort(asset, metadata).casefold() != normalized_subcohort:
        return False

    normalized_channel = channel.strip().casefold() if channel else ""
    asset_channel = (asset.channel or metadata.get("channel") or "").strip().casefold()
    if normalized_channel and asset_channel != normalized_channel:
        return False

    normalized_tags = {
        str(tag).strip().casefold()
        for tag in (metadata.get("tags") or [])
        if str(tag).strip()
    }
    for required in normalize_filter_values(require_tags):
        if required not in normalized_tags:
            return False
    for excluded in normalize_filter_values(exclude_tags):
        if excluded in normalized_tags:
            return False

    title_haystack = asset.title.casefold()
    for needle in normalize_filter_values(title_contains):
        if needle not in title_haystack:
            return False

    source_url_haystack = (asset.source_url or "").casefold()
    for needle in normalize_filter_values(source_url_contains):
        if needle not in source_url_haystack:
            return False

    return True


# ── Asset resolution & media path helpers ────────────────────────────────────


def resolve_asset(session: Session, asset_selector: str) -> Asset:
    """Find an asset by id, relative_path, or youtube_video_id."""
    asset = session.scalar(select(Asset).where(Asset.id == asset_selector))
    if asset:
        return asset

    asset = session.scalar(select(Asset).where(Asset.relative_path == asset_selector))
    if asset:
        return asset

    asset = session.scalar(select(Asset).where(Asset.youtube_video_id == asset_selector))
    if asset:
        return asset

    raise ValueError(f"Asset not found: {asset_selector}")


def resolve_existing_path(*raw_paths: str | Path | None) -> Path | None:
    """Return the first path that exists on disk, or ``None``."""
    for raw_path in raw_paths:
        if not raw_path:
            continue
        path = Path(raw_path).expanduser()
        if path.exists():
            return path.resolve()
    return None


def resolve_asset_media_path(asset: Asset) -> Path | None:
    """Return the resolved path to an asset's media file, preferring source_path."""
    return resolve_existing_path(asset.source_path, asset.storage_path)


def media_has_audio_stream(media_path: Path) -> bool | None:
    """Return whether *media_path* contains an audio stream (None if ffprobe unavailable)."""
    ffprobe_path = shutil.which("ffprobe")
    if not ffprobe_path:
        return None
    result = subprocess.run(
        [
            ffprobe_path,
            "-v",
            "error",
            "-select_streams",
            "a",
            "-show_entries",
            "stream=codec_type",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(media_path),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None
    return bool(result.stdout.strip())


# ── Semantic search helpers (moved from control plane) ────────────────────────

def resolve_search_match_asset(session: Session, match: dict[str, Any]) -> Asset | None:
    """Resolve an :class:`Asset` from a semantic search match dict."""
    metadata = match.get("metadata") or {}
    asset_id = metadata.get("asset_id")
    relative_path = metadata.get("relative_path")
    if asset_id:
        asset = session.scalar(select(Asset).where(Asset.id == str(asset_id)))
        if asset:
            return asset
    if relative_path:
        asset = session.scalar(select(Asset).where(Asset.relative_path == str(relative_path)))
        if asset:
            return asset
    return None


def filter_semantic_search_results_file(
    session_factory: sessionmaker[Session],
    search_results_path: Path,
    selection_status: str | None,
    cohort: str | None,
    subcohort: str | None = None,
) -> None:
    """Filter a search-results JSON file by curation criteria, rewriting in place."""
    if not selection_status and not cohort and not subcohort:
        return
    if not search_results_path.exists():
        raise FileNotFoundError(f"Search results file not found: {search_results_path}")

    payload = json.loads(search_results_path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        matches = payload.get("matches", [])
    elif isinstance(payload, list):
        matches = payload
    else:
        raise ValueError("Search results JSON must be an array or {\"matches\": [...]} for filtering.")

    kept_matches: list[dict[str, Any]] = []
    with session_factory() as session:
        for match in matches:
            if not isinstance(match, dict):
                continue
            asset = resolve_search_match_asset(session, match)
            if asset is None:
                continue
            if selection_status and asset_selection_status(asset) != selection_status:
                continue
            if cohort and asset_cohort(asset).casefold() != normalize_cohort(cohort):
                continue
            if subcohort and asset_subcohort(asset).casefold() != normalize_subcohort(subcohort):
                continue
            kept_matches.append(match)

    if isinstance(payload, dict):
        payload["matches"] = kept_matches
        payload["match_count"] = len(kept_matches)
        rendered = json.dumps(payload, ensure_ascii=False, indent=2)
    else:
        rendered = json.dumps(kept_matches, ensure_ascii=False, indent=2)
    search_results_path.write_text(rendered, encoding="utf-8")
    filters = []
    if selection_status:
        filters.append(f"selection_status={selection_status}")
    if cohort:
        filters.append(f"cohort={cohort}")
    if subcohort:
        filters.append(f"subcohort={subcohort}")
    print(f"Filtered semantic matches by {' '.join(filters)}: kept={len(kept_matches)}")


def run_semantic_search_export(
    session_factory: sessionmaker[Session],
    query: str,
    output_path: Path,
    semantic_limit: int,
    media_type: Optional[str],
    namespace: Optional[str],
    embedding_kind: Optional[str],
    rerank_feedback: bool,
    feedback_weight: float,
    candidate_k: Optional[int],
    min_feedback_score: Optional[float],
    cross_encoder_rerank: bool,
    cross_encoder_top_k: int,
    selection_status: Optional[str],
    cohort: Optional[str],
    subcohort: Optional[str],
) -> None:
    """Run the Pinecone semantic search CLI and write results to *output_path*."""
    # Resolve the search script relative to the project root
    # (bankara_brain/corpus/query.py → bankara_brain/corpus/ → bankara_brain/ → project root)
    project_root = Path(__file__).resolve().parent.parent.parent
    search_script = project_root / "gemini_pinecone_multimodal_mvp.py"

    command = [
        sys.executable,
        str(search_script),
        "search",
        "--query",
        query,
        "--top-k",
        str(semantic_limit),
        "--json-output",
        str(output_path),
    ]
    if media_type:
        command.extend(["--media-type", media_type])
    if namespace:
        command.extend(["--namespace", namespace])
    if embedding_kind:
        command.extend(["--embedding-kind", embedding_kind])
    if selection_status:
        command.extend(["--selection-status", selection_status])
    if cohort:
        command.extend(["--cohort", cohort])
    if subcohort:
        command.extend(["--subcohort", subcohort])
    if rerank_feedback:
        command.append("--rerank-feedback")
    if feedback_weight != 0.15:
        command.extend(["--feedback-weight", str(feedback_weight)])
    if candidate_k is not None:
        command.extend(["--candidate-k", str(candidate_k)])
    if min_feedback_score is not None:
        command.extend(["--min-feedback-score", str(min_feedback_score)])
    if cross_encoder_rerank:
        command.append("--cross-encoder-rerank")
    if cross_encoder_top_k != 12:
        command.extend(["--cross-encoder-top-k", str(cross_encoder_top_k)])

    result = subprocess.run(command, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise RuntimeError(
            "Semantic search export failed.\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    filter_semantic_search_results_file(
        session_factory=session_factory,
        search_results_path=output_path,
        selection_status=selection_status,
        cohort=cohort,
        subcohort=subcohort,
    )
