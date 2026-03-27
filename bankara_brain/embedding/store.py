"""Pinecone upsert/delete operations, metadata processing, and state management."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from bankara_brain.embedding.config import (
    FEEDBACK_SCORE_FIELDS,
    STATE_VERSION,
    DEFAULT_STATE_FILE,
)
from bankara_brain.embedding.client import with_transient_retries


def upsert_embedding(
    index: Any,
    namespace: str,
    record_id: str,
    vector: list[float],
    metadata: dict[str, Any],
) -> None:
    with_transient_retries(
        action_label="Pinecone upsert",
        operation=lambda: index.upsert(
            namespace=namespace,
            vectors=[
                {
                    "id": record_id,
                    "values": vector,
                    "metadata": sanitize_metadata(metadata),
                }
            ],
        ),
    )


def delete_embeddings(index: Any, namespace: str, record_ids: list[str]) -> None:
    if not record_ids:
        return
    with_transient_retries(
        action_label="Pinecone delete",
        operation=lambda: index.delete(namespace=namespace, ids=record_ids),
    )


def sanitize_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    normalized = prepare_metadata_for_index(metadata)
    clean: dict[str, Any] = {}
    for key, value in normalized.items():
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


def prepare_metadata_for_index(metadata: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(metadata)
    normalized = flatten_feedback_summary_metadata(
        normalized,
        summary_key="feedback_summary",
        prefix="",
        set_primary=True,
    )
    normalized = flatten_feedback_summary_metadata(
        normalized,
        summary_key="asset_feedback_summary",
        prefix="asset_",
        set_primary=False,
    )
    normalized = flatten_feedback_summary_metadata(
        normalized,
        summary_key="segment_feedback_summary",
        prefix="segment_",
        set_primary=False,
    )

    if normalized.get("feedback_score_v1") is None:
        for fallback_key in ("segment_feedback_score_v1", "asset_feedback_score_v1"):
            fallback = coerce_float(normalized.get(fallback_key))
            if fallback is not None:
                normalized["feedback_score_v1"] = fallback
                break

    return normalized


def flatten_feedback_summary_metadata(
    metadata: dict[str, Any],
    summary_key: str,
    prefix: str,
    set_primary: bool,
) -> dict[str, Any]:
    summary = parse_feedback_summary_value(metadata.get(summary_key))
    if not summary:
        return metadata

    enriched = dict(metadata)
    enriched[summary_key] = summary

    end_date = summary.get("end_date")
    end_date_key = f"{prefix}feedback_end_date" if prefix else "feedback_end_date"
    if end_date:
        enriched[end_date_key] = end_date
        if set_primary:
            enriched["feedback_end_date"] = end_date

    for field in FEEDBACK_SCORE_FIELDS:
        value = coerce_float(summary.get(field))
        if value is None:
            continue
        field_key = f"{prefix}{field}" if prefix else field
        enriched[field_key] = value
        if set_primary:
            enriched[field] = value

    return enriched


def parse_feedback_summary_value(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        loaded = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_generated_json_payload(text: str) -> dict[str, Any]:
    stripped = text.strip()
    if not stripped:
        raise ValueError("Empty Gemini JSON response.")
    candidates = [stripped]
    if "```json" in stripped:
        candidates.append(stripped.split("```json", 1)[1].split("```", 1)[0].strip())
    elif "```" in stripped:
        candidates.append(stripped.split("```", 1)[1].split("```", 1)[0].strip())
    if "{" in stripped and "}" in stripped:
        candidates.append(stripped[stripped.find("{") : stripped.rfind("}") + 1].strip())

    for candidate in candidates:
        try:
            loaded = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(loaded, dict):
            return loaded
    raise ValueError("Gemini facet extraction returned invalid JSON.")


def load_state(state_file: Path) -> dict[str, Any]:
    if not state_file.exists():
        return {"version": STATE_VERSION, "files": {}}

    try:
        data = json.loads(state_file.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid state file: {state_file} ({exc})") from exc

    if not isinstance(data, dict):
        raise ValueError(f"State file must contain a JSON object: {state_file}")

    version = data.get("version")
    if version != STATE_VERSION:
        return {"version": STATE_VERSION, "files": {}}

    files = data.get("files")
    if not isinstance(files, dict):
        files = {}

    return {"version": STATE_VERSION, "files": files}


def save_state(state_file: Path, state: dict[str, Any]) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
