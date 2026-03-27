from __future__ import annotations

import json
import os
from typing import Any

DEFAULT_CROSS_ENCODER_MODEL = "gemini-2.5-flash"
DEFAULT_CROSS_ENCODER_TOP_K = 12
DEFAULT_CROSS_ENCODER_WEIGHT = 0.20
SUMMARY_TEXT_KEY = "brain_summary_text_v1"
SUMMARY_JSON_KEY = "brain_summary_v1"


def rerank_matches_with_gemini(
    query: str,
    matches: list[dict[str, Any]],
    *,
    model: str = DEFAULT_CROSS_ENCODER_MODEL,
    top_k: int = DEFAULT_CROSS_ENCODER_TOP_K,
) -> list[dict[str, Any]]:
    client = build_default_genai_client()
    return rerank_matches_with_client(
        client,
        query,
        matches,
        model=model,
        top_k=top_k,
    )


def rerank_matches_with_client(
    client: Any,
    query: str,
    matches: list[dict[str, Any]],
    *,
    model: str = DEFAULT_CROSS_ENCODER_MODEL,
    top_k: int = DEFAULT_CROSS_ENCODER_TOP_K,
    score_weight: float = DEFAULT_CROSS_ENCODER_WEIGHT,
) -> list[dict[str, Any]]:
    if not matches:
        return []

    bounded_top_k = max(1, min(int(top_k), len(matches)))
    head = [clone_match(match) for match in matches[:bounded_top_k]]
    tail = [clone_match(match) for match in matches[bounded_top_k:]]
    prepared_candidates = [prepare_cross_encoder_candidate(match, index=i) for i, match in enumerate(head)]

    try:
        response_payload = cross_encoder_score_candidates(
            client=client,
            model=model,
            query=query,
            candidates=prepared_candidates,
        )
        scored = apply_cross_encoder_scores(
            matches=head,
            results=response_payload.get("results") or [],
            score_weight=score_weight,
        )
    except Exception as exc:  # pragma: no cover - exercised through tests via fallback behavior
        scored = apply_cross_encoder_fallback(
            matches=head,
            reason=f"cross-encoder unavailable: {exc}",
        )

    scored.sort(
        key=lambda match: (
            float(match.get("reranked_combined_score", match.get("combined_score", 0.0)) or 0.0),
            float(match.get("cross_encoder_score", 0.0) or 0.0),
            float(match.get("combined_score", 0.0) or 0.0),
        ),
        reverse=True,
    )
    return scored + tail


def build_default_genai_client() -> Any:
    try:
        from google import genai
    except ImportError as exc:  # pragma: no cover - import failure is environment-specific
        raise RuntimeError("Cross-encoder rerank requires google-genai to be installed.") from exc

    api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY (or GOOGLE_API_KEY) for cross-encoder reranking.")
    return genai.Client(api_key=api_key)


def cross_encoder_score_candidates(
    client: Any,
    *,
    model: str,
    query: str,
    candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    try:
        from google.genai import types
    except ImportError as exc:  # pragma: no cover - import failure is environment-specific
        raise RuntimeError("Cross-encoder rerank requires google-genai to be installed.") from exc

    prompt = build_cross_encoder_prompt(query=query, candidates=candidates)
    response = client.models.generate_content(
        model=model,
        contents=prompt,
        config=types.GenerateContentConfig(
            temperature=0,
            response_mime_type="application/json",
        ),
    )
    return parse_cross_encoder_response(getattr(response, "text", ""))


def build_cross_encoder_prompt(query: str, candidates: list[dict[str, Any]]) -> str:
    serialized = json.dumps({"query": query, "candidates": candidates}, ensure_ascii=False, indent=2)
    return (
        "あなたはバンカラブレインの検索 reranker です。\n"
        "query と candidate の一致度を 0.0 から 1.0 で厳密に採点してください。\n"
        "語感だけでなく premise, authority, setting, payoff, cohort/subcohort の一致を重視してください。\n"
        "返答は JSON のみ。\n"
        "形式:\n"
        "{\n"
        '  "results": [\n'
        '    {"index": 0, "score": 0.0, "reason": "short factual reason"}\n'
        "  ]\n"
        "}\n\n"
        "Rules:\n"
        "- score は 0.0-1.0\n"
        "- 既存回の lane/cohort が query とズレるなら厳しく下げる\n"
        "- reason は短く事実ベース\n"
        "- index は入力 candidate の index をそのまま使う\n\n"
        f"{serialized}\n"
    )


def parse_cross_encoder_response(text: str) -> dict[str, Any]:
    stripped = (text or "").strip()
    if not stripped:
        raise ValueError("Empty cross-encoder response.")

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
            loaded["results"] = normalize_cross_encoder_results(loaded.get("results"))
            return loaded
    raise ValueError("Invalid cross-encoder JSON response.")


def normalize_cross_encoder_results(results: Any) -> list[dict[str, Any]]:
    if not isinstance(results, list):
        return []
    normalized: list[dict[str, Any]] = []
    for item in results:
        if not isinstance(item, dict):
            continue
        index = item.get("index")
        try:
            index_int = int(index)
        except (TypeError, ValueError):
            continue
        score = clamp_score(item.get("score"))
        reason = str(item.get("reason") or "").strip()
        normalized.append(
            {
                "index": index_int,
                "score": score,
                "reason": reason[:240],
            }
        )
    return normalized


def clamp_score(value: Any) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return 0.0
    return round(max(0.0, min(1.0, numeric)), 4)


def apply_cross_encoder_scores(
    *,
    matches: list[dict[str, Any]],
    results: list[dict[str, Any]],
    score_weight: float,
) -> list[dict[str, Any]]:
    by_index = {
        int(item["index"]): {
            "score": clamp_score(item.get("score")),
            "reason": str(item.get("reason") or "").strip(),
        }
        for item in results
        if isinstance(item, dict) and item.get("index") is not None
    }

    enriched: list[dict[str, Any]] = []
    for index, match in enumerate(matches):
        current = clone_match(match)
        score_record = by_index.get(index)
        cross_score = score_record["score"] if score_record else 0.0
        reason = score_record["reason"] if score_record and score_record["reason"] else "no rerank reason returned"
        base_combined = float(current.get("combined_score", current.get("semantic_score", 0.0)) or 0.0)
        current["cross_encoder_score"] = cross_score
        current["cross_encoder_reason"] = reason
        current["reranked_combined_score"] = round(base_combined + (score_weight * cross_score), 6)
        enriched.append(current)
    return enriched


def apply_cross_encoder_fallback(matches: list[dict[str, Any]], reason: str) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    for match in matches:
        current = clone_match(match)
        base_combined = float(current.get("combined_score", current.get("semantic_score", 0.0)) or 0.0)
        current["cross_encoder_score"] = 0.0
        current["cross_encoder_reason"] = reason
        current["reranked_combined_score"] = round(base_combined, 6)
        enriched.append(current)
    return enriched


def prepare_cross_encoder_candidate(match: dict[str, Any], *, index: int) -> dict[str, Any]:
    metadata = dict(match.get("metadata") or {})
    title = str(metadata.get("title") or metadata.get("relative_path") or f"candidate-{index}")
    searchable_summary = extract_searchable_summary(metadata)
    transcript_excerpt = extract_transcript_excerpt(metadata)
    lane = str(metadata.get("curation_subcohort") or metadata.get("curation_cohort") or "").strip()
    candidate = {
        "index": index,
        "id": match.get("id"),
        "title": title,
        "cohort": metadata.get("curation_cohort") or "",
        "subcohort": metadata.get("curation_subcohort") or "",
        "lane": lane,
        "media_type": metadata.get("media_type") or "",
        "embedding_kind": metadata.get("embedding_kind") or "",
        "current_combined_score": round(float(match.get("combined_score", match.get("semantic_score", 0.0)) or 0.0), 6),
        "semantic_score": round(float(match.get("semantic_score", 0.0) or 0.0), 6),
        "feedback_score": round(float(match.get("feedback_score", 0.0) or 0.0), 6),
        "facet_score": round(float(match.get("facet_score", 0.0) or 0.0), 6),
        "searchable_summary": searchable_summary,
        "transcript_excerpt": transcript_excerpt,
    }
    return candidate


def extract_searchable_summary(metadata: dict[str, Any]) -> str:
    text_value = metadata.get(SUMMARY_TEXT_KEY)
    if isinstance(text_value, str) and text_value.strip():
        return text_value.strip()

    raw_summary = metadata.get(SUMMARY_JSON_KEY)
    if isinstance(raw_summary, str):
        try:
            raw_summary = json.loads(raw_summary)
        except json.JSONDecodeError:
            raw_summary = {}
    if not isinstance(raw_summary, dict):
        return ""

    preferred_fields = (
        "searchable_summary",
        "premise",
        "character_engine",
        "authority_flip",
        "hook_pattern",
        "escalation_pattern",
        "payoff_pattern",
        "setting",
    )
    parts: list[str] = []
    for field_name in preferred_fields:
        value = raw_summary.get(field_name)
        if isinstance(value, str) and value.strip():
            parts.append(value.strip())
    tone_tags = raw_summary.get("tone_tags")
    if isinstance(tone_tags, list):
        normalized_tags = [str(item).strip() for item in tone_tags if str(item).strip()]
        if normalized_tags:
            parts.append("tone_tags: " + ", ".join(normalized_tags))
    return "\n".join(parts)


def extract_transcript_excerpt(metadata: dict[str, Any]) -> str:
    for key in ("transcript_excerpt", "notes", "text", "transcript", "chunk_text"):
        value = metadata.get(key)
        if isinstance(value, str) and value.strip():
            return shorten_text(value.strip(), limit=500)
    return ""


def shorten_text(value: str, *, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: max(0, limit - 1)].rstrip() + "…"


def clone_match(match: dict[str, Any]) -> dict[str, Any]:
    cloned = dict(match)
    if isinstance(match.get("metadata"), dict):
        cloned["metadata"] = dict(match["metadata"])
    if isinstance(match.get("facet_breakdown"), dict):
        cloned["facet_breakdown"] = dict(match["facet_breakdown"])
    return cloned
