"""Semantic search: facet analysis, text matching, ranking, and output."""

from __future__ import annotations

import json
import logging
import os
import re
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

from google import genai
from google.genai import types
from bankara_cross_encoder_rerank import rerank_matches_with_client

from bankara_brain.embedding.config import (
    EMBEDDING_MODEL,
    SUMMARY_TEXT_KEY,
    SUMMARY_JSON_KEY,
    QUERY_FACET_MODEL,
    SEGMENT_KIND_PRIORITY,
    FACET_CONFLICT_COMBINED_WEIGHT,
    LANE_CONFLICT_COMBINED_WEIGHT,
    CANONICAL_MATCH_TAGS,
    CANONICAL_TAG_GROUPS,
    QUERY_TARGET_LANE_HINTS,
    LANE_TARGET_GUARDS,
)
from bankara_brain.embedding.client import with_transient_retries, _get_attr
from bankara_brain.embedding.vectors import embed_text
from bankara_brain.embedding.store import (
    coerce_float,
    parse_feedback_summary_value,
    parse_generated_json_payload,
    prepare_metadata_for_index,
)


def normalize_matching_text(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"[\W_]+", "", str(value).casefold(), flags=re.UNICODE)


def infer_canonical_match_tags(value: str | None) -> list[str]:
    normalized_value = normalize_matching_text(value)
    if not normalized_value:
        return []
    tags: list[str] = []
    for canonical, aliases in CANONICAL_MATCH_TAGS.items():
        if any(normalize_matching_text(alias) in normalized_value for alias in (canonical, *aliases)):
            tags.append(canonical)
    return tags


def augment_matching_text(value: str | None) -> str:
    base = (value or "").strip()
    if not base:
        return ""
    tags = infer_canonical_match_tags(base)
    if not tags:
        return base
    return f"{base} {' '.join(tags)}"


def collect_canonical_tags_from_query_facets(query_facets: dict[str, Any]) -> set[str]:
    tags: set[str] = set()
    for value in query_facets.values():
        if isinstance(value, str):
            tags.update(infer_canonical_match_tags(value))
        elif isinstance(value, list):
            for item in value:
                tags.update(infer_canonical_match_tags(str(item)))
    return tags


def group_canonical_tags(tags: set[str]) -> dict[str, set[str]]:
    grouped: dict[str, set[str]] = {}
    for group_name, group_tags in CANONICAL_TAG_GROUPS.items():
        overlaps = tags & set(group_tags)
        if overlaps:
            grouped[group_name] = overlaps
    return grouped


def query_haystack_text(query_facets: dict[str, Any]) -> str:
    parts: list[str] = []
    for key in ("raw_query", "premise_focus", "authority_focus"):
        value = query_facets.get(key)
        if isinstance(value, str) and value.strip():
            parts.append(value.strip())
    for key in ("setting_cues", "tone_cues", "hook_cues", "escalation_cues", "payoff_cues", "novelty_cues"):
        value = query_facets.get(key)
        if isinstance(value, list):
            parts.extend(str(item).strip() for item in value if str(item).strip())
    return " ".join(parts)


def lane_broad_family_label(lane: str | None) -> str:
    normalized_lane = (lane or "").strip()
    if not normalized_lane:
        return ""
    for prefix, label in (
        ("mother-", "mother"),
        ("genius-kid-", "genius-kid"),
        ("school-kid-", "school-kid"),
        ("ensemble-", "ensemble"),
    ):
        if normalized_lane.startswith(prefix):
            return label
    return normalized_lane.split("-", 1)[0]


def lane_cluster_label(lane: str | None) -> str:
    normalized_lane = (lane or "").strip()
    if not normalized_lane:
        return ""
    for prefix, label in (
        ("mother-profession-", "mother-profession"),
        ("genius-kid-shop-", "genius-kid-shop"),
        ("genius-kid-school-event-", "genius-kid-school-event"),
        ("school-kid-parody-", "school-kid-parody"),
    ):
        if normalized_lane.startswith(prefix):
            return label
    return normalized_lane


def lane_target_allowed(lane: str, query_tags: set[str], haystack_text: str) -> bool:
    broad_family = lane_broad_family_label(lane)
    guard_tags = LANE_TARGET_GUARDS.get(broad_family) or ()
    if guard_tags and query_tags & set(guard_tags):
        return True
    if broad_family == "school-kid":
        return "小学生" in haystack_text or bool(query_tags & set(LANE_TARGET_GUARDS["school-kid"]))
    return not guard_tags


def infer_query_target_lanes(query_facets: dict[str, Any]) -> list[str]:
    explicit_targets = [
        str(item).strip()
        for item in (query_facets.get("target_lanes") or [])
        if str(item).strip()
    ]
    if explicit_targets:
        return explicit_targets

    haystack_text = query_haystack_text(query_facets)
    normalized_haystack = normalize_matching_text(haystack_text)
    query_tags = collect_canonical_tags_from_query_facets(query_facets)
    targets: list[str] = []
    for lane, aliases in QUERY_TARGET_LANE_HINTS.items():
        if not lane_target_allowed(lane, query_tags, haystack_text):
            continue
        if any(normalize_matching_text(alias) in normalized_haystack for alias in aliases):
            targets.append(lane)
    return targets


def score_target_lane_alignment(metadata: dict[str, Any], query_facets: dict[str, Any]) -> tuple[float, float, list[str], str]:
    target_lanes = infer_query_target_lanes(query_facets)
    candidate_lane = str(metadata.get("curation_subcohort") or metadata.get("curation_cohort") or "").strip()
    if not target_lanes or not candidate_lane:
        return 0.0, 0.0, target_lanes, candidate_lane

    best_alignment = 0.0
    best_conflict = 1.0
    candidate_broad = lane_broad_family_label(candidate_lane)
    candidate_cluster = lane_cluster_label(candidate_lane)

    for target_lane in target_lanes:
        if candidate_lane == target_lane:
            return 1.0, 0.0, target_lanes, candidate_lane

        target_broad = lane_broad_family_label(target_lane)
        target_cluster = lane_cluster_label(target_lane)
        if candidate_cluster and target_cluster and candidate_cluster == target_cluster:
            best_alignment = max(best_alignment, 0.55)
            best_conflict = min(best_conflict, 0.65)
        elif candidate_broad and target_broad and candidate_broad == target_broad:
            best_alignment = max(best_alignment, 0.20)
            best_conflict = min(best_conflict, 0.90)
        else:
            best_conflict = min(best_conflict, 1.00)

    return round(best_alignment, 4), round(best_conflict, 4), target_lanes, candidate_lane


def split_summary_field_values(value: str | None) -> list[str]:
    if not value:
        return []
    values = []
    for fragment in re.split(r"[、,/|・]| and | と |\n", value):
        text = fragment.strip()
        if text:
            values.append(text)
    return values


def extract_matching_fragments(value: str | None) -> list[str]:
    if not value:
        return []
    candidates = split_summary_field_values(value)
    candidates.extend(re.findall(r"[一-龥ぁ-んァ-ヶーA-Za-z0-9]{2,}", value))
    unique: list[str] = []
    for candidate in candidates:
        text = candidate.strip()
        if not text or text in unique:
            continue
        unique.append(text)
    return unique


def extract_structured_summary_text(metadata: dict[str, Any]) -> str:
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

    lines = []
    for key in (
        "premise",
        "character_engine",
        "authority_flip",
        "hook_pattern",
        "escalation_pattern",
        "payoff_pattern",
        "setting",
        "searchable_summary",
    ):
        value = raw_summary.get(key)
        if isinstance(value, str) and value.strip():
            lines.append(f"{key}: {value.strip()}")
    tone_tags = raw_summary.get("tone_tags")
    if isinstance(tone_tags, list) and tone_tags:
        lines.append(f"tone_tags: {', '.join(str(item).strip() for item in tone_tags if str(item).strip())}")
    return "\n".join(lines)


def extract_summary_field_text(summary_text: str, field_name: str) -> str:
    for line in summary_text.splitlines():
        normalized = line.strip()
        prefix = f"{field_name}:"
        if normalized.startswith(prefix):
            return normalized.split(":", 1)[1].strip()
    return ""


def score_text_alignment(query_value: str | None, candidate_value: str | None) -> float:
    query_text = augment_matching_text(query_value)
    candidate_text = augment_matching_text(candidate_value)
    if not query_text or not candidate_text:
        return 0.0

    query_norm = normalize_matching_text(query_text)
    candidate_norm = normalize_matching_text(candidate_text)
    if not query_norm or not candidate_norm:
        return 0.0

    base = 0.0
    if len(query_norm) >= 4 and (query_norm in candidate_norm or candidate_norm in query_norm):
        base = 1.0 if query_norm in candidate_norm else 0.85

    query_fragments = extract_matching_fragments(query_text)
    fragment_hits = 0
    fragment_candidates = 0
    for fragment in query_fragments:
        fragment_norm = normalize_matching_text(fragment)
        if len(fragment_norm) < 2:
            continue
        fragment_candidates += 1
        if fragment_norm in candidate_norm:
            fragment_hits += 1
    if fragment_candidates:
        base = max(base, 0.85 * (fragment_hits / fragment_candidates))
    return round(min(base, 1.0), 4)


def score_list_alignment(query_values: list[str], candidate_value: str | None) -> float:
    if not query_values or not candidate_value:
        return 0.0
    best = 0.0
    for value in query_values:
        best = max(best, score_text_alignment(value, candidate_value))
    return best


def build_query_facets(client: genai.Client, query_text: str, cohort: str | None, subcohort: str | None = None) -> dict[str, Any]:
    effective_cohort = subcohort or cohort
    model_name = os.getenv("BANKARA_GENERATION_MODEL") or QUERY_FACET_MODEL
    prompt = (
        "あなたは検索クエリをバンカラコメディ用の facet に分解するアナライザーです。\n"
        "短い JSON オブジェクトだけを返してください。\n"
        "query が曖昧でも、cohort が分かっていればその文法に沿って facet を補完してよいです。\n"
        "形式:\n"
        "{premise_focus: string, authority_focus: string, setting_cues: string[], tone_cues: string[], "
        "hook_cues: string[], escalation_cues: string[], payoff_cues: string[], novelty_cues: string[]}\n\n"
        f"query: {query_text}\n"
        f"cohort: {effective_cohort or ''}\n"
    )
    try:
        response = client.models.generate_content(
            model=model_name,
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=0,
                response_mime_type="application/json",
            ),
        )
        parsed = parse_generated_json_payload(response.text or "")
    except Exception:
        logger.warning("Facet analysis failed for query %r; using fallback", query_text, exc_info=True)
        parsed = {
            "premise_focus": query_text,
            "authority_focus": effective_cohort or "",
            "setting_cues": [],
            "tone_cues": [],
            "hook_cues": [],
            "escalation_cues": split_summary_field_values(query_text),
            "payoff_cues": [],
            "novelty_cues": [],
        }

    normalized: dict[str, Any] = {}
    for key in ("premise_focus", "authority_focus"):
        value = parsed.get(key)
        normalized[key] = value.strip() if isinstance(value, str) else ""
    for key in ("setting_cues", "tone_cues", "hook_cues", "escalation_cues", "payoff_cues", "novelty_cues"):
        value = parsed.get(key)
        normalized[key] = [str(item).strip() for item in value if str(item).strip()] if isinstance(value, list) else []
    normalized["raw_query"] = query_text.strip()
    normalized["target_lanes"] = [effective_cohort] if effective_cohort else infer_query_target_lanes(normalized)
    return normalized


def query_facets_active(query_facets: dict[str, Any]) -> bool:
    for value in query_facets.values():
        if isinstance(value, str) and value.strip():
            return True
        if isinstance(value, list) and any(str(item).strip() for item in value):
            return True
    return False


def match_segment_priority(match: dict[str, Any]) -> int:
    metadata = match.get("metadata", {}) or {}
    segment_kind = str(metadata.get("timeline_segment_kind") or "").strip().casefold()
    return SEGMENT_KIND_PRIORITY.get(segment_kind, 0)


def match_asset_group_key(match: dict[str, Any]) -> str:
    metadata = match.get("metadata", {}) or {}
    for key in ("asset_id", "relative_path", "source_path", "title"):
        value = metadata.get(key)
        if value:
            return str(value)
    return str(match.get("id") or "")


def diversify_matches_by_asset(matches: list[dict[str, Any]], top_k: int) -> list[dict[str, Any]]:
    if top_k <= 0:
        return []
    diverse: list[dict[str, Any]] = []
    leftovers: list[dict[str, Any]] = []
    seen_assets: set[str] = set()

    for match in matches:
        asset_key = match_asset_group_key(match)
        if asset_key and asset_key not in seen_assets:
            seen_assets.add(asset_key)
            diverse.append(match)
        else:
            leftovers.append(match)
        if len(diverse) >= top_k:
            return diverse[:top_k]

    for match in leftovers:
        diverse.append(match)
        if len(diverse) >= top_k:
            break
    return diverse[:top_k]


def score_query_facets_against_match(metadata: dict[str, Any], query_facets: dict[str, Any]) -> tuple[float, dict[str, float]]:
    summary_text = extract_structured_summary_text(metadata)
    title_text = str(metadata.get("title") or metadata.get("relative_path") or "")
    tags_text = ", ".join(str(tag).strip() for tag in (metadata.get("tags") or []) if str(tag).strip())
    classification_text = " ".join(
        str(metadata.get(key) or "").strip()
        for key in ("curation_character", "curation_format", "curation_cohort", "curation_subcohort")
        if str(metadata.get(key) or "").strip()
    )
    if not summary_text:
        return 0.0, {"title": 0.0}

    searchable_summary = extract_summary_field_text(summary_text, "searchable_summary")
    summary_overview = searchable_summary or summary_text
    query_canonical_tags = collect_canonical_tags_from_query_facets(query_facets)
    candidate_canonical_tags = set(
        infer_canonical_match_tags(" ".join(part for part in (title_text, tags_text, summary_overview, classification_text) if part))
    )
    query_tag_groups = group_canonical_tags(query_canonical_tags)
    candidate_tag_groups = group_canonical_tags(candidate_canonical_tags)
    canonical_tag_score = 0.0
    if query_canonical_tags:
        canonical_tag_score = round(len(query_canonical_tags & candidate_canonical_tags) / len(query_canonical_tags), 4)
    canonical_group_alignment = 0.0
    canonical_group_conflict = 0.0
    if query_tag_groups:
        aligned_groups = 0
        conflicting_groups = 0
        for group_name, query_group_tags in query_tag_groups.items():
            candidate_group_tags = candidate_tag_groups.get(group_name) or set()
            if not candidate_group_tags:
                continue
            if query_group_tags & candidate_group_tags:
                aligned_groups += 1
            else:
                conflicting_groups += 1
        canonical_group_alignment = round(aligned_groups / len(query_tag_groups), 4)
        canonical_group_conflict = round(conflicting_groups / len(query_tag_groups), 4)
    lane_alignment_score, lane_conflict_penalty, query_target_lanes, candidate_lane = score_target_lane_alignment(
        metadata,
        query_facets,
    )

    field_scores = {
        "title": max(
            score_text_alignment(query_facets.get("premise_focus"), title_text),
            score_text_alignment(query_facets.get("authority_focus"), title_text),
            score_list_alignment(query_facets.get("setting_cues") or [], title_text),
            score_list_alignment(query_facets.get("hook_cues") or [], title_text),
            score_list_alignment(query_facets.get("setting_cues") or [], tags_text),
            score_text_alignment(query_facets.get("premise_focus"), classification_text),
        ),
        "premise": max(
            score_text_alignment(
                query_facets.get("premise_focus"),
                extract_summary_field_text(summary_text, "premise") or searchable_summary,
            ),
            score_text_alignment(query_facets.get("premise_focus"), summary_overview),
            score_text_alignment(query_facets.get("premise_focus"), tags_text),
            score_text_alignment(query_facets.get("premise_focus"), classification_text),
        ),
        "authority": max(
            score_text_alignment(query_facets.get("authority_focus"), extract_summary_field_text(summary_text, "authority_flip")),
            score_text_alignment(query_facets.get("authority_focus"), extract_summary_field_text(summary_text, "character_engine")),
            score_text_alignment(query_facets.get("authority_focus"), summary_overview),
            score_text_alignment(query_facets.get("authority_focus"), tags_text),
            score_text_alignment(query_facets.get("authority_focus"), classification_text),
        ),
        "setting": max(
            score_list_alignment(query_facets.get("setting_cues") or [], extract_summary_field_text(summary_text, "setting")),
            score_list_alignment(query_facets.get("setting_cues") or [], summary_overview),
            score_list_alignment(query_facets.get("setting_cues") or [], tags_text),
            score_list_alignment(query_facets.get("setting_cues") or [], classification_text),
        ),
        "tone": max(
            score_list_alignment(query_facets.get("tone_cues") or [], extract_summary_field_text(summary_text, "tone_tags")),
            score_list_alignment(query_facets.get("tone_cues") or [], summary_overview),
        ),
        "hook": max(
            score_list_alignment(query_facets.get("hook_cues") or [], extract_summary_field_text(summary_text, "hook_pattern")),
            score_list_alignment(query_facets.get("hook_cues") or [], summary_overview),
            score_list_alignment(query_facets.get("hook_cues") or [], tags_text),
        ),
        "escalation": score_list_alignment(
            query_facets.get("escalation_cues") or [],
            extract_summary_field_text(summary_text, "escalation_pattern") or summary_overview,
        ),
        "payoff": score_list_alignment(
            query_facets.get("payoff_cues") or [],
            extract_summary_field_text(summary_text, "payoff_pattern") or summary_overview,
        ),
        "canonical_tags": canonical_tag_score,
        "canonical_groups": canonical_group_alignment,
        "canonical_conflict_penalty": canonical_group_conflict,
        "lane_alignment": lane_alignment_score,
        "lane_conflict_penalty": lane_conflict_penalty,
    }
    structural_keys = ("title", "premise", "authority", "setting", "hook", "escalation", "payoff")
    if max(field_scores[key] for key in structural_keys) <= 0 and field_scores["tone"] > 0:
        field_scores["tone"] = round(field_scores["tone"] * 0.35, 4)
    weights = {
        "title": 0.12,
        "premise": 0.18,
        "authority": 0.16,
        "setting": 0.12,
        "tone": 0.08,
        "hook": 0.10,
        "escalation": 0.16,
        "payoff": 0.08,
        "canonical_tags": 0.14,
        "canonical_groups": 0.12,
        "lane_alignment": 0.18,
    }
    weighted_total = 0.0
    applied_weight = 0.0
    for key, score in field_scores.items():
        if key in {"canonical_conflict_penalty", "lane_conflict_penalty"}:
            continue
        weight = weights[key]
        if score > 0:
            weighted_total += score * weight
            applied_weight += weight
    if applied_weight == 0:
        return 0.0, field_scores
    raw_score = weighted_total / applied_weight
    if canonical_group_conflict > 0:
        raw_score = max(raw_score - (0.24 * canonical_group_conflict), 0.0)
    if lane_conflict_penalty > 0 and query_target_lanes and candidate_lane:
        raw_score = max(raw_score - (0.22 * lane_conflict_penalty), 0.0)
    return round(raw_score, 4), field_scores


def search_similar(
    client: genai.Client,
    index: Any,
    namespace: str,
    query_text: str,
    top_k: int = 3,
    media_type: str | None = None,
    embedding_kind: str | None = None,
    selection_status: str | None = None,
    cohort: str | None = None,
    subcohort: str | None = None,
    rerank_by_feedback: bool = False,
    feedback_weight: float = 0.15,
    facet_weight: float = 0.18,
    query_facets: dict[str, Any] | None = None,
    diversify_by_asset: bool = True,
    candidate_k: int | None = None,
    min_feedback_score: float | None = None,
    cross_encoder_rerank: bool = False,
    cross_encoder_top_k: int = 12,
) -> list[dict[str, Any]]:
    query_vector = embed_text(client, query_text, task_type="RETRIEVAL_QUERY")
    resolved_query_facets = query_facets or build_query_facets(client, query_text, cohort, subcohort=subcohort)
    facet_rerank_enabled = query_facets_active(resolved_query_facets)
    filter_payload: dict[str, Any] | None = None
    if media_type:
        filter_payload = {"media_type": {"$eq": media_type}}
    if embedding_kind:
        filter_payload = {**(filter_payload or {}), "embedding_kind": {"$eq": embedding_kind}}
    if selection_status:
        filter_payload = {**(filter_payload or {}), "selection_status": {"$eq": selection_status}}
    if cohort:
        filter_payload = {**(filter_payload or {}), "curation_cohort": {"$eq": cohort}}
    if subcohort:
        filter_payload = {**(filter_payload or {}), "curation_subcohort": {"$eq": subcohort}}

    query_top_k = candidate_k or top_k
    if rerank_by_feedback or min_feedback_score is not None or facet_rerank_enabled:
        query_top_k = max(query_top_k, top_k * 5)
    if cross_encoder_rerank:
        query_top_k = max(query_top_k, cross_encoder_top_k, top_k * 5)

    query_kwargs = {
        "namespace": namespace,
        "vector": query_vector,
        "top_k": query_top_k,
        "include_metadata": True,
    }
    if filter_payload is not None:
        query_kwargs["filter"] = filter_payload
    query_response = with_transient_retries(
        action_label="Pinecone query",
        operation=lambda: index.query(**query_kwargs),
    )
    matches = normalize_search_matches(
        query_response,
        feedback_weight=feedback_weight,
        facet_weight=facet_weight,
        query_facets=resolved_query_facets,
    )
    if min_feedback_score is not None:
        matches = [
            match
            for match in matches
            if match["feedback_score"] is not None and match["feedback_score"] >= min_feedback_score
        ]
    if cross_encoder_rerank and matches:
        matches = rerank_matches_with_client(
            client,
            query_text,
            matches,
            top_k=min(max(1, cross_encoder_top_k), len(matches)),
        )
    if rerank_by_feedback or facet_rerank_enabled:
        matches.sort(
            key=lambda match: (
                match.get("reranked_combined_score", match["combined_score"]),
                match.get("cross_encoder_score", 0.0),
                match["combined_score"],
                match["semantic_score"],
                match_segment_priority(match),
            ),
            reverse=True,
        )
    elif cross_encoder_rerank:
        matches.sort(
            key=lambda match: (
                match.get("reranked_combined_score", match["combined_score"]),
                match.get("cross_encoder_score", 0.0),
                match["combined_score"],
                match["semantic_score"],
                match_segment_priority(match),
            ),
            reverse=True,
        )
    else:
        matches.sort(
            key=lambda match: (match["semantic_score"], match_segment_priority(match)),
            reverse=True,
        )
    if diversify_by_asset and (embedding_kind == "timeline_segment" or any((m.get("metadata") or {}).get("asset_id") for m in matches)):
        return diversify_matches_by_asset(matches, top_k=top_k)
    return matches[:top_k]


def normalize_search_matches(
    query_response: Any,
    feedback_weight: float,
    facet_weight: float,
    query_facets: dict[str, Any],
) -> list[dict[str, Any]]:
    normalized = []
    raw_matches = _get_attr(query_response, "matches", []) or []
    for match in raw_matches:
        metadata = normalize_match_metadata(_get_attr(match, "metadata", {}) or {})
        semantic_score = coerce_float(_get_attr(match, "score", 0.0)) or 0.0
        feedback_score = extract_match_feedback_score(metadata)
        facet_score, facet_breakdown = score_query_facets_against_match(metadata, query_facets)
        facet_conflict_penalty = coerce_float(facet_breakdown.get("canonical_conflict_penalty")) or 0.0
        lane_conflict_penalty = coerce_float(facet_breakdown.get("lane_conflict_penalty")) or 0.0
        combined_score = (
            semantic_score
            + (feedback_weight * feedback_score if feedback_score is not None else 0.0)
            + (facet_weight * facet_score if facet_score is not None else 0.0)
            - (FACET_CONFLICT_COMBINED_WEIGHT * facet_conflict_penalty)
            - (LANE_CONFLICT_COMBINED_WEIGHT * lane_conflict_penalty)
        )
        normalized.append(
            {
                "id": _get_attr(match, "id", ""),
                "semantic_score": semantic_score,
                "feedback_score": feedback_score,
                "facet_score": facet_score,
                "facet_breakdown": facet_breakdown,
                "facet_conflict_penalty": facet_conflict_penalty,
                "lane_conflict_penalty": lane_conflict_penalty,
                "combined_score": combined_score,
                "metadata": metadata,
            }
        )
    return normalized


def build_search_payload(
    query_text: str,
    namespace: str,
    media_type: str | None,
    embedding_kind: str | None,
    selection_status: str | None,
    cohort: str | None,
    subcohort: str | None,
    rerank_by_feedback: bool,
    feedback_weight: float,
    facet_weight: float,
    candidate_k: int | None,
    min_feedback_score: float | None,
    cross_encoder_rerank: bool,
    cross_encoder_top_k: int,
    query_facets: dict[str, Any],
    matches: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "query": query_text,
        "namespace": namespace,
        "media_type": media_type,
        "embedding_kind": embedding_kind,
        "selection_status": selection_status,
        "cohort": cohort,
        "subcohort": subcohort,
        "rerank_by_feedback": rerank_by_feedback,
        "feedback_weight": feedback_weight,
        "facet_weight": facet_weight,
        "candidate_k": candidate_k,
        "min_feedback_score": min_feedback_score,
        "cross_encoder_rerank": cross_encoder_rerank,
        "cross_encoder_top_k": cross_encoder_top_k,
        "query_facets": query_facets,
        "matches": matches,
    }


def write_search_payload(output_path: Path, payload: dict[str, Any]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def normalize_match_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(metadata)
    for key in ("feedback_summary", "asset_feedback_summary", "segment_feedback_summary"):
        if key in normalized:
            normalized[key] = parse_feedback_summary_value(normalized.get(key))
    return normalized


def extract_match_feedback_score(metadata: dict[str, Any]) -> float | None:
    direct_score = coerce_float(metadata.get("feedback_score_v1"))
    if direct_score is not None:
        return direct_score

    for key in ("segment_feedback_summary", "feedback_summary", "asset_feedback_summary"):
        summary = parse_feedback_summary_value(metadata.get(key))
        score = coerce_float(summary.get("feedback_score_v1"))
        if score is not None:
            return score
    return None


def print_matches(matches: list[dict[str, Any]]) -> None:
    if not matches:
        print("No matches found.")
        return

    print("\nTop matches:")
    for rank, match in enumerate(matches, start=1):
        metadata = match.get("metadata", {}) or {}
        title = metadata.get("title", "-")
        media_type = metadata.get("media_type", "-")
        source_path = metadata.get("source_path", "-")
        relative_path = metadata.get("relative_path")
        chunk_index = metadata.get("chunk_index")
        chunk_count = metadata.get("chunk_count")
        time_range = format_match_time_range(metadata)
        feedback_score = match.get("feedback_score")
        facet_score = match.get("facet_score")
        combined_score = match.get("combined_score", match.get("semantic_score", 0.0))
        cross_encoder_score = match.get("cross_encoder_score")
        reranked_combined_score = match.get("reranked_combined_score")
        print(
            f"{rank}. semantic={match.get('semantic_score', 0.0):.4f} "
            f"combined={combined_score:.4f}  id={match.get('id', '')}"
        )
        print(f"   title={title}")
        print(f"   media_type={media_type}")
        if metadata.get("embedding_kind"):
            print(f"   embedding_kind={metadata['embedding_kind']}")
        if metadata.get("curation_subcohort"):
            print(f"   subcohort={metadata['curation_subcohort']}")
        if feedback_score is not None:
            print(f"   feedback_score_v1={feedback_score:.4f}")
        if facet_score is not None:
            print(f"   facet_score={facet_score:.4f}")
        if cross_encoder_score is not None:
            print(f"   cross_encoder_score={float(cross_encoder_score):.4f}")
        if reranked_combined_score is not None:
            print(f"   reranked_combined_score={float(reranked_combined_score):.4f}")
        if metadata.get("timeline_label") or metadata.get("timeline_segment_kind"):
            print(
                "   timeline="
                f"{metadata.get('timeline_segment_kind', '-')}"
                f" label={metadata.get('timeline_label', '-')}"
            )
        if relative_path:
            print(f"   relative_path={relative_path}")
        print(f"   source_path={source_path}")
        if chunk_index is not None and chunk_count is not None:
            print(f"   chunk={int(chunk_index) + 1}/{chunk_count}")
        if time_range:
            print(f"   time_range={time_range}")
        if metadata.get("tags"):
            print(f"   tags={metadata['tags']}")
        if metadata.get("notes"):
            print(f"   notes={metadata['notes']}")
        facet_breakdown = match.get("facet_breakdown") or {}
        if facet_breakdown:
            scored_parts = [f"{key}={value:.2f}" for key, value in facet_breakdown.items() if value and value > 0]
            if scored_parts:
                print(f"   facet_breakdown={' / '.join(scored_parts)}")
        if match.get("cross_encoder_reason"):
            print(f"   cross_encoder_reason={match['cross_encoder_reason']}")


def format_match_time_range(metadata: dict[str, Any]) -> str | None:
    start_seconds = metadata.get("chunk_start_seconds")
    end_seconds = metadata.get("chunk_end_seconds")
    if start_seconds is None or end_seconds is None:
        return None
    try:
        return f"{format_seconds(float(start_seconds))} - {format_seconds(float(end_seconds))}"
    except (TypeError, ValueError):
        return None


def format_seconds(value: float) -> str:
    total_milliseconds = int(round(value * 1000))
    total_seconds, milliseconds = divmod(total_milliseconds, 1000)
    minutes, seconds = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}.{milliseconds:03d}"
    return f"{minutes:02d}:{seconds:02d}.{milliseconds:03d}"
