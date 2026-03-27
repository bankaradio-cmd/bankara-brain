"""Brief assembly — build query briefs from Brain data."""
from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any, Optional

from bankara_brain import (
    BankaraBrain,
    derive_novelty_constraints,
    effective_cohort_label,
    extract_structured_summary_text,
    format_seconds_hms,
    now_utc,
    render_cohort_rules_text,
    render_novelty_constraints_text,
    resolve_cohort_rules,
)


# ── Brief payload helpers ────────────────────────────────────────────────────

def load_semantic_search_results(
    search_results_path: Optional[Path],
    limit: int,
) -> list[dict[str, Any]]:
    """Load and normalize semantic search results from a JSON file."""
    if not search_results_path:
        return []
    if not search_results_path.exists():
        raise FileNotFoundError(f"Search results file not found: {search_results_path}")

    payload = json.loads(search_results_path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        matches = payload.get("matches", [])
    elif isinstance(payload, list):
        matches = payload
    else:
        raise ValueError("Search results JSON must be an array or {\"matches\": [...]}")

    normalized = []
    for match in matches[:limit]:
        if not isinstance(match, dict):
            continue
        metadata = match.get("metadata") or {}
        normalized.append(
            {
                "id": match.get("id"),
                "semantic_score": match.get("semantic_score"),
                "feedback_score": match.get("feedback_score"),
                "facet_score": match.get("facet_score"),
                "combined_score": match.get("combined_score"),
                "title": metadata.get("title"),
                "media_type": metadata.get("media_type"),
                "embedding_kind": metadata.get("embedding_kind"),
                "relative_path": metadata.get("relative_path"),
                "cohort": metadata.get("curation_cohort") or "",
                "subcohort": metadata.get("curation_subcohort") or "",
                "timeline_label": metadata.get("timeline_label"),
                "timeline_segment_kind": metadata.get("timeline_segment_kind"),
                "time_range": format_brief_match_time_range(metadata),
                "notes": metadata.get("notes") or "",
                "summary": extract_structured_summary_text(metadata, compact=True),
            }
        )
    return normalized


def format_brief_match_time_range(metadata: dict[str, Any]) -> str:
    """Format a time range from match metadata."""
    start_seconds = metadata.get("chunk_start_seconds")
    end_seconds = metadata.get("chunk_end_seconds")
    if start_seconds is None or end_seconds is None:
        return ""
    try:
        return f"{format_seconds_hms(float(start_seconds))}-{format_seconds_hms(float(end_seconds))}"
    except (TypeError, ValueError):
        return ""


def parse_markdown_brief_payload(raw: str) -> dict[str, Any]:
    """Parse a Markdown-formatted brief back into a payload dict."""
    query = ""
    generated_at = now_utc().isoformat()
    prompt_scaffold = raw
    lines = raw.splitlines()
    for line in lines:
        if line.startswith("- Query:"):
            query = line.split(":", 1)[1].strip()
        elif line.startswith("- Generated At:"):
            generated_at = line.split(":", 1)[1].strip()

    marker = "## Prompt Scaffold"
    if marker in raw:
        prompt_scaffold = raw.split(marker, 1)[1].strip()

    return {
        "query": query,
        "generated_at": generated_at,
        "raw_brief_markdown": raw,
        "semantic_matches": [],
        "recommended_timeline_patterns": [],
        "recommended_asset_patterns": [],
        "cohort_rules": {},
        "cohort_rules_text": "",
        "novelty_constraints": {},
        "novelty_constraints_text": "",
        "prompt_scaffold": prompt_scaffold,
    }


def load_brief_payload(brief_path: Path) -> dict[str, Any]:
    """Load a brief from disk (JSON or Markdown)."""
    if not brief_path.exists():
        raise FileNotFoundError(f"Brief file not found: {brief_path}")
    raw = brief_path.read_text(encoding="utf-8")
    try:
        loaded = json.loads(raw)
    except json.JSONDecodeError:
        return parse_markdown_brief_payload(raw)
    if not isinstance(loaded, dict):
        raise ValueError("Brief JSON must be an object.")
    return loaded


# ── Prompt scaffold ──────────────────────────────────────────────────────────

def build_query_prompt_scaffold(
    query: str,
    semantic_matches: list[dict[str, Any]],
    timeline_patterns: list[dict[str, Any]],
    asset_patterns: list[dict[str, Any]],
    cohort_rules: dict[str, Any],
    novelty_constraints: dict[str, Any],
) -> str:
    """Build a prompt scaffold from brief components."""
    lines = [
        f"企画テーマ: {query}",
        "",
        "使うべき要素:",
    ]
    for pattern in timeline_patterns[:3]:
        label = pattern.get("segment_label") or pattern.get("segment_kind") or "segment"
        note = pattern.get("notes") or pattern.get("transcript") or pattern.get("asset_notes") or ""
        lines.append(
            f"- {label} / {pattern.get('time_range') or '-'} / score={pattern.get('score_value', 0.0):.3f} / {note}"
        )
    for asset in asset_patterns[:2]:
        asset_context = asset.get("asset_summary_text") or asset.get("asset_notes") or asset.get("asset_transcript_excerpt") or ""
        lines.append(
            f"- asset reference: {asset.get('asset_title') or asset.get('asset_relative_path')}"
            f" / score={asset.get('score_value', 0.0):.3f}"
            f" / {asset_context}"
        )

    if semantic_matches:
        lines.append("")
        lines.append("近い過去データ:")
        for match in semantic_matches[:3]:
            descriptor = match.get("title") or match.get("relative_path") or match.get("id")
            notes = match.get("summary") or match.get("notes") or ""
            lines.append(
                f"- {descriptor} / semantic={float(match.get('semantic_score') or 0.0):.3f}"
                f" / feedback={float(match.get('feedback_score') or 0.0):.3f}"
                f" / {notes}"
            )

    lines.extend(
        [
            "",
            "生成ルール:",
            "- 導入は最初の数秒でフックを置く",
            "- テンポを維持し、setup を長引かせない",
            "- 過去の高スコア要素を流用しつつ、表現は焼き直しにしない",
        ]
    )
    cohort_rules_text = render_cohort_rules_text(cohort_rules)
    if cohort_rules_text:
        lines.extend(["", "cohort 固定ルール:", cohort_rules_text])
    novelty_text = render_novelty_constraints_text(novelty_constraints)
    if novelty_text:
        lines.extend(["", "近すぎる既存回の回避ルール:", novelty_text])
    return "\n".join(lines)


# ── Brief assembly ───────────────────────────────────────────────────────────

def assemble_query_brief_payload(
    brain: BankaraBrain,
    query: str,
    search_results_path: Optional[Path],
    timeline_limit: int,
    asset_limit: int,
    semantic_limit: int,
    media_type: Optional[str],
    score_name: str,
    min_score: Optional[float],
    selection_status: Optional[str] = None,
    cohort: Optional[str] = None,
    subcohort: Optional[str] = None,
) -> dict[str, Any]:
    """Assemble a complete brief payload from Brain data."""
    effective_cohort = effective_cohort_label(cohort, subcohort)
    cohort_rules = resolve_cohort_rules(cohort, subcohort=subcohort)
    timeline_patterns = brain.get_top_feedback_patterns(
        scope_type="timeline_segment",
        score_name=score_name,
        media_type=media_type,
        limit=timeline_limit,
        min_score=min_score,
        selection_status=selection_status,
        cohort=cohort,
        subcohort=subcohort,
    )
    asset_patterns = brain.get_top_feedback_patterns(
        scope_type="asset",
        score_name=score_name,
        media_type=media_type,
        limit=asset_limit,
        min_score=min_score,
        selection_status=selection_status,
        cohort=cohort,
        subcohort=subcohort,
    )

    semantic_matches = load_semantic_search_results(search_results_path, semantic_limit)
    novelty_constraints = derive_novelty_constraints(
        query=query,
        semantic_matches=semantic_matches,
        asset_patterns=asset_patterns,
        cohort=effective_cohort,
    )
    return {
        "query": query,
        "generated_at": now_utc().isoformat(),
        "score_name": score_name,
        "media_type": media_type,
        "selection_status": selection_status,
        "cohort": cohort or "",
        "subcohort": subcohort or "",
        "effective_cohort": effective_cohort,
        "cohort_rules": cohort_rules,
        "cohort_rules_text": render_cohort_rules_text(cohort_rules),
        "novelty_constraints": novelty_constraints,
        "novelty_constraints_text": render_novelty_constraints_text(novelty_constraints),
        "semantic_matches": semantic_matches,
        "recommended_timeline_patterns": timeline_patterns,
        "recommended_asset_patterns": asset_patterns,
        "prompt_scaffold": build_query_prompt_scaffold(
            query=query,
            semantic_matches=semantic_matches,
            timeline_patterns=timeline_patterns,
            asset_patterns=asset_patterns,
            cohort_rules=cohort_rules,
            novelty_constraints=novelty_constraints,
        ),
    }


# ── Markdown rendering ──────────────────────────────────────────────────────

def render_query_brief_markdown(payload: dict[str, Any]) -> str:
    """Render a brief payload as Markdown."""
    lines = [
        "# Query Brief",
        "",
        f"- Query: {payload['query']}",
        f"- Generated At: {payload['generated_at']}",
    ]
    if payload.get("media_type"):
        lines.append(f"- Media Type: {payload['media_type']}")
    if payload.get("cohort"):
        lines.append(f"- Cohort: {payload['cohort']}")
    if payload.get("subcohort"):
        lines.append(f"- Subcohort: {payload['subcohort']}")
    if payload.get("effective_cohort"):
        lines.append(f"- Effective Cohort: {payload['effective_cohort']}")

    lines.extend(["", "## Semantic Matches", ""])
    semantic_matches = payload.get("semantic_matches", [])
    if semantic_matches:
        for index, match in enumerate(semantic_matches, start=1):
            lines.append(
                f"{index}. {match.get('title') or match.get('relative_path') or match.get('id')} "
                f"(semantic={float(match.get('semantic_score') or 0.0):.3f}, "
                f"feedback={float(match.get('feedback_score') or 0.0):.3f}, "
                f"facet={float(match.get('facet_score') or 0.0):.3f}, "
                f"combined={float(match.get('combined_score') or 0.0):.3f})"
            )
            details = []
            if match.get("embedding_kind"):
                details.append(f"kind={match['embedding_kind']}")
            if match.get("subcohort"):
                details.append(f"subcohort={match['subcohort']}")
            if match.get("timeline_segment_kind") or match.get("timeline_label"):
                details.append(
                    f"timeline={match.get('timeline_segment_kind') or '-'}:{match.get('timeline_label') or '-'}"
                )
            if match.get("time_range"):
                details.append(f"time={match['time_range']}")
            if details:
                lines.append(f"   {' / '.join(details)}")
            if match.get("notes"):
                lines.append(f"   notes={match['notes']}")
            if match.get("summary"):
                lines.append(f"   summary={match['summary']}")
    else:
        lines.append("No semantic matches attached.")

    lines.extend(["", "## Winning Timeline Patterns", ""])
    timeline_patterns = payload.get("recommended_timeline_patterns", [])
    if timeline_patterns:
        for index, pattern in enumerate(timeline_patterns, start=1):
            lines.append(
                f"{index}. {pattern.get('segment_label') or pattern.get('segment_kind') or 'segment'} "
                f"(score={pattern.get('score_value', 0.0):.3f}, "
                f"time={pattern.get('time_range') or '-'})"
            )
            if pattern.get("transcript"):
                lines.append(f"   transcript={pattern['transcript']}")
            if pattern.get("notes"):
                lines.append(f"   notes={pattern['notes']}")
    else:
        lines.append("No timeline patterns found.")

    lines.extend(["", "## Winning Assets", ""])
    asset_patterns = payload.get("recommended_asset_patterns", [])
    if asset_patterns:
        for index, pattern in enumerate(asset_patterns, start=1):
            lines.append(
                f"{index}. {pattern.get('asset_title') or pattern.get('asset_relative_path') or pattern.get('asset_id')} "
                f"(score={pattern.get('score_value', 0.0):.3f})"
            )
            if pattern.get("asset_transcript_excerpt"):
                lines.append(f"   transcript_excerpt={pattern['asset_transcript_excerpt']}")
            if pattern.get("asset_notes"):
                lines.append(f"   notes={pattern['asset_notes']}")
            if pattern.get("asset_summary_text"):
                lines.append(f"   summary={pattern['asset_summary_text']}")
    else:
        lines.append("No asset patterns found.")

    lines.extend(["", "## Cohort Rules", ""])
    if payload.get("cohort_rules_text"):
        lines.append(payload["cohort_rules_text"])
    else:
        lines.append("No cohort rules loaded.")

    lines.extend(["", "## Novelty Guardrails", ""])
    if payload.get("novelty_constraints_text"):
        lines.append(payload["novelty_constraints_text"])
    else:
        lines.append("No novelty constraints derived.")

    lines.extend(["", "## Prompt Scaffold", "", payload["prompt_scaffold"], ""])
    return "\n".join(lines)


# ── CLI entry points ─────────────────────────────────────────────────────────

def build_query_brief(
    brain: BankaraBrain,
    query: str,
    output_path: Optional[Path],
    output_format: str,
    search_results_path: Optional[Path],
    timeline_limit: int,
    asset_limit: int,
    semantic_limit: int,
    media_type: Optional[str],
    score_name: str,
    min_score: Optional[float],
    selection_status: Optional[str] = None,
    cohort: Optional[str] = None,
    subcohort: Optional[str] = None,
) -> None:
    """Assemble and write a query brief (CLI entry point)."""
    brief_payload = assemble_query_brief_payload(
        brain=brain,
        query=query,
        search_results_path=search_results_path,
        timeline_limit=timeline_limit,
        asset_limit=asset_limit,
        semantic_limit=semantic_limit,
        media_type=media_type,
        score_name=score_name,
        min_score=min_score,
        selection_status=selection_status,
        cohort=cohort,
        subcohort=subcohort,
    )

    if output_format == "json":
        rendered = json.dumps(brief_payload, ensure_ascii=False, indent=2)
    else:
        rendered = render_query_brief_markdown(brief_payload)

    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered, encoding="utf-8")
        print(f"Wrote query brief: {output_path}")
        return

    print(rendered)


def build_live_query_brief(
    brain: BankaraBrain,
    query: str,
    output_path: Optional[Path],
    output_format: str,
    timeline_limit: int,
    asset_limit: int,
    semantic_limit: int,
    media_type: Optional[str],
    score_name: str,
    min_score: Optional[float],
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
    subcohort: Optional[str] = None,
) -> None:
    """Run semantic search then build a brief (CLI entry point)."""
    with tempfile.NamedTemporaryFile(prefix="bankara_search_", suffix=".json", delete=False) as handle:
        search_results_path = Path(handle.name)

    try:
        brain.run_semantic_search(
            query=query,
            output_path=search_results_path,
            semantic_limit=semantic_limit,
            media_type=media_type,
            namespace=namespace,
            embedding_kind=embedding_kind,
            rerank_feedback=rerank_feedback,
            feedback_weight=feedback_weight,
            candidate_k=candidate_k,
            min_feedback_score=min_feedback_score,
            cross_encoder_rerank=cross_encoder_rerank,
            cross_encoder_top_k=cross_encoder_top_k,
            selection_status=selection_status,
            cohort=cohort,
            subcohort=subcohort,
        )
        build_query_brief(
            brain=brain,
            query=query,
            output_path=output_path,
            output_format=output_format,
            search_results_path=search_results_path,
            timeline_limit=timeline_limit,
            asset_limit=asset_limit,
            semantic_limit=semantic_limit,
            media_type=media_type,
            score_name=score_name,
            min_score=min_score,
            selection_status=selection_status,
            cohort=cohort,
            subcohort=subcohort,
        )
    finally:
        search_results_path.unlink(missing_ok=True)
