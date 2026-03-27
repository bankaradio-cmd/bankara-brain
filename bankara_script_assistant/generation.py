"""Draft and idea-batch generation via Gemini."""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv

from bankara_brain import BankaraBrain, normalize_match_text, now_utc

from bankara_script_assistant.brief import (
    assemble_query_brief_payload,
    load_brief_payload,
    render_query_brief_markdown,
)
from bankara_script_assistant.gemini_helpers import (
    generate_content_text,
    parse_generated_json,
    parse_or_repair_generated_json,
)

# Default model (shared with CLI).
DEFAULT_GENERATION_MODEL = "gemini-2.5-flash"


# ── Prompt templates ─────────────────────────────────────────────────────────

def render_draft_generation_prompt(
    brief_payload: dict[str, Any],
    draft_kind: str,
    output_format: str,
    style_notes: str,
) -> str:
    """Render the Gemini prompt for draft generation."""
    brief_json = json.dumps(brief_payload, ensure_ascii=False, indent=2)
    style_section = style_notes.strip() if style_notes.strip() else "なし"
    cohort = brief_payload.get("cohort") or "指定なし"
    subcohort = brief_payload.get("subcohort") or "なし"
    effective_cohort = brief_payload.get("effective_cohort") or cohort
    cohort_rules_text = brief_payload.get("cohort_rules_text") or "なし"
    novelty_constraints_text = brief_payload.get("novelty_constraints_text") or "なし"
    if draft_kind == "script":
        deliverable = (
            "短い動画台本ドラフト。導入フック、展開、オチ、セリフ断片、編集メモを含める。"
        )
    else:
        deliverable = (
            "企画ドラフト。タイトル案、サムネ文言案、導入フック、ビート案、編集メモを含める。"
        )

    if output_format == "json":
        output_spec = (
            "次の JSON オブジェクトだけを返す: "
            "{title_candidates: string[], thumbnail_lines: string[], concept_summary: string, "
            "hook: string, beat_sheet: [{label: string, goal: string, sample: string}], "
            "editing_notes: string[], risk_checks: string[]}"
        )
    else:
        output_spec = (
            "Markdown で返す。見出しは `Title Candidates`, `Thumbnail Lines`, `Concept Summary`, "
            "`Hook`, `Beat Sheet`, `Editing Notes`, `Risk Checks` を使う。"
        )

    return (
        "あなたはバンカラブレイン専属の企画・脚本エンジンです。\n"
        "以下の brief を読み、過去の高スコア要素を参考にしながら、焼き直しではない新規ドラフトを作ってください。\n"
        "勢いのある導入、短い setup、明確な payoff を優先してください。\n"
        "cohort 固定ルールがある場合は、必ず守ってください。\n"
        "subcohort が指定されている場合は、その細いレーンから外れないでください。\n"
        "近すぎる既存回の回避ルールがある場合は、タイトル・役職・店種・舞台を重ねないでください。\n"
        "コンプラや危険行為は助長しないこと。\n"
        f"追加スタイル指示: {style_section}\n"
        f"対象 cohort: {cohort}\n"
        f"対象 subcohort: {subcohort}\n"
        f"有効レーン: {effective_cohort}\n"
        f"cohort 固定ルール:\n{cohort_rules_text}\n"
        f"近すぎる既存回の回避ルール:\n{novelty_constraints_text}\n"
        f"成果物: {deliverable}\n"
        f"出力形式: {output_spec}\n\n"
        "Brief JSON:\n"
        f"{brief_json}\n"
    )


def render_batch_idea_generation_prompt(
    brief_payload: dict[str, Any],
    count: int,
    style_notes: str,
) -> str:
    """Render the Gemini prompt for batch idea generation."""
    brief_json = json.dumps(brief_payload, ensure_ascii=False, indent=2)
    style_section = style_notes.strip() if style_notes.strip() else "なし"
    cohort = brief_payload.get("cohort") or "指定なし"
    subcohort = brief_payload.get("subcohort") or "なし"
    effective_cohort = brief_payload.get("effective_cohort") or cohort
    cohort_rules_text = brief_payload.get("cohort_rules_text") or "なし"
    novelty_constraints_text = brief_payload.get("novelty_constraints_text") or "なし"
    return (
        "あなたはバンカラブレイン専属の企画量産エンジンです。\n"
        "brief を読み、同じノリを保ちつつ焼き直しではない新規企画を複数本まとめて作ってください。\n"
        "各案は設定・職業・対立・オチを明確に変え、似すぎた案を避けてください。\n"
        "cohort が指定されている場合は、その主人公レーンを守ってください。\n"
        "subcohort が指定されている場合は、その細いレーンから絶対に外れないでください。\n"
        "cohort 固定ルールがある場合は、必ず守ってください。\n"
        "近すぎる既存回の回避ルールがある場合は、タイトル・役職・店種・舞台を重ねないでください。\n"
        "コンプラや危険行為は助長しないこと。\n"
        f"追加スタイル指示: {style_section}\n"
        f"対象 cohort: {cohort}\n"
        f"対象 subcohort: {subcohort}\n"
        f"有効レーン: {effective_cohort}\n"
        f"cohort 固定ルール:\n{cohort_rules_text}\n"
        f"近すぎる既存回の回避ルール:\n{novelty_constraints_text}\n"
        f"{count} 本の案を返してください。\n"
        "JSON オブジェクトだけを返すこと。\n"
        "形式: "
        "{ideas: [{rank: number, title: string, thumbnail_line: string, concept_summary: string, "
        "hook: string, beat_outline: string[], editing_notes: string[], risk_checks: string[], "
        "source_patterns: string[]}]} \n\n"
        "Brief JSON:\n"
        f"{brief_json}\n"
    )


def render_batch_idea_repair_prompt(
    brief_payload: dict[str, Any],
    current_ideas: list[dict[str, Any]],
    quality_report: dict[str, Any],
    count: int,
    style_notes: str,
) -> str:
    """Render the Gemini prompt for repairing violated ideas."""
    brief_json = json.dumps(brief_payload, ensure_ascii=False, indent=2)
    ideas_json = json.dumps({"ideas": current_ideas}, ensure_ascii=False, indent=2)
    quality_json = json.dumps(quality_report, ensure_ascii=False, indent=2)
    style_section = style_notes.strip() if style_notes.strip() else "なし"
    cohort = brief_payload.get("cohort") or "指定なし"
    subcohort = brief_payload.get("subcohort") or "なし"
    effective_cohort = brief_payload.get("effective_cohort") or cohort
    cohort_rules_text = brief_payload.get("cohort_rules_text") or "なし"
    novelty_constraints_text = brief_payload.get("novelty_constraints_text") or "なし"
    return (
        "あなたはバンカラブレイン専属の企画修正エンジンです。\n"
        "既存案のうち、近すぎる既存回の回避ルールに違反している案を必ず修正してください。\n"
        "特に exact_title_hits と signature_hits が出ている案は、職業・店種・舞台・権力の見え方を変えてください。\n"
        "構造は活かしてよいですが、タイトル・役職・店種・舞台は重ねないでください。\n"
        "subcohort が指定されている場合は、その細いレーンから外れる修正をしてはいけません。\n"
        f"追加スタイル指示: {style_section}\n"
        f"対象 cohort: {cohort}\n"
        f"対象 subcohort: {subcohort}\n"
        f"有効レーン: {effective_cohort}\n"
        f"cohort 固定ルール:\n{cohort_rules_text}\n"
        f"近すぎる既存回の回避ルール:\n{novelty_constraints_text}\n"
        f"{count} 本の案を同じ JSON 形式で返してください。\n"
        "JSON オブジェクトだけを返すこと。\n\n"
        "Brief JSON:\n"
        f"{brief_json}\n\n"
        "Current Ideas JSON:\n"
        f"{ideas_json}\n\n"
        "Novelty Quality Report JSON:\n"
        f"{quality_json}\n"
    )


# ── Novelty evaluation ──────────────────────────────────────────────────────

def normalize_idea_novelty_text(idea: dict[str, Any]) -> str:
    """Combine idea fields into a single normalized text for novelty checking."""
    fields: list[str] = [
        str(idea.get("title") or ""),
        str(idea.get("thumbnail_line") or ""),
        str(idea.get("concept_summary") or ""),
        str(idea.get("hook") or ""),
    ]
    fields.extend(str(item) for item in (idea.get("beat_outline") or []))
    fields.extend(str(item) for item in (idea.get("editing_notes") or []))
    fields.extend(str(item) for item in (idea.get("source_patterns") or []))
    return normalize_match_text(" ".join(fields))


def evaluate_idea_batch_novelty(
    ideas: list[dict[str, Any]],
    novelty_constraints: dict[str, Any],
) -> dict[str, Any]:
    """Evaluate batch ideas against novelty constraints."""
    if not novelty_constraints:
        return {
            "needs_revision": False,
            "average_score": 1.0,
            "violating_ideas": 0,
            "items": [],
        }

    avoid_titles = novelty_constraints.get("avoid_titles") or []
    avoid_signatures = novelty_constraints.get("avoid_signatures") or []
    avoid_settings = novelty_constraints.get("avoid_settings") or []
    normalized_titles = [(title, normalize_match_text(title)) for title in avoid_titles]
    normalized_signatures = [(value, normalize_match_text(value)) for value in avoid_signatures]
    normalized_settings = [(value, normalize_match_text(value)) for value in avoid_settings]

    items: list[dict[str, Any]] = []
    total_score = 0.0
    violating_ideas = 0
    for idea in ideas:
        combined_text = normalize_idea_novelty_text(idea)
        title_text = normalize_match_text(str(idea.get("title") or ""))
        exact_title_hits = [
            original
            for original, normalized in normalized_titles
            if normalized and (title_text == normalized or normalized in title_text)
        ]
        signature_hits = [
            original
            for original, normalized in normalized_signatures
            if normalized and normalized in combined_text
        ]
        setting_hits = [
            original
            for original, normalized in normalized_settings
            if normalized and normalized in combined_text
        ]

        score = 1.0
        if exact_title_hits:
            score -= 0.45
        score -= min(0.30, 0.12 * len(signature_hits))
        score -= min(0.20, 0.08 * len(setting_hits))
        score = max(0.0, round(score, 3))
        violated = bool(exact_title_hits or signature_hits)
        if violated:
            violating_ideas += 1
        total_score += score
        items.append(
            {
                "rank": idea.get("rank"),
                "title": idea.get("title"),
                "score": score,
                "violated": violated,
                "exact_title_hits": exact_title_hits,
                "signature_hits": signature_hits,
                "setting_hits": setting_hits,
            }
        )

    average_score = round(total_score / len(ideas), 3) if ideas else 0.0
    return {
        "needs_revision": violating_ideas > 0,
        "average_score": average_score,
        "violating_ideas": violating_ideas,
        "items": items,
    }


# ── Markdown rendering ──────────────────────────────────────────────────────

def render_batch_ideas_markdown(payload: dict[str, Any]) -> str:
    """Render an idea batch payload as Markdown."""
    lines = [
        "# Idea Batch",
        "",
        f"- Query: {payload.get('query', '')}",
        f"- Cohort: {payload.get('cohort') or '-'}",
        f"- Subcohort: {payload.get('subcohort') or '-'}",
        f"- Effective Cohort: {payload.get('effective_cohort') or payload.get('cohort') or '-'}",
        f"- Generated At: {payload.get('generated_at', '')}",
        f"- Generation Attempts: {payload.get('generation_attempts', 1)}",
        "",
    ]
    quality_report = payload.get("quality_report") or {}
    if quality_report:
        lines.extend(
            [
                "## Quality Report",
                "",
                f"- Average Novelty Score: {float(quality_report.get('average_score') or 0.0):.3f}",
                f"- Violating Ideas: {int(quality_report.get('violating_ideas') or 0)}",
                "",
            ]
        )
    for idea in payload.get("ideas") or []:
        lines.extend(
            [
                f"## {idea.get('rank', '-')}. {idea.get('title', '(untitled)')}",
                "",
                f"- Thumbnail: {idea.get('thumbnail_line', '')}",
                "",
                "### Concept Summary",
                "",
                str(idea.get("concept_summary", "")).strip(),
                "",
                "### Hook",
                "",
                str(idea.get("hook", "")).strip(),
                "",
                "### Beat Outline",
                "",
            ]
        )
        for beat in idea.get("beat_outline") or []:
            lines.append(f"- {beat}")
        lines.extend(["", "### Editing Notes", ""])
        for note in idea.get("editing_notes") or []:
            lines.append(f"- {note}")
        lines.extend(["", "### Risk Checks", ""])
        for risk in idea.get("risk_checks") or []:
            lines.append(f"- {risk}")
        if idea.get("source_patterns"):
            lines.extend(["", "### Source Patterns", ""])
            for pattern in idea.get("source_patterns") or []:
                lines.append(f"- {pattern}")
        idea_quality = next(
            (item for item in quality_report.get("items") or [] if item.get("rank") == idea.get("rank")),
            None,
        )
        if idea_quality:
            lines.extend(["", "### Novelty Checks", ""])
            lines.append(f"- score: {float(idea_quality.get('score') or 0.0):.3f}")
            if idea_quality.get("exact_title_hits"):
                lines.append(f"- exact_title_hits: {' / '.join(idea_quality['exact_title_hits'])}")
            if idea_quality.get("signature_hits"):
                lines.append(f"- signature_hits: {' / '.join(idea_quality['signature_hits'])}")
            if idea_quality.get("setting_hits"):
                lines.append(f"- setting_hits: {' / '.join(idea_quality['setting_hits'])}")
        lines.append("")
    return "\n".join(lines).strip() + "\n"


# ── Gemini generation calls ─────────────────────────────────────────────────

def run_gemini_draft_generation(
    brief_payload: dict[str, Any],
    output_format: str,
    draft_kind: str,
    model_name: str,
    temperature: float,
    style_notes: str,
) -> str:
    """Generate a draft via Gemini."""
    try:
        from google import genai
        from google.genai import types
    except ImportError as exc:
        raise RuntimeError("Draft generation requires google-genai to be installed.") from exc

    load_dotenv(override=False)
    api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY (or GOOGLE_API_KEY) for draft generation.")

    client = genai.Client(api_key=api_key)
    prompt = render_draft_generation_prompt(
        brief_payload=brief_payload,
        draft_kind=draft_kind,
        output_format=output_format,
        style_notes=style_notes,
    )
    config_kwargs: dict[str, Any] = {"temperature": temperature}
    if output_format == "json":
        config_kwargs["response_mime_type"] = "application/json"

    response = client.models.generate_content(
        model=model_name,
        contents=prompt,
        config=types.GenerateContentConfig(**config_kwargs),
    )
    text = response.text or ""
    if not text.strip():
        raise RuntimeError("Gemini generation returned empty text.")

    if output_format == "json":
        parsed = parse_generated_json(text)
        return json.dumps(parsed, ensure_ascii=False, indent=2)
    return text.strip()


def generate_batch_ideas_from_prompt(
    client: Any,
    model_name: str,
    prompt: str,
    config: Any,
) -> list[dict[str, Any]]:
    """Generate batch ideas from a prompt via Gemini."""
    text = generate_content_text(
        client=client,
        model_name=model_name,
        contents=prompt,
        config=config,
        empty_error="Gemini batch idea generation returned empty text.",
    )
    parsed = parse_or_repair_generated_json(
        client=client,
        model_name=model_name,
        raw_text=text,
        empty_error="Gemini batch idea generation returned empty text.",
    )
    ideas = parsed.get("ideas")
    if not isinstance(ideas, list) or not ideas:
        raise ValueError("Batch idea generation returned no ideas.")
    return ideas


def run_gemini_batch_idea_generation(
    brief_payload: dict[str, Any],
    count: int,
    model_name: str,
    temperature: float,
    style_notes: str,
) -> dict[str, Any]:
    """Generate a batch of ideas via Gemini with novelty checking."""
    try:
        from google import genai
        from google.genai import types
    except ImportError as exc:
        raise RuntimeError("Idea batch generation requires google-genai to be installed.") from exc

    load_dotenv(override=False)
    api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY (or GOOGLE_API_KEY) for batch idea generation.")

    client = genai.Client(api_key=api_key)
    config = types.GenerateContentConfig(
        temperature=temperature,
        response_mime_type="application/json",
    )
    prompt = render_batch_idea_generation_prompt(
        brief_payload=brief_payload,
        count=count,
        style_notes=style_notes,
    )
    ideas = generate_batch_ideas_from_prompt(
        client=client,
        model_name=model_name,
        prompt=prompt,
        config=config,
    )
    quality_report = evaluate_idea_batch_novelty(
        ideas=ideas,
        novelty_constraints=brief_payload.get("novelty_constraints") or {},
    )

    attempts = 1
    while quality_report["needs_revision"] and attempts < 2:
        attempts += 1
        repair_prompt = render_batch_idea_repair_prompt(
            brief_payload=brief_payload,
            current_ideas=ideas,
            quality_report=quality_report,
            count=count,
            style_notes=style_notes,
        )
        ideas = generate_batch_ideas_from_prompt(
            client=client,
            model_name=model_name,
            prompt=repair_prompt,
            config=config,
        )
        quality_report = evaluate_idea_batch_novelty(
            ideas=ideas,
            novelty_constraints=brief_payload.get("novelty_constraints") or {},
        )

    return {
        "query": brief_payload.get("query", ""),
        "cohort": brief_payload.get("cohort", ""),
        "subcohort": brief_payload.get("subcohort", ""),
        "effective_cohort": brief_payload.get("effective_cohort", "") or brief_payload.get("cohort", ""),
        "generated_at": now_utc().isoformat(),
        "idea_count": len(ideas),
        "generation_attempts": attempts,
        "quality_report": quality_report,
        "ideas": ideas,
    }


# ── CLI entry points ─────────────────────────────────────────────────────────

def generate_draft_from_brief(
    brief_payload: dict[str, Any],
    output_path: Optional[Path],
    output_format: str,
    draft_kind: str,
    model_name: str,
    temperature: float,
    style_notes: str,
) -> None:
    """Generate a draft from a brief payload."""
    generated = run_gemini_draft_generation(
        brief_payload=brief_payload,
        output_format=output_format,
        draft_kind=draft_kind,
        model_name=model_name,
        temperature=temperature,
        style_notes=style_notes,
    )

    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(generated, encoding="utf-8")
        print(f"Wrote generated draft: {output_path}")
        return

    print(generated)


def generate_draft_from_brief_file(
    brief_path: Path,
    output_path: Optional[Path],
    output_format: str,
    draft_kind: str,
    model_name: str,
    temperature: float,
    style_notes: str,
) -> None:
    """Load a brief from file and generate a draft (CLI entry point)."""
    brief_payload = load_brief_payload(brief_path)
    generate_draft_from_brief(
        brief_payload=brief_payload,
        output_path=output_path,
        output_format=output_format,
        draft_kind=draft_kind,
        model_name=model_name,
        temperature=temperature,
        style_notes=style_notes,
    )


def generate_live_draft(
    brain: BankaraBrain,
    query: str,
    output_path: Optional[Path],
    output_format: str,
    draft_kind: str,
    model_name: str,
    temperature: float,
    style_notes: str,
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
    subcohort: Optional[str],
    brief_output_path: Optional[Path],
    brief_output_format: str,
) -> None:
    """Search, build brief, and generate draft in one call (CLI entry point)."""
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
        if brief_output_path:
            brief_output_path.parent.mkdir(parents=True, exist_ok=True)
            rendered_brief = (
                json.dumps(brief_payload, ensure_ascii=False, indent=2)
                if brief_output_format == "json"
                else render_query_brief_markdown(brief_payload)
            )
            brief_output_path.write_text(rendered_brief, encoding="utf-8")
            print(f"Wrote query brief: {brief_output_path}")

        generate_draft_from_brief(
            brief_payload=brief_payload,
            output_path=output_path,
            output_format=output_format,
            draft_kind=draft_kind,
            model_name=model_name,
            temperature=temperature,
            style_notes=style_notes,
        )
    finally:
        search_results_path.unlink(missing_ok=True)


def generate_idea_batch(
    brain: BankaraBrain,
    query: str,
    output_path: Optional[Path],
    output_format: str,
    count: int,
    model_name: str,
    temperature: float,
    style_notes: str,
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
    subcohort: Optional[str],
    brief_output_path: Optional[Path],
    brief_output_format: str,
) -> None:
    """Search, build brief, and generate idea batch (CLI entry point)."""
    if count < 1:
        raise ValueError("--count must be >= 1")

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
        if brief_output_path:
            brief_output_path.parent.mkdir(parents=True, exist_ok=True)
            rendered_brief = (
                json.dumps(brief_payload, ensure_ascii=False, indent=2)
                if brief_output_format == "json"
                else render_query_brief_markdown(brief_payload)
            )
            brief_output_path.write_text(rendered_brief, encoding="utf-8")
            print(f"Wrote query brief: {brief_output_path}")

        ideas_payload = run_gemini_batch_idea_generation(
            brief_payload=brief_payload,
            count=count,
            model_name=model_name,
            temperature=temperature,
            style_notes=style_notes,
        )
        rendered = (
            json.dumps(ideas_payload, ensure_ascii=False, indent=2)
            if output_format == "json"
            else render_batch_ideas_markdown(ideas_payload)
        )
        if output_path:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(rendered, encoding="utf-8")
            print(f"Wrote idea batch: {output_path}")
            return

        print(rendered)
    finally:
        search_results_path.unlink(missing_ok=True)
