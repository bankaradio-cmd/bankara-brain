"""Draft review and revision via Gemini."""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv

from bankara_brain import BankaraBrain

from bankara_script_assistant.brief import (
    assemble_query_brief_payload,
    load_brief_payload,
)
from bankara_script_assistant.generation import (
    generate_draft_from_brief,
    run_gemini_draft_generation,
)
from bankara_script_assistant.gemini_helpers import parse_generated_json


# ── Review payload ───────────────────────────────────────────────────────────

def load_review_payload(review_path: Path) -> dict[str, Any]:
    """Load a review from disk (JSON or fallback to raw Markdown)."""
    raw = review_path.read_text(encoding="utf-8")
    try:
        loaded = json.loads(raw)
    except json.JSONDecodeError:
        return {
            "summary": "",
            "strengths": [],
            "risks": [],
            "revision_actions": [],
            "raw_review_markdown": raw,
        }
    if not isinstance(loaded, dict):
        raise ValueError("Review JSON must be an object.")
    return loaded


# ── Prompt templates ─────────────────────────────────────────────────────────

def render_draft_review_prompt(
    brief_payload: dict[str, Any],
    draft_text: str,
    output_format: str,
) -> str:
    """Render the Gemini prompt for draft review."""
    brief_json = json.dumps(brief_payload, ensure_ascii=False, indent=2)
    cohort = brief_payload.get("cohort") or "指定なし"
    subcohort = brief_payload.get("subcohort") or "なし"
    effective_cohort = brief_payload.get("effective_cohort") or cohort
    cohort_rules_text = brief_payload.get("cohort_rules_text") or "なし"
    novelty_constraints_text = brief_payload.get("novelty_constraints_text") or "なし"
    if output_format == "json":
        output_spec = (
            "次の JSON オブジェクトだけを返す: "
            "{overall_score: number, hook_score: number, pacing_score: number, originality_score: number, "
            "summary: string, strengths: string[], risks: string[], revision_actions: string[], "
            "keep_elements: string[], experiment_next: string[]}"
        )
    else:
        output_spec = (
            "Markdown で返す。見出しは `Summary`, `Scores`, `Strengths`, `Risks`, "
            "`Revision Actions`, `Keep Elements`, `Next Experiments` を使う。"
        )

    return (
        "あなたはバンカラブレインのレビューエンジンです。\n"
        "brief と draft を比較し、フック、テンポ、オリジナリティ、再利用価値の観点で厳しめにレビューしてください。\n"
        "cohort 固定ルールから外れていないかも厳しく確認してください。\n"
        "subcohort が指定されている場合は、その細いレーンからのズレも厳しく確認してください。\n"
        "近すぎる既存回の回避ルールを破っていないかも厳しく確認してください。\n"
        "特に『導入が弱い』『setup が長い』『payoff が薄い』『過去データの焼き直し』を重点チェックしてください。\n"
        f"対象 cohort: {cohort}\n"
        f"対象 subcohort: {subcohort}\n"
        f"有効レーン: {effective_cohort}\n"
        f"cohort 固定ルール:\n{cohort_rules_text}\n"
        f"近すぎる既存回の回避ルール:\n{novelty_constraints_text}\n"
        f"出力形式: {output_spec}\n\n"
        "Brief JSON:\n"
        f"{brief_json}\n\n"
        "Draft:\n"
        f"{draft_text}\n"
    )


def render_draft_revision_prompt(
    brief_payload: dict[str, Any],
    draft_text: str,
    review_payload: dict[str, Any],
    output_format: str,
    draft_kind: str,
) -> str:
    """Render the Gemini prompt for draft revision."""
    brief_json = json.dumps(brief_payload, ensure_ascii=False, indent=2)
    review_json = json.dumps(review_payload, ensure_ascii=False, indent=2)
    cohort = brief_payload.get("cohort") or "指定なし"
    subcohort = brief_payload.get("subcohort") or "なし"
    effective_cohort = brief_payload.get("effective_cohort") or cohort
    cohort_rules_text = brief_payload.get("cohort_rules_text") or "なし"
    novelty_constraints_text = brief_payload.get("novelty_constraints_text") or "なし"
    if draft_kind == "script":
        deliverable = "修正版の短い動画台本。"
    else:
        deliverable = "修正版の企画ドラフト。"

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
        "あなたはバンカラブレインの改稿エンジンです。\n"
        "brief と現在の draft と review を読み、review の revision_actions と risks を解消した改稿版を返してください。\n"
        "良い要素は保持しつつ、フック、テンポ、オチの明確さを強化してください。\n"
        "cohort 固定ルールがある場合は、必ず守ってください。\n"
        "subcohort が指定されている場合は、その細いレーンから外れる改稿をしないでください。\n"
        "近すぎる既存回の回避ルールがある場合は、被りを外してください。\n"
        f"対象 cohort: {cohort}\n"
        f"対象 subcohort: {subcohort}\n"
        f"有効レーン: {effective_cohort}\n"
        f"cohort 固定ルール:\n{cohort_rules_text}\n"
        f"近すぎる既存回の回避ルール:\n{novelty_constraints_text}\n"
        f"成果物: {deliverable}\n"
        f"出力形式: {output_spec}\n\n"
        "Brief JSON:\n"
        f"{brief_json}\n\n"
        "Current Draft:\n"
        f"{draft_text}\n\n"
        "Review JSON:\n"
        f"{review_json}\n"
    )


# ── Gemini review / revision calls ──────────────────────────────────────────

def run_gemini_draft_review(
    brief_payload: dict[str, Any],
    draft_text: str,
    output_format: str,
    model_name: str,
    temperature: float,
) -> str:
    """Review a draft via Gemini."""
    try:
        from google import genai
        from google.genai import types
    except ImportError as exc:
        raise RuntimeError("Draft review requires google-genai to be installed.") from exc

    load_dotenv(override=False)
    api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY (or GOOGLE_API_KEY) for draft review.")

    client = genai.Client(api_key=api_key)
    prompt = render_draft_review_prompt(
        brief_payload=brief_payload,
        draft_text=draft_text,
        output_format=output_format,
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
        raise RuntimeError("Gemini review returned empty text.")

    if output_format == "json":
        parsed = parse_generated_json(text)
        return json.dumps(parsed, ensure_ascii=False, indent=2)
    return text.strip()


def run_gemini_draft_revision(
    brief_payload: dict[str, Any],
    draft_text: str,
    review_payload: dict[str, Any],
    output_format: str,
    draft_kind: str,
    model_name: str,
    temperature: float,
) -> str:
    """Revise a draft via Gemini."""
    try:
        from google import genai
        from google.genai import types
    except ImportError as exc:
        raise RuntimeError("Draft revision requires google-genai to be installed.") from exc

    load_dotenv(override=False)
    api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY (or GOOGLE_API_KEY) for draft revision.")

    client = genai.Client(api_key=api_key)
    prompt = render_draft_revision_prompt(
        brief_payload=brief_payload,
        draft_text=draft_text,
        review_payload=review_payload,
        output_format=output_format,
        draft_kind=draft_kind,
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
        raise RuntimeError("Gemini revision returned empty text.")

    if output_format == "json":
        parsed = parse_generated_json(text)
        return json.dumps(parsed, ensure_ascii=False, indent=2)
    return text.strip()


# ── CLI entry points ─────────────────────────────────────────────────────────

def evaluate_draft_from_files(
    brief_path: Path,
    draft_path: Path,
    output_path: Optional[Path],
    output_format: str,
    model_name: str,
    temperature: float,
) -> None:
    """Evaluate a draft against a brief (CLI entry point)."""
    brief_payload = load_brief_payload(brief_path)
    if not draft_path.exists():
        raise FileNotFoundError(f"Draft file not found: {draft_path}")
    draft_text = draft_path.read_text(encoding="utf-8")
    rendered = run_gemini_draft_review(
        brief_payload=brief_payload,
        draft_text=draft_text,
        output_format=output_format,
        model_name=model_name,
        temperature=temperature,
    )

    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered, encoding="utf-8")
        print(f"Wrote draft review: {output_path}")
        return

    print(rendered)


def revise_draft_from_files(
    brief_path: Path,
    draft_path: Path,
    review_path: Path,
    output_path: Optional[Path],
    output_format: str,
    draft_kind: str,
    model_name: str,
    temperature: float,
) -> None:
    """Revise a draft using a brief and review file (CLI entry point)."""
    brief_payload = load_brief_payload(brief_path)
    if not draft_path.exists():
        raise FileNotFoundError(f"Draft file not found: {draft_path}")
    if not review_path.exists():
        raise FileNotFoundError(f"Review file not found: {review_path}")

    draft_text = draft_path.read_text(encoding="utf-8")
    review_payload = load_review_payload(review_path)
    revised = run_gemini_draft_revision(
        brief_payload=brief_payload,
        draft_text=draft_text,
        review_payload=review_payload,
        output_format=output_format,
        draft_kind=draft_kind,
        model_name=model_name,
        temperature=temperature,
    )

    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(revised, encoding="utf-8")
        print(f"Wrote revised draft: {output_path}")
        return

    print(revised)


def run_generation_cycle(
    brain: BankaraBrain,
    output_dir: Path,
    query: Optional[str],
    brief_path: Optional[Path],
    search_results_path: Optional[Path],
    iterations: int,
    draft_kind: str,
    draft_format: str,
    model_name: str,
    draft_temperature: float,
    review_temperature: float,
    revision_temperature: float,
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
) -> None:
    """Run brief -> draft -> review -> revise cycle (CLI entry point)."""
    if iterations < 1:
        raise ValueError("--iterations must be >= 1")

    output_dir.mkdir(parents=True, exist_ok=True)
    cycle_brief_path = output_dir / "brief.json"

    if brief_path is not None:
        brief_payload = load_brief_payload(brief_path)
    else:
        if not query:
            raise ValueError("run-cycle requires --query when --brief is not provided.")
        resolved_search_results = search_results_path
        temp_search_path: Optional[Path] = None
        if resolved_search_results is None:
            with tempfile.NamedTemporaryFile(prefix="bankara_cycle_search_", suffix=".json", delete=False) as handle:
                temp_search_path = Path(handle.name)
            brain.run_semantic_search(
                query=query,
                output_path=temp_search_path,
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
            resolved_search_results = temp_search_path

        try:
            brief_payload = assemble_query_brief_payload(
                brain=brain,
                query=query,
                search_results_path=resolved_search_results,
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
            if temp_search_path is not None:
                temp_search_path.unlink(missing_ok=True)

    cycle_brief_path.write_text(json.dumps(brief_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote cycle brief: {cycle_brief_path}")

    draft_suffix = ".json" if draft_format == "json" else ".md"
    current_draft_text = run_gemini_draft_generation(
        brief_payload=brief_payload,
        output_format=draft_format,
        draft_kind=draft_kind,
        model_name=model_name,
        temperature=draft_temperature,
        style_notes=style_notes,
    )
    current_draft_path = output_dir / f"draft_01{draft_suffix}"
    current_draft_path.write_text(current_draft_text, encoding="utf-8")
    print(f"Wrote draft: {current_draft_path}")

    for iteration in range(1, iterations + 1):
        review_json_text = run_gemini_draft_review(
            brief_payload=brief_payload,
            draft_text=current_draft_text,
            output_format="json",
            model_name=model_name,
            temperature=review_temperature,
        )
        review_path = output_dir / f"review_{iteration:02d}.json"
        review_path.write_text(review_json_text, encoding="utf-8")
        print(f"Wrote review: {review_path}")

        if iteration >= iterations:
            break

        review_payload = json.loads(review_json_text)
        current_draft_text = run_gemini_draft_revision(
            brief_payload=brief_payload,
            draft_text=current_draft_text,
            review_payload=review_payload,
            output_format=draft_format,
            draft_kind=draft_kind,
            model_name=model_name,
            temperature=revision_temperature,
        )
        current_draft_path = output_dir / f"draft_{iteration + 1:02d}{draft_suffix}"
        current_draft_path.write_text(current_draft_text, encoding="utf-8")
        print(f"Wrote revised draft: {current_draft_path}")
