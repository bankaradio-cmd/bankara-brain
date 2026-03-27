"""Structured summary helpers — text normalization, rendering, cohort rules, novelty."""
from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

from bankara_brain.utils import shorten_text

# ── Constants ─────────────────────────────────────────────────────────────────

BRAIN_SUMMARY_KEY = "brain_summary_v1"
BRAIN_SUMMARY_TEXT_KEY = "brain_summary_text_v1"
BRAIN_SUMMARY_MODEL_KEY = "brain_summary_model_v1"
BRAIN_SUMMARY_UPDATED_AT_KEY = "brain_summary_updated_at"
DEFAULT_COHORT_RULES_FILE = "cohort_rules.json"

# Project root — one level above the bankara_brain/ package directory.
_BRAIN_PACKAGE_DIR = Path(__file__).resolve().parent.parent
_PROJECT_DIR = _BRAIN_PACKAGE_DIR.parent


# ── Summary value normalization ───────────────────────────────────────────────

def normalize_summary_value_text(value: Any, max_length: int) -> str:
    """Normalize a summary value to a short text string."""
    if value in (None, ""):
        return ""
    if isinstance(value, list):
        text = ", ".join(str(item).strip() for item in value if str(item).strip())
    else:
        text = str(value).strip()
    return shorten_text(text, max_length)


def normalize_summary_list(value: Any, max_items: int, max_length: int) -> list[str]:
    """Normalize a list-valued summary field."""
    if not isinstance(value, list):
        return []
    normalized: list[str] = []
    for item in value:
        text = normalize_summary_value_text(item, max_length)
        if text:
            normalized.append(text)
        if len(normalized) >= max_items:
            break
    return normalized


# ── Structured summary extraction / rendering ─────────────────────────────────

def extract_structured_summary_payload(metadata: dict[str, Any]) -> dict[str, Any]:
    """Extract the structured summary dict from asset metadata."""
    raw = metadata.get(BRAIN_SUMMARY_KEY)
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            loaded = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return loaded if isinstance(loaded, dict) else {}
    return {}


def render_structured_summary_text(summary: dict[str, Any], compact: bool = False) -> str:
    """Render a structured summary dict as human-readable text."""
    if not summary:
        return ""

    ordered_fields = [
        ("premise", "premise"),
        ("character_engine", "character_engine"),
        ("authority_flip", "authority_flip"),
        ("hook_pattern", "hook_pattern"),
        ("escalation_pattern", "escalation_pattern"),
        ("payoff_pattern", "payoff_pattern"),
        ("setting", "setting"),
        ("searchable_summary", "searchable_summary"),
    ]
    lines: list[str] = []
    for key, label in ordered_fields:
        value = normalize_summary_value_text(summary.get(key), 240)
        if value:
            lines.append(f"{label}: {value}")

    tone_tags = normalize_summary_list(summary.get("tone_tags"), max_items=8, max_length=32)
    if tone_tags:
        lines.append(f"tone_tags: {', '.join(tone_tags)}")

    novelty_guardrails = normalize_summary_list(summary.get("novelty_guardrails"), max_items=5, max_length=80)
    if novelty_guardrails:
        lines.append(f"novelty_guardrails: {' / '.join(novelty_guardrails)}")

    rendered = "\n".join(lines)
    if compact:
        rendered = rendered.replace("\n", " | ")
    return shorten_text(rendered, 900)


def extract_structured_summary_text(metadata: dict[str, Any], compact: bool = False) -> str:
    """Extract or render summary text from asset metadata."""
    text_value = metadata.get(BRAIN_SUMMARY_TEXT_KEY)
    if isinstance(text_value, str) and text_value.strip():
        rendered = shorten_text(text_value.strip(), 900)
        return rendered.replace("\n", " | ") if compact else rendered
    return render_structured_summary_text(extract_structured_summary_payload(metadata), compact=compact)


# ── Cohort rules ──────────────────────────────────────────────────────────────

def cohort_rules_file_path() -> Path:
    """Return the path to the cohort rules JSON file."""
    configured = os.getenv("BANKARA_COHORT_RULES_FILE")
    if configured:
        return Path(configured).expanduser().resolve()
    return _PROJECT_DIR / DEFAULT_COHORT_RULES_FILE


def load_cohort_rules_catalog() -> dict[str, Any]:
    """Load the full cohort rules catalog from disk."""
    path = cohort_rules_file_path()
    if not path.exists():
        return {}
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return loaded if isinstance(loaded, dict) else {}


def normalize_rule_text_list(value: Any, max_items: int = 12, max_length: int = 160) -> list[str]:
    """Normalize a list of rule text values."""
    if not isinstance(value, list):
        return []
    normalized: list[str] = []
    for item in value:
        text = normalize_summary_value_text(item, max_length)
        if text:
            normalized.append(text)
        if len(normalized) >= max_items:
            break
    return normalized


def normalize_cohort_rules_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Normalize a raw cohort rules payload."""
    if not isinstance(payload, dict):
        return {}
    normalized = {
        "identity": normalize_summary_value_text(payload.get("identity"), 200),
        "must_keep": normalize_rule_text_list(payload.get("must_keep"), max_items=8, max_length=140),
        "avoid": normalize_rule_text_list(payload.get("avoid"), max_items=8, max_length=140),
        "novelty_targets": normalize_rule_text_list(payload.get("novelty_targets"), max_items=8, max_length=140),
        "thumbnail_bias": normalize_rule_text_list(payload.get("thumbnail_bias"), max_items=6, max_length=120),
        "editing_bias": normalize_rule_text_list(payload.get("editing_bias"), max_items=8, max_length=120),
        "payoff_bias": normalize_rule_text_list(payload.get("payoff_bias"), max_items=6, max_length=120),
    }
    return {key: value for key, value in normalized.items() if value not in ("", [], None)}


def merge_cohort_rules(base_rules: dict[str, Any], cohort_rules: dict[str, Any]) -> dict[str, Any]:
    """Merge cohort-specific rules onto base rules, deduplicating lists."""
    merged = dict(base_rules)
    for key, value in cohort_rules.items():
        if isinstance(value, list):
            existing = merged.get(key)
            combined = []
            for item in (existing or []):
                if item not in combined:
                    combined.append(item)
            for item in value:
                if item not in combined:
                    combined.append(item)
            merged[key] = combined
        else:
            merged[key] = value
    return merged


def resolve_cohort_rules(cohort: str | None, subcohort: str | None = None) -> dict[str, Any]:
    """Resolve the effective cohort rules by merging default + cohort + subcohort."""
    catalog = load_cohort_rules_catalog()
    default_rules = normalize_cohort_rules_payload(catalog.get("default", {}))
    resolved_rules = dict(default_rules)
    normalized_cohort = (cohort or "").strip()
    normalized_subcohort = (subcohort or "").strip()
    if normalized_cohort:
        resolved_rules = merge_cohort_rules(
            resolved_rules,
            normalize_cohort_rules_payload(catalog.get(normalized_cohort, {})),
        )
    if normalized_subcohort:
        resolved_rules = merge_cohort_rules(
            resolved_rules,
            normalize_cohort_rules_payload(catalog.get(normalized_subcohort, {})),
        )
    return resolved_rules


def render_cohort_rules_text(rules: dict[str, Any]) -> str:
    """Render cohort rules as human-readable text."""
    if not rules:
        return ""
    lines: list[str] = []
    if rules.get("identity"):
        lines.append(f"identity: {rules['identity']}")
    for key, label in (
        ("must_keep", "must_keep"),
        ("avoid", "avoid"),
        ("novelty_targets", "novelty_targets"),
        ("thumbnail_bias", "thumbnail_bias"),
        ("editing_bias", "editing_bias"),
        ("payoff_bias", "payoff_bias"),
    ):
        values = rules.get(key) or []
        if values:
            lines.append(f"{label}:")
            lines.extend(f"- {value}" for value in values)
    return "\n".join(lines)


# ── Novelty / dedup helpers ───────────────────────────────────────────────────

def dedupe_preserve_order(items: list[str], max_items: int | None = None) -> list[str]:
    """Remove duplicates while preserving order."""
    unique: list[str] = []
    for item in items:
        text = item.strip()
        if not text or text in unique:
            continue
        unique.append(text)
        if max_items is not None and len(unique) >= max_items:
            break
    return unique


def extract_summary_field_values(summary_text: str, field_name: str) -> list[str]:
    """Extract values for a named field from summary text."""
    if not summary_text.strip():
        return []
    lines = summary_text.replace(" | ", "\n").splitlines()
    values: list[str] = []
    for line in lines:
        normalized = line.strip()
        prefix = f"{field_name}:"
        if not normalized.startswith(prefix):
            continue
        raw_value = normalized.split(":", 1)[1].strip()
        for fragment in re.split(r"[、,/・]| and | と ", raw_value):
            text = normalize_summary_value_text(fragment, 48)
            if text:
                values.append(text)
    return dedupe_preserve_order(values, max_items=6)


def extract_title_signature_candidates(title: str, cohort: str | None = None) -> list[str]:
    """Extract title 'signature' candidates (the variable part of formulaic titles)."""
    normalized_title = normalize_summary_value_text(title, 120)
    if not normalized_title:
        return []

    patterns_by_cohort = {
        "mother-profession": [
            r"最恐の母(?:親)?が(.+?)になったら",
            r"もしも.+?が(.+?)になったら",
        ],
        "genius-kid-shop": [
            r"天才小学生が(.+?)を開いたら",
            r"もしも.+?が(.+?)を開いたら",
        ],
        "genius-kid-school-event": [
            r"天才小学生が(.+?)に出たら",
            r"もしも.+?が(.+?)に出たら",
        ],
        "genius-kid-game-world": [
            r"天才小学生が(.+?)にどハマりしたら",
            r"もしも.+?が(.+?)にどハマりしたら",
        ],
    }
    generic_patterns = [
        r"もしも.+?が(.+?)になったら",
        r"もしも.+?が(.+?)を開いたら",
        r"もしも.+?が(.+?)に出たら",
        r"もしも.+?が(.+?)にどハマりしたら",
    ]

    normalized_cohort = (cohort or "").strip()
    patterns: list[str] = []
    candidate_cohort = normalized_cohort
    while candidate_cohort:
        patterns = list(patterns_by_cohort.get(candidate_cohort, []))
        if patterns:
            break
        if "-" not in candidate_cohort:
            break
        candidate_cohort = candidate_cohort.rsplit("-", 1)[0]
    patterns += generic_patterns
    signatures: list[str] = []
    for pattern in patterns:
        match = re.search(pattern, normalized_title)
        if not match:
            continue
        candidate = normalize_summary_value_text(match.group(1), 48)
        if candidate:
            signatures.append(candidate)
    return dedupe_preserve_order(signatures, max_items=4)


def derive_novelty_constraints(
    query: str,
    semantic_matches: list[dict[str, Any]],
    asset_patterns: list[dict[str, Any]],
    cohort: str | None,
) -> dict[str, Any]:
    """Derive novelty constraints from semantic search matches and feedback patterns."""
    titles: list[str] = []
    signatures: list[str] = []
    settings: list[str] = []

    for match in semantic_matches[:6]:
        title = normalize_summary_value_text(match.get("title"), 100)
        if title:
            titles.append(title)
            signatures.extend(extract_title_signature_candidates(title, cohort=cohort))
        settings.extend(extract_summary_field_values(str(match.get("summary") or ""), "setting"))

    for pattern in asset_patterns[:4]:
        asset_title = normalize_summary_value_text(pattern.get("asset_title"), 100)
        if asset_title:
            titles.append(asset_title)
            signatures.extend(extract_title_signature_candidates(asset_title, cohort=cohort))
        settings.extend(extract_summary_field_values(str(pattern.get("asset_summary_text") or ""), "setting"))

    normalized_query = normalize_summary_value_text(query, 120)
    filtered_signatures = [
        signature
        for signature in dedupe_preserve_order(signatures, max_items=8)
        if signature and signature not in normalized_query
    ]

    return {
        "avoid_titles": dedupe_preserve_order(titles, max_items=6),
        "avoid_signatures": filtered_signatures[:8],
        "avoid_settings": dedupe_preserve_order(settings, max_items=6),
        "directive": "既存の近い回の構造は借りてよいが、タイトル・役職・店種・舞台をそのままなぞらない。",
    }


def render_novelty_constraints_text(constraints: dict[str, Any]) -> str:
    """Render novelty constraints as human-readable text."""
    if not constraints:
        return ""
    lines: list[str] = []
    if constraints.get("directive"):
        lines.append(f"directive: {constraints['directive']}")
    for key, label in (
        ("avoid_titles", "avoid_titles"),
        ("avoid_signatures", "avoid_signatures"),
        ("avoid_settings", "avoid_settings"),
    ):
        values = normalize_rule_text_list(constraints.get(key), max_items=8, max_length=120)
        if values:
            lines.append(f"{label}: {' / '.join(values)}")
    return "\n".join(lines)
