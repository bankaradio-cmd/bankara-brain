"""Gemini API helpers — thin wrappers for content generation and JSON parsing."""
from __future__ import annotations

import json
from typing import Any


def generate_content_text(
    client: Any,
    model_name: str,
    contents: str,
    config: Any,
    empty_error: str,
) -> str:
    """Call Gemini ``generate_content`` and return the text, raising on empty."""
    response = client.models.generate_content(
        model=model_name,
        contents=contents,
        config=config,
    )
    text = response.text or ""
    if not text.strip():
        raise RuntimeError(empty_error)
    return text


def parse_generated_json(text: str) -> dict[str, Any]:
    """Extract a JSON object from Gemini output, tolerating code fences."""
    stripped = text.strip()
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
    raise ValueError("Gemini JSON output could not be parsed.")


def parse_or_repair_generated_json(
    client: Any,
    model_name: str,
    raw_text: str,
    empty_error: str,
) -> dict[str, Any]:
    """Parse JSON from Gemini output; on failure, ask Gemini to repair it."""
    try:
        return parse_generated_json(raw_text)
    except ValueError:
        repair_prompt = (
            "次のテキストを、有効な JSON オブジェクトだけに整形して返してください。\n"
            "情報を捨てず、説明文やコードフェンスは付けないこと。\n\n"
            f"{raw_text}\n"
        )
        repair_text = generate_content_text(
            client=client,
            model_name=model_name,
            contents=repair_prompt,
            config={"temperature": 0, "response_mime_type": "application/json"},
            empty_error=empty_error,
        )
        return parse_generated_json(repair_text)
