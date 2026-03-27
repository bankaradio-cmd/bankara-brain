"""Shim — re-exports everything from bankara_brain.embedding.rerank.

This file exists for backward compatibility. All code now lives in
``bankara_brain.embedding.rerank``.
"""
from bankara_brain.embedding.rerank import (  # noqa: F401
    DEFAULT_CROSS_ENCODER_MODEL,
    DEFAULT_CROSS_ENCODER_TOP_K,
    DEFAULT_CROSS_ENCODER_WEIGHT,
    SUMMARY_JSON_KEY,
    SUMMARY_TEXT_KEY,
    apply_cross_encoder_fallback,
    apply_cross_encoder_scores,
    build_cross_encoder_prompt,
    build_default_genai_client,
    clone_match,
    cross_encoder_score_candidates,
    extract_searchable_summary,
    extract_transcript_excerpt,
    normalize_cross_encoder_results,
    parse_cross_encoder_response,
    prepare_cross_encoder_candidate,
    rerank_matches_with_client,
    rerank_matches_with_gemini,
    shorten_text,
)
