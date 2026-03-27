#!/usr/bin/env python3
"""Backward-compatible shim — re-exports Brain + consumer functions.

All business logic now lives in ``bankara_brain.*`` submodules.
This file re-exports every public name for callers that still
``import bankara_brain_control_plane``.

For the canonical CLI, see:
  ``python -m bankara_brain.cli``   (Brain-only, 37 commands)
  ``python -m bankara_script_assistant.cli``  (consumer, 8 commands)
  ``python bankara_brain_control_plane.py``   (combined, 45 commands)
"""

from __future__ import annotations

# ── Models & DB infrastructure ───────────────────────────────────────────────
from bankara_brain.models import (  # noqa: F401
    Base,
    Asset,
    AssetCuration,
    TextSegment,
    EmbeddingRecord,
    YoutubeDailyMetric,
    YoutubeRetentionPoint,
    TimelineSegment,
    FeedbackScore,
    now_utc,
)
from bankara_brain.db import (  # noqa: F401
    AppConfig,
    BlobStore,
    create_engine_and_sessionmaker,
    init_db,
)

# ── YouTube sub-package ──────────────────────────────────────────────────────
from bankara_brain.youtube.auth import (  # noqa: F401
    YOUTUBE_SCOPES,
    auth_youtube,
    build_youtube_analytics_service,
    build_youtube_data_service,
    get_youtube_credentials,
    load_google_api_dependencies,
)
from bankara_brain.youtube.analytics import (  # noqa: F401
    fetch_youtube_daily_metrics,
    fetch_youtube_retention,
    report_response_to_rows,
)
from bankara_brain.youtube.data_api import (  # noqa: F401
    check_expected_youtube_channel,
    ensure_expected_youtube_channel,
    fetch_authorized_channel_payload,
    fetch_youtube_video_catalog,
    summarize_authorized_youtube_channel,
    youtube_whoami,
)
from bankara_brain.youtube.helpers import (  # noqa: F401
    YOUTUBE_VIDEO_ID_RE,
    extract_youtube_video_id,
    first_present,
    is_valid_youtube_video_id,
    resolve_asset_id_for_video_id,
)
from bankara_brain.youtube.sync import (  # noqa: F401
    import_analytics_csv,
    sync_youtube_analytics,
)
from bankara_brain.youtube.linking import (  # noqa: F401
    list_youtube_videos,
    link_youtube_assets,
)
from bankara_brain.youtube.public import (  # noqa: F401
    DEFAULT_BANKARA_PUBLIC_CHANNEL_URL,
    DEFAULT_BANKARA_PUBLIC_CHANNEL_ID,
    DEFAULT_BANKARA_PUBLIC_EXCLUDE_KEYWORDS,
    require_yt_dlp_path,
    run_subprocess_checked,
    fetch_public_youtube_catalog,
    filter_public_youtube_catalog,
    write_download_sidecar,
    find_downloaded_media_path,
    list_public_youtube_videos,
    download_public_youtube_videos,
)

# ── Utilities ────────────────────────────────────────────────────────────────
from bankara_brain.utils import (  # noqa: F401
    format_seconds_hms,
    parse_date_value as parse_date,
    parse_int,
    parse_float,
    safe_int,
    safe_json_load,
)

# ── Ingest ───────────────────────────────────────────────────────────────────
from bankara_brain.ingest.stage import (  # noqa: F401
    build_transcript_excerpt,
    file_fingerprint,
    file_sha256,
    guess_text_mime,
    is_text_sidecar_for_media,
    iter_supported_files,
    load_segments_into_db,
    stage_asset,
    stage_dataset,
)
from bankara_brain.ingest.transcript import (  # noqa: F401
    build_synthetic_transcript_file,
    build_synthetic_transcript_line,
    default_transcribe_script_path,
    format_seconds_srt,
    load_existing_record_ids,
    load_transcript_segments,
    load_transcript_window_text,
    replace_transcript_segments,
    resolve_asset_transcript_path,
    sync_asset_transcript,
    transcribe_asset_with_faster_whisper,
)
from bankara_brain.ingest.pipeline import (  # noqa: F401
    count_jsonl_rows,
    default_embedding_python_path,
    run_embedding_manifest_ingest,
    run_ingest_pipeline,
    run_logged_subprocess,
    write_jsonl_report_row,
)

# ── Corpus ───────────────────────────────────────────────────────────────────
from bankara_brain.corpus.query import (  # noqa: F401
    asset_cohort,
    asset_matches_filters,
    asset_selection_status,
    asset_subcohort,
    effective_cohort_label,
    filter_semantic_search_results_file,
    media_has_audio_stream,
    normalize_cohort,
    normalize_filter_values,
    normalize_match_text,
    normalize_selection_status,
    normalize_subcohort,
    resolve_asset,
    resolve_asset_media_path,
    resolve_existing_path,
    resolve_search_match_asset,
    run_semantic_search_export,
    select_assets_for_filters,
)
from bankara_brain.corpus.timeline import (  # noqa: F401
    bootstrap_shot_timeline,
    build_bootstrap_timeline_segments,
    build_transcript_cues,
    import_shot_timeline,
    infer_bootstrap_segment_kind,
    list_timeline_segments,
    load_timeline_segments_file,
    normalize_timeline_segment_row,
    replace_timeline_segments,
    resolve_bootstrap_assets,
    validate_timeline_segments,
)
from bankara_brain.corpus.curation import (  # noqa: F401
    DEFAULT_BANKARA_CHANNEL,
    DEFAULT_COMEDY_EXCLUDE_KEYWORDS,
    DEFAULT_COMEDY_INCLUDE_KEYWORDS,
    WARNING_PROBLEMS,
    BLOCKER_PROBLEMS,
    asset_text_haystacks,
    audit_assets,
    auto_assign_cohorts,
    auto_curate_bankara_assets,
    build_audit_summary,
    classify_bankara_comedy_asset,
    corpus_status,
    curate_assets,
    detect_asset_problems,
    infer_bankara_asset_cohort,
    infer_embedding_kind_from_metadata,
    list_assets,
    problem_severity,
    quarantine_assets,
    split_asset_problems,
)

# ── Analysis ─────────────────────────────────────────────────────────────────
from bankara_brain.analysis.structured_summary import (  # noqa: F401
    BRAIN_SUMMARY_KEY,
    BRAIN_SUMMARY_MODEL_KEY,
    BRAIN_SUMMARY_TEXT_KEY,
    BRAIN_SUMMARY_UPDATED_AT_KEY,
    cohort_rules_file_path,
    dedupe_preserve_order,
    derive_novelty_constraints,
    extract_structured_summary_payload,
    extract_structured_summary_text,
    extract_summary_field_values,
    extract_title_signature_candidates,
    load_cohort_rules_catalog,
    merge_cohort_rules,
    normalize_cohort_rules_payload,
    normalize_rule_text_list,
    normalize_summary_list,
    normalize_summary_value_text,
    render_cohort_rules_text,
    render_novelty_constraints_text,
    render_structured_summary_text,
    resolve_cohort_rules,
)
from bankara_brain.analysis.feedback import (  # noqa: F401
    collect_feedback_pattern_rows,
    serialize_feedback_pattern,
)
from bankara_brain.analysis.scoring import (  # noqa: F401
    FEEDBACK_SCORE_FIELDS,
    average,
    clamp,
    combine_feedback_score,
    feedback_diagnostics,
    feedback_filter_kwargs,
    list_feedback_scores,
    load_feedback_summary_for_window,
    load_latest_asset_feedback_summary,
    load_latest_feedback_summary,
    print_asset_feedback_recommendation_from_pattern,
    print_timeline_feedback_recommendation_from_pattern,
    recommend_feedback_patterns,
    render_feedback_diagnostics_markdown,
    resolve_feedback_assets,
    resolve_feedback_assets_filtered,
    retention_points_for_segment,
    run_feedback_pipeline,
    score_asset_level_feedback,
    score_asset_level_feedback_from_daily_metrics,
    score_feedback,
    score_timeline_feedback,
    score_timeline_feedback_from_asset_proxy,
    write_feedback_score,
    write_hook_score,
)
from bankara_brain.analysis.enrichment import (  # noqa: F401
    BRAIN_VISUAL_AUDIO_UPDATED_AT_KEY,
    DEFAULT_GENERATION_MODEL,
    build_asset_summary_source_text,
    enrich_structured_summaries,
    enrich_visual_audio_summaries,
    normalize_structured_summary_payload,
    render_structured_summary_prompt,
    run_gemini_structured_summary_generation,
)

# ── Embedding ────────────────────────────────────────────────────────────────
from bankara_brain.embedding.manifest import (  # noqa: F401
    BRAIN_SEARCHABLE_SUMMARY_V2_KEY,
    BRAIN_VISUAL_AUDIO_SUMMARY_KEY,
    attr_or_key,
    build_embedding_record_sync_metadata,
    build_media_manifest_notes,
    build_timeline_segment_embedding_text,
    build_timeline_segment_notes,
    build_timeline_segment_title,
    chunk_list,
    enrich_curation_metadata,
    export_embedding_manifest,
    import_embedding_results,
    load_pinecone_index_from_env,
    merge_feedback_metadata,
    normalize_index_metadata,
    strip_dynamic_embedding_metadata,
)
from bankara_brain.embedding.sync import (  # noqa: F401
    purge_embeddings,
    sync_embedding_metadata,
)
from bankara_brain.embedding.benchmark import (  # noqa: F401
    DEFAULT_RETRIEVAL_BENCHMARK_FILE,
    benchmark_case_effective_lane,
    benchmark_case_matches_filters,
    benchmark_expected_lane_labels,
    benchmark_lane_support_count,
    benchmark_title_matches_expected,
    benchmark_title_rank,
    benchmark_title_support_count,
    normalize_benchmark_case,
    normalize_effective_lane_label,
    render_retrieval_benchmark_markdown,
    retrieval_benchmark_file_path,
    run_retrieval_benchmark,
)

# ── Maintenance & Pipelines ──────────────────────────────────────────────────
from bankara_brain.maintenance import doctor, repair_assets  # noqa: F401
from bankara_brain.pipelines import (  # noqa: F401
    run_corpus_cycle,
    run_maintenance_pipeline,
)

# ── External library re-exports (backward compat) ───────────────────────────
from bankara_media_utils import (  # noqa: F401
    SUPPORTED_SUFFIXES,
    build_manifest_record_id,
    build_text_chunks,
    find_sidecar_text_file,
    humanize_stem,
    infer_media_type_and_mime,
    infer_record_kind,
    load_sidecar_metadata,
    normalize_sidecar_metadata,
    probe_media_duration,
    shorten_text,
)
from bankara_feedback_v2 import (  # noqa: F401
    FeedbackInputs,
    build_feedback_inputs_from_aggregates,
    combine_feedback_score_v2,
)
from bankara_visual_audio_summary import (  # noqa: F401
    generate_visual_audio_summary,
    render_searchable_text,
    SubtitleCue,
    VisualAudioSummary,
)


# ── Consumer functions — moved to bankara_script_assistant ────────────────────
# Lazy re-exports for backward compatibility.
# Eager imports would cause a circular-import error because
# bankara_script_assistant modules import *this* module.

_CONSUMER_RE_EXPORTS: dict[str, str] = {
    # brief.py
    "assemble_query_brief_payload": "bankara_script_assistant.brief",
    "build_live_query_brief": "bankara_script_assistant.brief",
    "build_query_brief": "bankara_script_assistant.brief",
    "build_query_prompt_scaffold": "bankara_script_assistant.brief",
    "format_brief_match_time_range": "bankara_script_assistant.brief",
    "load_brief_payload": "bankara_script_assistant.brief",
    "load_semantic_search_results": "bankara_script_assistant.brief",
    "parse_markdown_brief_payload": "bankara_script_assistant.brief",
    "render_query_brief_markdown": "bankara_script_assistant.brief",
    # generation.py
    "evaluate_idea_batch_novelty": "bankara_script_assistant.generation",
    "generate_batch_ideas_from_prompt": "bankara_script_assistant.generation",
    "generate_draft_from_brief": "bankara_script_assistant.generation",
    "generate_draft_from_brief_file": "bankara_script_assistant.generation",
    "generate_idea_batch": "bankara_script_assistant.generation",
    "generate_live_draft": "bankara_script_assistant.generation",
    "normalize_idea_novelty_text": "bankara_script_assistant.generation",
    "render_batch_idea_generation_prompt": "bankara_script_assistant.generation",
    "render_batch_idea_repair_prompt": "bankara_script_assistant.generation",
    "render_batch_ideas_markdown": "bankara_script_assistant.generation",
    "render_draft_generation_prompt": "bankara_script_assistant.generation",
    "run_gemini_batch_idea_generation": "bankara_script_assistant.generation",
    "run_gemini_draft_generation": "bankara_script_assistant.generation",
    # review.py
    "evaluate_draft_from_files": "bankara_script_assistant.review",
    "load_review_payload": "bankara_script_assistant.review",
    "render_draft_review_prompt": "bankara_script_assistant.review",
    "render_draft_revision_prompt": "bankara_script_assistant.review",
    "revise_draft_from_files": "bankara_script_assistant.review",
    "run_gemini_draft_review": "bankara_script_assistant.review",
    "run_gemini_draft_revision": "bankara_script_assistant.review",
    "run_generation_cycle": "bankara_script_assistant.review",
    # gemini_helpers.py
    "generate_content_text": "bankara_script_assistant.gemini_helpers",
    "parse_generated_json": "bankara_script_assistant.gemini_helpers",
    "parse_or_repair_generated_json": "bankara_script_assistant.gemini_helpers",
}


def __getattr__(name: str):
    module_path = _CONSUMER_RE_EXPORTS.get(name)
    if module_path is not None:
        import importlib
        mod = importlib.import_module(module_path)
        value = getattr(mod, name)
        # Cache in module dict so __getattr__ is only called once.
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# ── CLI entry point ──────────────────────────────────────────────────────────
# The Brain-only CLI lives in bankara_brain.cli.  This shim combines Brain
# and consumer (script-assistant) commands for full backward compatibility.

if __name__ == "__main__":
    import sys as _sys

    from bankara_brain.cli import build_parser, run
    from bankara_script_assistant.cli import (
        dispatch_script_assistant_command,
        register_script_assistant_commands,
    )

    _parser = build_parser(extra_commands=register_script_assistant_commands)
    _args = _parser.parse_args()
    try:
        run(_args, fallback_dispatcher=dispatch_script_assistant_command)
    except Exception as _exc:
        print(f"ERROR: {_exc}", file=_sys.stderr)
        raise SystemExit(1)
