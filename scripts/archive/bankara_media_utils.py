"""Shim — re-exports everything from bankara_brain.utils.

This file exists for backward compatibility. All code now lives in
``bankara_brain.utils``.
"""
from bankara_brain.utils import (  # noqa: F401
    AUDIO_SUFFIXES,
    SIDECAR_JSON_SUFFIX,
    SUBTITLE_CHUNK_TARGET_CHARS,
    SUPPORTED_SUFFIXES,
    TEXT_CHUNK_OVERLAP_CHARS,
    TEXT_CHUNK_TARGET_CHARS,
    TEXT_SUFFIXES,
    TextChunk,
    VIDEO_SUFFIXES,
    YT_DLP_INFO_JSON_SUFFIX,
    build_manifest_record_id,
    build_plain_text_chunks,
    build_subtitle_chunks,
    build_text_chunks,
    clean_subtitle_text,
    find_sidecar_text_file,
    humanize_stem,
    infer_media_type_and_mime,
    infer_record_kind,
    load_sidecar_metadata,
    load_text_file,
    normalize_sidecar_metadata,
    normalize_whitespace,
    parse_subtitle_cues,
    parse_time_range,
    parse_timestamp,
    probe_media_duration,
    shorten_text,
    split_long_text,
)
