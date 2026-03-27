"""Shim — re-exports everything from bankara_brain.analysis.visual_audio.

This file exists for backward compatibility. All code now lives in
``bankara_brain.analysis.visual_audio``.
"""
from bankara_brain.analysis.visual_audio import (  # noqa: F401
    AudioFeatures,
    Beat,
    ShotBoundary,
    SubtitleCue,
    VisualAudioSummary,
    build_transcript_window_for_shot,
    build_visual_audio_prompt,
    call_gemini_visual_audio,
    detect_shot_boundaries,
    extract_audio_features_for_shots,
    extract_representative_frames,
    generate_visual_audio_summary,
    group_shots_into_scenes,
    parse_cues_from_srt_text,
    render_searchable_text,
)
