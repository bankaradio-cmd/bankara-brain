"""Bankara Brain — Analysis pipelines.

Sub-modules:
    visual_audio    Shot-based visual + audio summary (Gemini)
    feedback        Feedback scoring (v2) from YouTube analytics
"""
from bankara_brain.analysis.visual_audio import (  # noqa: F401
    VisualAudioSummary,
    SubtitleCue,
    generate_visual_audio_summary,
    render_searchable_text,
)
from bankara_brain.analysis.feedback import (  # noqa: F401
    FeedbackInputs,
    build_feedback_inputs_from_aggregates,
    combine_feedback_score_v2,
)
