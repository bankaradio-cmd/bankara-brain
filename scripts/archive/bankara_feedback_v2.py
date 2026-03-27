"""Shim — re-exports everything from bankara_brain.analysis.feedback.

This file exists for backward compatibility. All code now lives in
``bankara_brain.analysis.feedback``.
"""
from bankara_brain.analysis.feedback import (  # noqa: F401
    FeedbackInputs,
    FeedbackScoreBreakdown,
    build_feedback_inputs_from_aggregates,
    combine_feedback_score_v2,
    compute_recency_multiplier,
)
