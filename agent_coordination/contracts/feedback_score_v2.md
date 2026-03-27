# Contract: `P0-FEEDBACK-V2`

## Goal

Replace the current fixed `feedback_score_v1` emphasis with a richer score that reflects:
- watch ratio
- relative retention
- hook retention
- CTR
- engagement
- recency

## New Module

- `bankara_feedback_v2.py`

## Expected Public API

```python
from dataclasses import dataclass
from datetime import date
from typing import Optional

@dataclass
class FeedbackInputs:
    watch_ratio: Optional[float]
    relative_retention: Optional[float]
    hook_watch_ratio: Optional[float]
    impressions_ctr: Optional[float]
    engagement_rate: Optional[float]
    published_date: Optional[date]
    reference_date: Optional[date] = None

@dataclass
class FeedbackScoreBreakdown:
    score: float
    watch_component: float
    retention_component: float
    hook_component: float
    ctr_component: float
    engagement_component: float
    recency_multiplier: float
    notes: list[str]

def combine_feedback_score_v2(inputs: FeedbackInputs) -> FeedbackScoreBreakdown:
    ...
```

## Scoring Rules

- preserve `0.0 - 1.0` range for final score
- non-fatal missing inputs should degrade gracefully, not crash
- score should prefer recent wins without wiping out older evergreen hits
- `hook_watch_ratio` must have real weight; do not leave it as a no-op

Default weighting for the first implementation:
- watch: `0.35`
- retention: `0.15`
- hook: `0.20`
- CTR: `0.15`
- engagement: `0.15`
- recency as multiplier, not additive feature

Default recency rule:
- half-life target: `180 days`
- apply as multiplier in the range `0.70 - 1.00`
- recommended formula:
  - `recency_multiplier = 0.70 + 0.30 * (1.0 / (1.0 + days_old / 180.0))`

## Integration Requirements

- keep `feedback_score_v1` for backward compatibility
- add `feedback_score_v2`
- `run-feedback-pipeline` must write `feedback_score_v2`
- timeline propagation must use asset-level fallback when segment-level data is sparse
- diagnostics must expose count of non-zero `feedback_score_v2`
- retrieval must be able to rerank on `feedback_score_v2` later, even if that toggle is not wired in the first patch

## Acceptance

- one diagnostics report that shows score distribution for the latest50 corpus
- at least one report with before/after score examples on real Bankara assets
- no breakage to existing `feedback_score_v1` searches
