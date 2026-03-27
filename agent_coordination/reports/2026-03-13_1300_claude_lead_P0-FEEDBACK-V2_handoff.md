# Handoff Report: P0-FEEDBACK-V2

- **Agent**: Claude Code Lead
- **Packet**: `P0-FEEDBACK-V2`
- **Status**: `complete`
- **Date**: 2026-03-13 13:00 JST

## What Was Done

### 1. New Module: `bankara_feedback_v2.py`

Self-contained scoring module with no control-plane coupling.

**Public API:**
- `FeedbackInputs` — dataclass for raw metric inputs
- `FeedbackScoreBreakdown` — dataclass with full score decomposition + `.to_dict()`
- `combine_feedback_score_v2(inputs, weights=None)` — core scorer
- `build_feedback_inputs_from_aggregates(...)` — convenience builder from DB-level aggregates

**Scoring design:**
- Default weights: watch=0.35, retention=0.15, hook=0.20, CTR=0.15, engagement=0.15
- Missing inputs → weight redistributed proportionally to present axes (score stays in [0,1])
- Recency multiplier: `0.70 + 0.30 / (1 + days_old / 180)` → range [0.70, 1.00]
- Engagement rate derived as `(likes + comments*3 + shares*2) / views`

### 2. Control Plane Integration: `bankara_brain_control_plane.py`

**Changes:**

| Area | What changed |
|------|-------------|
| Import | Added `from bankara_feedback_v2 import ...` (line 64) |
| `FEEDBACK_SCORE_FIELDS` | Added `"feedback_score_v2"` to the tuple |
| `score_feedback()` | Always fetches `daily_rows` before branching; passes to both `score_asset_level_feedback` and `score_timeline_feedback` |
| `score_asset_level_feedback()` | Added `daily_rows` kwarg; computes v2 at end using hook avg from retention ≤0.15 + daily metrics |
| `score_asset_level_feedback_from_daily_metrics()` | Computes v2 from daily metrics proxy; passes `feedback_score_v2` to `score_timeline_feedback_from_asset_proxy` |
| `score_timeline_feedback_from_asset_proxy()` | Added `feedback_score_v2` parameter; writes v2 per segment (asset-level fallback) |
| `score_timeline_feedback()` | Added `daily_rows` kwarg; computes per-segment v2 using segment-local retention + asset-level CTR/engagement |
| `_parse_published_date()` | New helper to safely parse `asset.published_at` → `date | None` |
| `feedback_diagnostics()` | Added `feedback_score_v2` to per-asset rows; added `assets_with_nonzero_feedback_score_v2` to payload; added v2 recommendation |
| `render_feedback_diagnostics_markdown()` | Shows v2 coverage count; top-asset lines show both v1 and v2 |

### 3. Backward Compatibility

- `feedback_score_v1` is untouched — same formula, same writes, same retrieval
- `combine_feedback_score()` (the v1 function) is not modified
- Existing search/rerank on `feedback_score_v1` continues to work
- v2 is purely additive — new `FeedbackScore` rows with `score_name="feedback_score_v2"`

## Smoke Test Results

```
Test 1 (full inputs): score=0.33284
  watch=0.105 retention=0.085 hook=0.096 ctr=0.06 eng=0.048
  recency=0.844772

Test 2 (sparse, watch only): score=0.333333
  notes=['missing: retention, hook, ctr, engagement']

Test 3 (empty): score=0.0
  notes=['no input data — score=0.0']
```

## How to Validate

1. Run `python bankara_brain_control_plane.py run-feedback-pipeline --start-date 2025-01-01 --end-date 2026-03-13`
2. Run `python bankara_brain_control_plane.py feedback-diagnostics --start-date 2025-01-01 --end-date 2026-03-13 --format markdown`
3. Check that `assets_with_nonzero_feedback_score_v2` > 0

## Files Changed

- `bankara_feedback_v2.py` — **NEW** (262 lines)
- `bankara_brain_control_plane.py` — **MODIFIED** (import, 6 functions, diagnostics)

## Next Steps / Integration Notes

- **Retrieval rerank on v2**: The `feedback_score_v2` is stored and queryable but not yet used in `rerank_by_feedback`. A future toggle can switch reranking from v1 to v2.
- **Cross-encoder integration**: Once `P0-CROSS-ENCODER-RERANK` is integrated, the retrieval pipeline can combine v2 feedback reranking with cross-encoder reranking.
- **Benchmark**: Run `retrieval-benchmark` before/after `run-feedback-pipeline` to see if v2 reranking improves hit quality for the latest50 corpus.
