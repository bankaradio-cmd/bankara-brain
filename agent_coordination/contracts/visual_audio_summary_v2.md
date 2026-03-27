# Contract: `P0-VISUAL-AUDIO-SUMMARY`

## Goal

Produce richer, shot-based summaries for Bankara videos so retrieval and generation can use:
- visual beats
- text overlays / telops
- pacing
- audio peaks / SE-like moments
- BGM state
- expression / staging cues

Do not use fixed 2-second windows as the primary segmentation rule.

## New Module

- `bankara_visual_audio_summary.py`

Optional helpers if needed:
- `bankara_scene_utils.py`
- `bankara_audio_features.py`

## Input

- local video path
- optional transcript text
- optional timed subtitle segments

## Pipeline Contract

1. detect shot boundaries
2. choose one representative frame per shot
3. build a short text window around the shot using transcript/subtitles
4. derive simple audio features:
   - loudness peak
   - silence / non-silence
   - coarse BGM/SE presence hints
5. call Gemini 2.5 Flash/Pro to emit structured JSON

## Output Schema

```json
{
  "summary_version": "visual_audio_summary_v1",
  "asset_id": 123,
  "beats": [
    {
      "start_sec": 0.0,
      "end_sec": 2.8,
      "visual_event": "mother enters classroom and dominates the frame",
      "telop_text": ["最恐の母", "教師着任"],
      "dialogue_summary": "mother declares she will run the class",
      "audio_events": ["impact SE", "crowd reaction"],
      "pace_label": "fast",
      "tension_label": "high",
      "hook_strength": 0.92
    }
  ],
  "editing_patterns": ["jump cuts", "stacked impact se", "aggressive telops"],
  "searchable_summary_text": "..."
}
```

## Storage Contract

Do not mutate existing fields silently.

Additive storage is preferred:
- `brain_visual_audio_summary_v1` as JSON
- `brain_searchable_summary_v2` as text

## Acceptance

- valid JSON emitted for 5 real Bankara videos
- no crash on silent video
- searchable summary text exists and is human-readable
- one report includes 2-3 concrete examples of recovered visual/audio cues that transcript-only summaries missed
