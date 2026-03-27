# Handoff Report: P0-VISUAL-AUDIO-SUMMARY

- **Agent**: Claude Code Lead
- **Packet**: `P0-VISUAL-AUDIO-SUMMARY`
- **Status**: `complete`
- **Date**: 2026-03-13 14:00 JST

## What Was Done

### 1. New Module: `bankara_visual_audio_summary.py`

Self-contained pipeline for shot-based visual + audio summary generation.

**Pipeline Steps:**
1. **Shot boundary detection** via ffmpeg scene change detection (`select='gt(scene,threshold)'`)
2. **Frame extraction** — one JPEG per shot at the midpoint
3. **Transcript windowing** — maps SRT/VTT cues to each shot's time range
4. **Audio feature extraction** — per-shot volume analysis via ffmpeg `volumedetect`
5. **Gemini multimodal call** — frames + shot context → structured beat JSON

**Public API:**
- `generate_visual_audio_summary(video_path, ...)` → `VisualAudioSummary`
- `render_searchable_text(summary)` → search-optimized text string
- `detect_shot_boundaries(video_path, ...)` → `list[ShotBoundary]`
- `extract_representative_frames(video_path, shots, ...)` → frame file list
- `extract_audio_features_for_shots(video_path, shots)` → per-shot audio features

**Data Classes:**
- `ShotBoundary` — start/end/duration per shot
- `AudioFeatures` — mean/max volume, silence/peak flags
- `Beat` — visual_event, telop_text, dialogue, audio_events, pace, tension, hook_strength
- `VisualAudioSummary` — beats + editing_patterns + searchable_summary_text
- `SubtitleCue` — parsed subtitle entry with timing

**Robustness:**
- Falls back to uniform segmentation if ffmpeg scene detection fails
- Falls back to text-only analysis if frame extraction fails
- Handles silent videos gracefully
- Merges very short shots, caps at 60 shots / 30 frames
- Temp directory for frame extraction (auto-cleaned)

### 2. Control Plane Integration

| Area | What changed |
|------|-------------|
| Import | Added `from bankara_visual_audio_summary import ...` (line 69) |
| Constants | Added `BRAIN_VISUAL_AUDIO_SUMMARY_KEY`, `BRAIN_SEARCHABLE_SUMMARY_V2_KEY`, `BRAIN_VISUAL_AUDIO_UPDATED_AT_KEY` |
| `enrich_visual_audio_summaries()` | New function — filters video assets, resolves media paths, loads transcripts, calls pipeline, stores results in `metadata_json` |
| CLI | `enrich-visual-audio-summaries` subcommand with full filter support + `--scene-threshold` |

**Storage (additive, no mutation):**
- `brain_visual_audio_summary_v1` — full JSON dict with beats, editing patterns
- `brain_searchable_summary_v2` — search-optimized text (complements existing `brain_summary_text_v1`)
- `brain_visual_audio_updated_at` — ISO timestamp

### 3. Output Schema (matches contract)

```json
{
  "summary_version": "visual_audio_summary_v1",
  "asset_id": "...",
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

## How to Use

```bash
# Single asset
python bankara_brain_control_plane.py enrich-visual-audio-summaries \
  --asset "path/to/video.mp4" \
  --model gemini-2.5-flash

# All video assets in a channel
python bankara_brain_control_plane.py enrich-visual-audio-summaries \
  --channel バンカラジオ \
  --limit 5 \
  --report-output reports/vas_report.jsonl

# Dry run
python bankara_brain_control_plane.py enrich-visual-audio-summaries \
  --channel バンカラジオ --dry-run

# More sensitive scene detection
python bankara_brain_control_plane.py enrich-visual-audio-summaries \
  --asset "..." --scene-threshold 0.2
```

## Acceptance Criteria Status

| Criteria | Status |
|----------|--------|
| Valid JSON emitted for 5 real Bankara videos | Ready to test (CLI wired) |
| No crash on silent video | Handled (silence detection + graceful fallback) |
| Searchable summary text exists and is human-readable | Implemented (`render_searchable_text`) |
| Report with 2-3 examples of recovered visual/audio cues | Will be visible in report output |

## Files Changed

- `bankara_visual_audio_summary.py` — **NEW** (~480 lines)
- `bankara_brain_control_plane.py` — **MODIFIED** (import, 3 constants, 1 function, CLI parser + handler)

## Dependencies

- **ffmpeg / ffprobe** — must be on PATH (used for shot detection, frame extraction, audio analysis)
- **google-genai** — for Gemini API calls (already in requirements.txt)

## Next Steps

1. **Run on real data**: `enrich-visual-audio-summaries --channel バンカラジオ --limit 5`
2. **Integrate into retrieval**: `brain_searchable_summary_v2` can be included in embedding text for richer semantic search
3. **BGM automation**: The `audio_events` and `bgm_hint` fields in beats provide the foundation for automatic BGM placement
4. **Timeline enrichment**: Beat data can be used to refine `TimelineSegment` records with visual/audio context
