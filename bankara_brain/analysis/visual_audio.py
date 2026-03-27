"""bankara_visual_audio_summary.py

Shot-based visual + audio summary pipeline for Bankara Brain.

Contract: P0-VISUAL-AUDIO-SUMMARY
Produces structured beat-level summaries that capture visual events,
telop text, dialogue, audio events, pacing, and tension — information
that transcript-only summaries miss entirely.

Pipeline:
  1. Detect shot boundaries via ffmpeg scene detection
  2. Extract one representative frame per shot
  3. Build a text window around each shot from transcript/subtitles
  4. Derive simple audio features per shot (loudness, silence, SE hints)
  5. Call Gemini 2.5 Flash/Pro with frames + context → structured JSON

Dependencies:
  - ffmpeg / ffprobe  (CLI, must be on PATH)
  - google-genai      (Gemini API)

Storage keys written by the control plane integration:
  - brain_visual_audio_summary_v1  (JSON dict)
  - brain_searchable_summary_v2    (text)
"""

from __future__ import annotations

import base64
import json
import os
import re
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ShotBoundary:
    """A detected shot boundary."""
    index: int
    start_sec: float
    end_sec: float
    duration: float = 0.0

    def __post_init__(self) -> None:
        self.duration = max(0.0, self.end_sec - self.start_sec)


@dataclass
class AudioFeatures:
    """Simple per-shot audio features derived from ffmpeg."""
    mean_volume_db: float | None = None
    max_volume_db: float | None = None
    is_silence: bool = False
    has_loudness_peak: bool = False
    # coarse hints — set by Gemini later when frames are analyzed
    bgm_hint: str = ""       # "none" | "low" | "present"
    se_hint: str = ""         # "" | "impact" | "crowd" | ...


@dataclass
class Beat:
    """One beat in the visual-audio summary."""
    start_sec: float
    end_sec: float
    visual_event: str = ""
    telop_text: list[str] = field(default_factory=list)
    dialogue_summary: str = ""
    audio_events: list[str] = field(default_factory=list)
    pace_label: str = ""       # "slow" | "normal" | "fast"
    tension_label: str = ""    # "low" | "medium" | "high"
    hook_strength: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "start_sec": round(self.start_sec, 2),
            "end_sec": round(self.end_sec, 2),
            "visual_event": self.visual_event,
            "telop_text": self.telop_text,
            "dialogue_summary": self.dialogue_summary,
            "audio_events": self.audio_events,
            "pace_label": self.pace_label,
            "tension_label": self.tension_label,
            "hook_strength": round(self.hook_strength, 2),
        }


@dataclass
class VisualAudioSummary:
    """Complete visual-audio summary for one asset."""
    summary_version: str = "visual_audio_summary_v1"
    asset_id: str = ""
    beats: list[Beat] = field(default_factory=list)
    editing_patterns: list[str] = field(default_factory=list)
    searchable_summary_text: str = ""
    model_used: str = ""
    frame_count: int = 0
    shot_count: int = 0
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "summary_version": self.summary_version,
            "asset_id": self.asset_id,
            "beats": [b.to_dict() for b in self.beats],
            "editing_patterns": self.editing_patterns,
            "searchable_summary_text": self.searchable_summary_text,
            "model_used": self.model_used,
            "frame_count": self.frame_count,
            "shot_count": self.shot_count,
            "notes": self.notes,
        }


# ---------------------------------------------------------------------------
# 1. Shot boundary detection via ffmpeg
# ---------------------------------------------------------------------------

DEFAULT_SCENE_THRESHOLD = 0.3
MAX_SHOTS = 200          # raw shot cap before grouping
MAX_SCENES = 20          # final scene (beat) count after grouping
MIN_SHOT_DURATION = 0.3  # seconds


def detect_shot_boundaries(
    video_path: Path,
    threshold: float = DEFAULT_SCENE_THRESHOLD,
    duration_seconds: float | None = None,
) -> list[ShotBoundary]:
    """Detect shot boundaries using ffmpeg scene change detection.

    Returns a list of ShotBoundary objects sorted by start time.
    Falls back to uniform segmentation if ffmpeg is unavailable or fails.
    """
    ffprobe_path = shutil.which("ffprobe")
    if not ffprobe_path:
        return _fallback_uniform_shots(duration_seconds or 0.0)

    # Use ffprobe with lavfi to detect scene changes
    cmd = [
        ffprobe_path,
        "-v", "quiet",
        "-f", "lavfi",
        f"movie={str(video_path)},select='gt(scene\\,{threshold})'",
        "-show_entries", "frame=pts_time",
        "-of", "csv=p=0",
    ]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, check=False, timeout=120,
        )
    except (subprocess.TimeoutExpired, OSError):
        return _fallback_uniform_shots(duration_seconds or 0.0)

    # Parse scene change timestamps
    timestamps: list[float] = [0.0]
    if result.returncode == 0 and result.stdout.strip():
        for line in result.stdout.strip().splitlines():
            line = line.strip().rstrip(",")
            if not line:
                continue
            try:
                ts = float(line)
                if ts > timestamps[-1] + MIN_SHOT_DURATION:
                    timestamps.append(ts)
            except ValueError:
                continue

    # If scene detection returned nothing useful, try alternate method
    if len(timestamps) <= 1:
        timestamps = _detect_scenes_via_filter(video_path, threshold)

    total_duration = duration_seconds or _probe_duration(video_path) or 0.0
    if total_duration > 0 and (not timestamps or timestamps[-1] < total_duration - 0.5):
        timestamps.append(total_duration)

    if len(timestamps) < 2:
        return _fallback_uniform_shots(total_duration)

    # Build shot boundaries
    shots: list[ShotBoundary] = []
    for i in range(len(timestamps) - 1):
        shot = ShotBoundary(
            index=i,
            start_sec=timestamps[i],
            end_sec=timestamps[i + 1],
        )
        if shot.duration >= MIN_SHOT_DURATION:
            shots.append(shot)

    # Merge very short shots
    shots = _merge_short_shots(shots)

    # Limit raw shots
    if len(shots) > MAX_SHOTS:
        shots = _subsample_shots(shots, MAX_SHOTS)

    # Re-index
    for i, shot in enumerate(shots):
        shot.index = i

    return shots


def group_shots_into_scenes(
    shots: list[ShotBoundary],
    max_scenes: int = MAX_SCENES,
) -> list[ShotBoundary]:
    """Group adjacent shots into scenes for high-cut-rate videos.

    Bankara-style comedy videos have 200+ cuts in a 10-minute video.
    Sending each cut as a separate beat makes Gemini output uniform.
    Instead, group shots into ~max_scenes contiguous scenes.
    """
    if len(shots) <= max_scenes:
        return shots

    total_duration = shots[-1].end_sec - shots[0].start_sec if shots else 0
    if total_duration <= 0:
        return shots[:max_scenes]

    target_scene_duration = total_duration / max_scenes
    scenes: list[ShotBoundary] = []
    current_start = shots[0].start_sec
    current_end = shots[0].end_sec
    idx = 0

    for shot in shots[1:]:
        current_end = shot.end_sec
        elapsed = current_end - current_start
        if elapsed >= target_scene_duration and len(scenes) < max_scenes - 1:
            scenes.append(ShotBoundary(
                index=idx,
                start_sec=current_start,
                end_sec=current_end,
            ))
            idx += 1
            current_start = shot.end_sec

    # Last scene
    if current_start < shots[-1].end_sec:
        scenes.append(ShotBoundary(
            index=idx,
            start_sec=current_start,
            end_sec=shots[-1].end_sec,
        ))

    return scenes


def _detect_scenes_via_filter(
    video_path: Path,
    threshold: float,
) -> list[float]:
    """Alternate scene detection using ffmpeg select filter with metadata."""
    ffmpeg_path = shutil.which("ffmpeg")
    if not ffmpeg_path:
        return [0.0]

    cmd = [
        ffmpeg_path,
        "-i", str(video_path),
        "-vf", f"select='gt(scene,{threshold})',showinfo",
        "-vsync", "vfr",
        "-f", "null",
        "-",
    ]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, check=False, timeout=180,
        )
    except (subprocess.TimeoutExpired, OSError):
        return [0.0]

    timestamps = [0.0]
    # Parse showinfo output for pts_time
    for line in result.stderr.splitlines():
        match = re.search(r"pts_time:\s*([\d.]+)", line)
        if match:
            ts = float(match.group(1))
            if ts > timestamps[-1] + MIN_SHOT_DURATION:
                timestamps.append(ts)
    return timestamps


def _fallback_uniform_shots(
    duration: float,
    target_count: int = 12,
) -> list[ShotBoundary]:
    """Create uniform shots when scene detection is unavailable."""
    if duration <= 0:
        return []
    segment_len = max(2.0, duration / target_count)
    shots: list[ShotBoundary] = []
    start = 0.0
    idx = 0
    while start < duration - 0.5:
        end = min(start + segment_len, duration)
        shots.append(ShotBoundary(index=idx, start_sec=start, end_sec=end))
        start = end
        idx += 1
    return shots


def _merge_short_shots(
    shots: list[ShotBoundary],
    min_duration: float = 0.5,
) -> list[ShotBoundary]:
    """Merge shots shorter than min_duration into their neighbors."""
    if not shots:
        return shots
    merged: list[ShotBoundary] = [shots[0]]
    for shot in shots[1:]:
        if shot.duration < min_duration:
            merged[-1].end_sec = shot.end_sec
            merged[-1].duration = merged[-1].end_sec - merged[-1].start_sec
        else:
            merged.append(shot)
    return merged


def _subsample_shots(shots: list[ShotBoundary], max_count: int) -> list[ShotBoundary]:
    """Keep max_count shots, evenly spaced."""
    if len(shots) <= max_count:
        return shots
    step = len(shots) / max_count
    indices = [int(i * step) for i in range(max_count)]
    result: list[ShotBoundary] = []
    for idx in indices:
        s = shots[idx]
        # Extend to next selected shot's start
        next_idx_pos = indices.index(idx) + 1 if idx in indices else -1
        result.append(s)
    # Fix boundaries so they're contiguous
    for i in range(len(result) - 1):
        result[i].end_sec = result[i + 1].start_sec
        result[i].duration = result[i].end_sec - result[i].start_sec
    if result:
        result[-1].end_sec = shots[-1].end_sec
        result[-1].duration = result[-1].end_sec - result[-1].start_sec
    return result


# ---------------------------------------------------------------------------
# 2. Frame extraction
# ---------------------------------------------------------------------------

MAX_FRAMES = 30  # Gemini token budget limit
FRAME_JPEG_QUALITY = 75


def extract_representative_frames(
    video_path: Path,
    shots: list[ShotBoundary],
    output_dir: Path,
    max_frames: int = MAX_FRAMES,
) -> list[tuple[ShotBoundary, Path]]:
    """Extract one JPEG frame per shot at the midpoint.

    Returns list of (shot, frame_path) tuples.
    """
    ffmpeg_path = shutil.which("ffmpeg")
    if not ffmpeg_path:
        return []

    output_dir.mkdir(parents=True, exist_ok=True)

    # Subsample if too many shots
    selected = shots
    if len(shots) > max_frames:
        step = len(shots) / max_frames
        indices = sorted(set(int(i * step) for i in range(max_frames)))
        selected = [shots[i] for i in indices if i < len(shots)]

    results: list[tuple[ShotBoundary, Path]] = []
    for shot in selected:
        midpoint = shot.start_sec + shot.duration / 2.0
        frame_path = output_dir / f"frame_{shot.index:04d}.jpg"
        cmd = [
            ffmpeg_path,
            "-ss", f"{midpoint:.3f}",
            "-i", str(video_path),
            "-frames:v", "1",
            "-q:v", str(FRAME_JPEG_QUALITY),
            "-y",
            str(frame_path),
        ]
        try:
            subprocess.run(cmd, capture_output=True, check=False, timeout=30)
        except (subprocess.TimeoutExpired, OSError):
            continue

        if frame_path.exists() and frame_path.stat().st_size > 100:
            results.append((shot, frame_path))

    return results


# ---------------------------------------------------------------------------
# 3. Transcript window builder
# ---------------------------------------------------------------------------

@dataclass
class SubtitleCue:
    start_sec: float
    end_sec: float
    text: str


def build_transcript_window_for_shot(
    shot: ShotBoundary,
    cues: list[SubtitleCue],
    max_chars: int = 400,
) -> str:
    """Extract subtitle text overlapping with a shot's time range."""
    relevant = [
        cue for cue in cues
        if cue.end_sec > shot.start_sec and cue.start_sec < shot.end_sec
    ]
    if not relevant:
        # Try nearest
        if cues:
            mid = (shot.start_sec + shot.end_sec) / 2
            nearest = min(cues, key=lambda c: abs((c.start_sec + c.end_sec) / 2 - mid))
            relevant = [nearest]
        else:
            return ""

    text = " ".join(c.text for c in relevant).strip()
    if len(text) > max_chars:
        text = text[:max_chars] + "..."
    return text


def parse_cues_from_srt_text(raw: str) -> list[SubtitleCue]:
    """Parse SRT/VTT raw text into SubtitleCue objects."""
    from bankara_media_utils import parse_subtitle_cues
    raw_cues = parse_subtitle_cues(raw)
    return [
        SubtitleCue(
            start_sec=c["start_seconds"],
            end_sec=c["end_seconds"],
            text=c["text"],
        )
        for c in raw_cues
        if c.get("start_seconds") is not None
    ]


# ---------------------------------------------------------------------------
# 4. Audio feature extraction
# ---------------------------------------------------------------------------

SILENCE_THRESHOLD_DB = -40.0
LOUDNESS_PEAK_DB = -10.0


def extract_audio_features_for_shots(
    video_path: Path,
    shots: list[ShotBoundary],
) -> dict[int, AudioFeatures]:
    """Extract basic audio features per shot using ffmpeg volumedetect.

    Returns dict mapping shot.index → AudioFeatures.
    """
    ffmpeg_path = shutil.which("ffmpeg")
    if not ffmpeg_path:
        return {s.index: AudioFeatures() for s in shots}

    features: dict[int, AudioFeatures] = {}
    for shot in shots:
        af = AudioFeatures()
        cmd = [
            ffmpeg_path,
            "-ss", f"{shot.start_sec:.3f}",
            "-t", f"{shot.duration:.3f}",
            "-i", str(video_path),
            "-af", "volumedetect",
            "-f", "null",
            "-",
        ]
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, check=False, timeout=30,
            )
            stderr = result.stderr
            mean_match = re.search(r"mean_volume:\s*([-\d.]+)\s*dB", stderr)
            max_match = re.search(r"max_volume:\s*([-\d.]+)\s*dB", stderr)
            if mean_match:
                af.mean_volume_db = float(mean_match.group(1))
            if max_match:
                af.max_volume_db = float(max_match.group(1))
            if af.mean_volume_db is not None:
                af.is_silence = af.mean_volume_db < SILENCE_THRESHOLD_DB
                af.has_loudness_peak = (af.max_volume_db or -99) > LOUDNESS_PEAK_DB
        except (subprocess.TimeoutExpired, OSError):
            pass

        features[shot.index] = af

    return features


# ---------------------------------------------------------------------------
# 5. Gemini multimodal call
# ---------------------------------------------------------------------------

DEFAULT_MODEL = "gemini-2.5-flash"


def build_visual_audio_prompt(
    title: str,
    shots_context: list[dict[str, Any]],
    cohort: str = "",
    subcohort: str = "",
) -> str:
    """Build the Gemini prompt for visual-audio summary generation."""
    shots_text = json.dumps(shots_context, ensure_ascii=False, indent=2)
    return (
        "あなたはバンカラブレインの映像・音声分析エンジンです。\n"
        "動画のフレーム画像と各ショットのメタ情報（タイムスタンプ、セリフ、音量）を受け取り、\n"
        "ショットごとの構造化ビート分析を行ってください。\n\n"
        "## 抽出項目\n"
        "各ビートについて以下を抽出:\n"
        "- visual_event: そのショットで起きている映像上の出来事（テロップ出現、人物の動き、カメラワーク）\n"
        "- telop_text: 画面に表示されているテロップ・テキスト（配列、なければ空配列）\n"
        "- dialogue_summary: そのショットのセリフ要約（1文）\n"
        "- audio_events: 音響イベント（impact SE, 笑い声, crowd reaction, BGM変化, 無音 など）\n"
        "- pace_label: 編集テンポ (slow / normal / fast)\n"
        "- tension_label: 緊張度 (low / medium / high)\n"
        "- hook_strength: 視聴者を引きつける強さ 0.0〜1.0\n\n"
        "## 全体分析\n"
        "- editing_patterns: この動画で使われている編集パターン（配列）\n"
        "  例: jump cuts, stacked impact se, aggressive telops, slow zoom, reaction insert\n"
        "- searchable_summary_text: 映像・音声の特徴を含む検索用サマリー（2-3文、日本語）\n\n"
        "## 出力形式\n"
        "JSONオブジェクトだけを返してください。\n"
        "```\n"
        "{\n"
        '  "beats": [{start_sec, end_sec, visual_event, telop_text, dialogue_summary, '
        'audio_events, pace_label, tension_label, hook_strength}, ...],\n'
        '  "editing_patterns": [...],\n'
        '  "searchable_summary_text": "..."\n'
        "}\n"
        "```\n\n"
        f"動画タイトル: {title}\n"
        f"cohort: {cohort or 'unspecified'}\n"
        f"subcohort: {subcohort or 'unspecified'}\n\n"
        "ショット情報:\n"
        f"{shots_text}\n"
    )


def call_gemini_visual_audio(
    frames: list[tuple[ShotBoundary, Path]],
    shots_context: list[dict[str, Any]],
    title: str,
    cohort: str = "",
    subcohort: str = "",
    model_name: str = DEFAULT_MODEL,
    temperature: float = 0.7,
) -> dict[str, Any]:
    """Call Gemini with frames and shot context to generate structured summary.

    Returns parsed JSON dict with beats, editing_patterns, searchable_summary_text.
    """
    try:
        from google import genai
        from google.genai import types
    except ImportError as exc:
        raise RuntimeError(
            "Visual-audio summary requires google-genai. Install: pip install google-genai"
        ) from exc

    from dotenv import load_dotenv
    load_dotenv(override=False)

    api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY (or GOOGLE_API_KEY).")

    client = genai.Client(api_key=api_key)

    # Build multimodal content: text prompt + frame images
    prompt_text = build_visual_audio_prompt(title, shots_context, cohort, subcohort)

    contents: list[Any] = [prompt_text]

    # Add frame images as inline parts
    for shot, frame_path in frames:
        frame_bytes = frame_path.read_bytes()
        contents.append(
            types.Part.from_bytes(data=frame_bytes, mime_type="image/jpeg")
        )
        contents.append(f"[Frame for shot #{shot.index}, {shot.start_sec:.1f}s - {shot.end_sec:.1f}s]")

    # Retry with exponential backoff for transient errors (rate limits, server errors)
    max_retries = 4
    base_delay = 3.0
    last_exc: Exception | None = None

    for attempt in range(max_retries + 1):
        try:
            response = client.models.generate_content(
                model=model_name,
                contents=contents,
                config=types.GenerateContentConfig(
                    temperature=temperature,
                    response_mime_type="application/json",
                ),
            )

            text = response.text or ""
            if not text.strip():
                raise RuntimeError("Gemini returned empty response for visual-audio summary.")

            return _parse_json_response(text)

        except Exception as exc:
            last_exc = exc
            msg = str(exc).upper()
            transient = any(m in msg for m in (
                "429", "500", "502", "503", "504",
                "RESOURCE_EXHAUSTED", "RATE_LIMIT", "TOO MANY REQUESTS",
                "UNAVAILABLE", "TIMEOUT", "TIMED OUT", "BAD GATEWAY",
            ))
            if not transient or attempt >= max_retries:
                raise

            delay = base_delay * (2 ** attempt)
            print(
                f"  Gemini transient error (attempt {attempt + 1}/{max_retries + 1}), "
                f"retrying in {delay:.0f}s: {exc}",
                flush=True,
            )
            time.sleep(delay)

    raise last_exc  # unreachable, but keeps type checker happy


def _parse_json_response(text: str) -> dict[str, Any]:
    """Parse JSON from Gemini response, handling markdown code blocks."""
    text = text.strip()
    # Remove markdown code blocks
    code_match = re.search(r"```(?:json)?\s*\n?(.*?)```", text, re.DOTALL)
    if code_match:
        text = code_match.group(1).strip()

    # Find JSON object
    brace_start = text.find("{")
    if brace_start < 0:
        raise ValueError(f"No JSON object found in response: {text[:200]}")

    # Find matching closing brace
    depth = 0
    for i, ch in enumerate(text[brace_start:], start=brace_start):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                json_str = text[brace_start : i + 1]
                return json.loads(json_str)

    # Fallback: try parsing the whole thing
    return json.loads(text)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def generate_visual_audio_summary(
    video_path: Path,
    asset_id: str = "",
    title: str = "",
    cohort: str = "",
    subcohort: str = "",
    transcript_text: str | None = None,
    subtitle_cues: list[SubtitleCue] | None = None,
    duration_seconds: float | None = None,
    model_name: str = DEFAULT_MODEL,
    temperature: float = 0.7,
    scene_threshold: float = DEFAULT_SCENE_THRESHOLD,
) -> VisualAudioSummary:
    """Full pipeline: detect shots → extract frames → analyze audio → call Gemini.

    Args:
        video_path: Path to video file (.mp4 / .mov)
        asset_id: Asset identifier for the summary
        title: Video title
        cohort/subcohort: Content classification
        transcript_text: Raw SRT/VTT text (will be parsed into cues)
        subtitle_cues: Pre-parsed subtitle cues (takes precedence over transcript_text)
        duration_seconds: Video duration (probed if not provided)
        model_name: Gemini model to use
        temperature: Gemini temperature
        scene_threshold: ffmpeg scene change threshold (0-1, lower = more sensitive)

    Returns:
        VisualAudioSummary with beats, editing patterns, and searchable text.
    """
    if not video_path.exists():
        raise FileNotFoundError(f"Video not found: {video_path}")

    # Probe duration if needed
    if duration_seconds is None:
        duration_seconds = _probe_duration(video_path) or 0.0

    summary = VisualAudioSummary(
        asset_id=asset_id,
        model_used=model_name,
    )

    # 1. Detect shots and group into scenes
    raw_shots = detect_shot_boundaries(video_path, scene_threshold, duration_seconds)
    if not raw_shots:
        summary.notes.append("No shots detected; skipped visual-audio analysis.")
        return summary

    summary.shot_count = len(raw_shots)

    # Group many cuts into manageable scenes (Bankara videos have 200+ cuts)
    scenes = group_shots_into_scenes(raw_shots, max_scenes=MAX_SCENES)
    summary.notes.append(f"raw_shots={len(raw_shots)} grouped_scenes={len(scenes)}")

    # 2. Parse subtitle cues
    cues: list[SubtitleCue] = subtitle_cues or []
    if not cues and transcript_text:
        cues = parse_cues_from_srt_text(transcript_text)

    # 3. Extract frames + audio in a temp directory
    with tempfile.TemporaryDirectory(prefix="bankara_vas_") as tmpdir:
        tmp_path = Path(tmpdir)

        # Extract frames (one per scene, not per raw shot)
        frame_results = extract_representative_frames(
            video_path, scenes, tmp_path / "frames"
        )
        summary.frame_count = len(frame_results)

        if not frame_results:
            summary.notes.append("Frame extraction failed; proceeding with text-only analysis.")

        # Extract audio features per scene
        audio_features = extract_audio_features_for_shots(video_path, scenes)

        # 4. Build context per scene
        shots_context: list[dict[str, Any]] = []
        for shot in scenes:
            ctx: dict[str, Any] = {
                "shot_index": shot.index,
                "start_sec": round(shot.start_sec, 2),
                "end_sec": round(shot.end_sec, 2),
                "duration": round(shot.duration, 2),
            }

            # Transcript window (larger for scenes than individual shots)
            transcript_window = build_transcript_window_for_shot(shot, cues, max_chars=600)
            if transcript_window:
                ctx["dialogue"] = transcript_window

            # Audio features
            af = audio_features.get(shot.index)
            if af:
                if af.mean_volume_db is not None:
                    ctx["mean_volume_db"] = round(af.mean_volume_db, 1)
                if af.max_volume_db is not None:
                    ctx["max_volume_db"] = round(af.max_volume_db, 1)
                if af.is_silence:
                    ctx["audio_note"] = "silence"
                elif af.has_loudness_peak:
                    ctx["audio_note"] = "loudness_peak"

            shots_context.append(ctx)

        # 5. Call Gemini
        try:
            gemini_result = call_gemini_visual_audio(
                frames=frame_results,
                shots_context=shots_context,
                title=title,
                cohort=cohort,
                subcohort=subcohort,
                model_name=model_name,
                temperature=temperature,
            )
        except Exception as exc:
            summary.notes.append(f"Gemini call failed: {exc}")
            return summary

    # Parse Gemini response into Beat objects
    raw_beats = gemini_result.get("beats") or []
    for rb in raw_beats:
        beat = Beat(
            start_sec=float(rb.get("start_sec", 0)),
            end_sec=float(rb.get("end_sec", 0)),
            visual_event=str(rb.get("visual_event", "")),
            telop_text=_ensure_str_list(rb.get("telop_text")),
            dialogue_summary=str(rb.get("dialogue_summary", "")),
            audio_events=_ensure_str_list(rb.get("audio_events")),
            pace_label=str(rb.get("pace_label", "")),
            tension_label=str(rb.get("tension_label", "")),
            hook_strength=_clamp(float(rb.get("hook_strength", 0)), 0.0, 1.0),
        )
        summary.beats.append(beat)

    summary.editing_patterns = _ensure_str_list(gemini_result.get("editing_patterns"))
    summary.searchable_summary_text = str(
        gemini_result.get("searchable_summary_text", "")
    ).strip()

    return summary


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _probe_duration(video_path: Path) -> float | None:
    """Probe video duration via ffprobe."""
    from bankara_media_utils import probe_media_duration
    return probe_media_duration(video_path)


def _ensure_str_list(value: Any) -> list[str]:
    """Coerce value to list[str]."""
    if isinstance(value, list):
        return [str(v) for v in value if v]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


# ---------------------------------------------------------------------------
# Render searchable text from summary
# ---------------------------------------------------------------------------

def render_searchable_text(summary: VisualAudioSummary, max_chars: int = 600) -> str:
    """Render a human-readable, search-optimized text from the summary."""
    parts: list[str] = []

    if summary.searchable_summary_text:
        parts.append(summary.searchable_summary_text)

    if summary.editing_patterns:
        parts.append(f"editing: {', '.join(summary.editing_patterns)}")

    # Add key visual events
    for beat in summary.beats[:8]:
        if beat.visual_event:
            time_label = f"[{beat.start_sec:.0f}s]"
            parts.append(f"{time_label} {beat.visual_event}")
        if beat.telop_text:
            parts.append(f"telop: {', '.join(beat.telop_text)}")

    text = " / ".join(parts)
    if len(text) > max_chars:
        text = text[:max_chars - 3] + "..."
    return text
