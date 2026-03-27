#!/usr/bin/env python3
"""
層1: scene_card v1 自動抽出スクリプト

Gemini分析テキスト → Opus 4.6 API → 構造化 scene_card → DB保存
v1: イベント単位細分化（15-45秒）+ primary/secondary comedy_type + micro_hotspots

使い方:
  # 3本テスト（推奨: まずこれで品質確認）
  python scripts/extract_scene_cards.py --test

  # 特定の動画だけ
  python scripts/extract_scene_cards.py --slug police,conbini,tosochu1

  # 全60本実行
  python scripts/extract_scene_cards.py --all

  # ドライラン（API呼び出しなし、プロンプト確認用）
  python scripts/extract_scene_cards.py --slug police --dry-run

必要な環境変数:
  ANTHROPIC_API_KEY=sk-ant-xxxxx
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

# プロジェクトルートをパスに追加
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# .env から環境変数を読み込み
try:
    from dotenv import load_dotenv
    load_dotenv(str(PROJECT_ROOT / ".env"), override=True)
except ImportError:
    pass  # dotenvがなければ環境変数を直接使う

from bankara_brain.analysis.prompts import (
    SCENE_CARD_EXTRACTION_SYSTEM,
    SCENE_CARD_EXTRACTION_PROMPT,
)
from bankara_brain.analysis.schema_design import (
    SCENE_TYPES,
    COMEDY_TYPES,
    EMOTION_TYPES,
    NARRATIVE_ROLES,
)

DB_PATH = PROJECT_ROOT / "bankara_brain.db"

# Anthropic API の設定
MODEL = "claude-opus-4-20250514"  # v1: 全層Opus
MAX_TOKENS = 16384  # 細分化でシーン数が増えるため拡大
RATE_LIMIT_DELAY = 2.0  # Opusはレート制限が厳しいため少し長めに

# 出自情報
ONTOLOGY_VERSION = "v1"
SCENE_CARD_VERSION = "v1"


def _override_model(new_model: str):
    """モジュールレベルのMODEL変数をオーバーライド"""
    global MODEL
    MODEL = new_model


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DB操作
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def get_db():
    """DB接続を取得"""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def get_video_info(conn, slug: str | None = None, all_videos: bool = False, test: bool = False):
    """動画情報を取得"""
    cur = conn.cursor()

    if test:
        # テスト用: お店系、最恐の母系、ゲーム系から1本ずつ
        test_slugs = ("conbini", "spy", "smashbros")
        placeholders = ",".join("?" * len(test_slugs))
        cur.execute(f"""
            SELECT v.slug, v.series, v.asset_id, a.title
            FROM video_slugs v
            JOIN assets a ON v.asset_id = a.id
            WHERE v.slug IN ({placeholders})
            ORDER BY v.slug
        """, test_slugs)
    elif slug:
        slugs = [s.strip() for s in slug.split(",")]
        placeholders = ",".join("?" * len(slugs))
        cur.execute(f"""
            SELECT v.slug, v.series, v.asset_id, a.title
            FROM video_slugs v
            JOIN assets a ON v.asset_id = a.id
            WHERE v.slug IN ({placeholders})
            ORDER BY v.slug
        """, slugs)
    elif all_videos:
        cur.execute("""
            SELECT v.slug, v.series, v.asset_id, a.title
            FROM video_slugs v
            JOIN assets a ON v.asset_id = a.id
            ORDER BY v.slug
        """)
    else:
        return []

    return [dict(row) for row in cur.fetchall()]


def get_gemini_analysis(conn, asset_id: str) -> str | None:
    """Gemini分析テキストを取得"""
    cur = conn.cursor()
    cur.execute(
        "SELECT gemini_analysis FROM brain_deep_analysis WHERE asset_id = ?",
        (asset_id,),
    )
    row = cur.fetchone()
    return row["gemini_analysis"] if row else None


def get_estimated_video_length(conn, asset_id: str) -> float:
    """推定動画長を計算（秒）"""
    cur = conn.cursor()
    # まずassetsテーブルのduration_secondsを使う
    cur.execute("SELECT duration_seconds FROM assets WHERE id = ?", (asset_id,))
    row = cur.fetchone()
    if row and row[0] and row[0] > 0:
        return row[0]
    # なければメトリクスから推定
    cur.execute("""
        SELECT AVG(average_view_duration_seconds), AVG(average_view_percentage)
        FROM youtube_daily_metrics WHERE asset_id = ?
    """, (asset_id,))
    row = cur.fetchone()
    if not row or not row[0] or not row[1] or row[1] == 0:
        return 600.0  # デフォルト10分
    return row[0] / (row[1] / 100)


def get_retention_for_range(conn, asset_id: str, video_length: float,
                             start_sec: float, end_sec: float) -> dict:
    """シーンの時間範囲に対応する維持率を取得"""
    start_ratio = start_sec / video_length if video_length > 0 else 0
    end_ratio = end_sec / video_length if video_length > 0 else 0
    cur = conn.cursor()
    cur.execute("""
        SELECT
            ROUND(MAX(audience_watch_ratio) * 100, 1),
            ROUND(MIN(audience_watch_ratio) * 100, 1),
            ROUND(AVG(audience_watch_ratio) * 100, 1)
        FROM youtube_retention_points
        WHERE asset_id = ?
          AND elapsed_video_time_ratio >= ?
          AND elapsed_video_time_ratio <= ?
    """, (asset_id, start_ratio, end_ratio))
    row = cur.fetchone()
    max_r = row[0] or 0
    min_r = row[1] or 0
    avg_r = row[2] or 0
    return {
        "start": max_r,
        "end": min_r,
        "avg": avg_r,
        "delta": round(min_r - max_r, 1),
    }


def save_scene_cards(conn, cards: list[dict]):
    """scene_cards v1をDBに保存（既存データは先に削除）"""
    cur = conn.cursor()
    # 既存データを削除（新旧混在を防ぐ）
    if cards:
        asset_id = cards[0]["asset_id"]
        cur.execute("DELETE FROM scene_cards WHERE asset_id = ?", (asset_id,))
    for card in cards:
        cur.execute("""
            INSERT OR REPLACE INTO scene_cards (
                scene_id, asset_id, video_title, scene_index, scene_count,
                start_seconds, end_seconds, duration_seconds,
                retention_start_pct, retention_end_pct, retention_avg_pct, retention_delta_pct,
                series, scene_type, comedy_type,
                primary_comedy_type, secondary_comedy_type,
                energy, emotion, narrative_role,
                one_line, key_dialogue, comedy_mechanism,
                characters, scene_driver, tags,
                micro_hotspots, classification_confidence,
                source_model, ontology_version, scene_card_version, generated_at,
                updated_at
            ) VALUES (
                ?, ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?,
                ?, ?, ?, ?,
                CURRENT_TIMESTAMP
            )
        """, (
            card["scene_id"], card["asset_id"], card["video_title"],
            card["scene_index"], card["scene_count"],
            card["start_seconds"], card["end_seconds"], card["duration_seconds"],
            card["retention_start_pct"], card["retention_end_pct"],
            card["retention_avg_pct"], card["retention_delta_pct"],
            card["series"], card["scene_type"],
            card["primary_comedy_type"],  # comedy_type = primary（後方互換）
            card["primary_comedy_type"], card["secondary_comedy_type"],
            card["energy"], card["emotion"], card["narrative_role"],
            card["one_line"], card.get("key_dialogue", ""),
            card.get("comedy_mechanism", ""),
            json.dumps(card.get("characters", []), ensure_ascii=False),
            card.get("scene_driver", ""),
            json.dumps(card.get("tags", []), ensure_ascii=False),
            json.dumps(card.get("micro_hotspots", []), ensure_ascii=False),
            card.get("classification_confidence", 1.0),
            card.get("source_model", MODEL),
            card.get("ontology_version", ONTOLOGY_VERSION),
            card.get("scene_card_version", SCENE_CARD_VERSION),
            card.get("generated_at", ""),
        ))
    conn.commit()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# API呼び出し
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def call_anthropic_api(system: str, prompt: str, model: str = None) -> str:
    """Anthropic APIを呼び出してテキストを返す"""
    if model is None:
        model = MODEL
    try:
        import anthropic
    except ImportError:
        print("❌ anthropic パッケージが必要です: pip install anthropic")
        sys.exit(1)

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("❌ ANTHROPIC_API_KEY 環境変数を設定してください")
        sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key)

    # Opusは応答時間が長いためストリーミングを使用
    collected_text = []
    with client.messages.stream(
        model=model,
        max_tokens=MAX_TOKENS,
        system=system,
        messages=[{"role": "user", "content": prompt}],
    ) as stream:
        for text in stream.text_stream:
            collected_text.append(text)

    return "".join(collected_text)


def parse_json_response(text: str) -> list[dict]:
    """API応答からJSONを抽出してパース"""
    cleaned = text.strip()
    if cleaned.startswith("```"):
        lines = cleaned.split("\n")
        start = 1
        end = len(lines) - 1
        for i, line in enumerate(lines):
            if i > 0 and line.strip() == "```":
                end = i
                break
        cleaned = "\n".join(lines[start:end])

    return json.loads(cleaned)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# バリデーション
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def validate_scene_card(card: dict, valid_enums: dict) -> list[str]:
    """scene_card v1のバリデーション。エラーリストを返す"""
    errors = []

    # 必須フィールドチェック
    required = ["scene_index", "start_seconds", "end_seconds", "scene_type",
                "primary_comedy_type", "energy", "emotion", "narrative_role",
                "one_line", "characters", "scene_driver"]
    for field in required:
        if field not in card or card[field] is None:
            errors.append(f"  必須フィールド '{field}' が欠けています")

    # enum値チェック
    if card.get("scene_type") not in valid_enums["scene_type"]:
        errors.append(f"  scene_type '{card.get('scene_type')}' は無効")
    if card.get("primary_comedy_type") not in valid_enums["comedy_type"]:
        errors.append(f"  primary_comedy_type '{card.get('primary_comedy_type')}' は無効")
    if card.get("secondary_comedy_type") and card.get("secondary_comedy_type") not in valid_enums["comedy_type"]:
        errors.append(f"  secondary_comedy_type '{card.get('secondary_comedy_type')}' は無効")
    if card.get("emotion") not in valid_enums["emotion"]:
        errors.append(f"  emotion '{card.get('emotion')}' は無効")
    if card.get("narrative_role") not in valid_enums["narrative_role"]:
        errors.append(f"  narrative_role '{card.get('narrative_role')}' は無効")

    # energy範囲チェック
    energy = card.get("energy", 0)
    if not (1 <= energy <= 5):
        errors.append(f"  energy {energy} は範囲外（1-5）")

    # classification_confidenceチェック
    conf = card.get("classification_confidence", 1.0)
    if not (0.0 <= conf <= 1.0):
        errors.append(f"  classification_confidence {conf} は範囲外（0.0-1.0）")

    # one_lineの長さチェック
    one_line = card.get("one_line", "")
    if len(one_line) > 60:
        errors.append(f"  one_line が長すぎます（{len(one_line)}文字、目安30文字以内）")

    return errors


def auto_fix_enum(card: dict, valid_enums: dict) -> list[str]:
    """enum外の値を最も近い有効値に自動修正する。修正内容のリストを返す"""
    fixes = []
    defaults = {
        "scene_type": "展開_エスカレーション",
        "primary_comedy_type": "なし",
        "secondary_comedy_type": "なし",
        "comedy_type": "なし",
        "emotion": "コミカル",
        "narrative_role": "escalation",
    }

    # v1: primary/secondary comedy_type
    for field in ["primary_comedy_type", "secondary_comedy_type"]:
        val = card.get(field)
        if val and val not in valid_enums["comedy_type"]:
            fallback = defaults.get(field, "なし")
            fixes.append(f"  🔧 {field}: '{val}' → '{fallback}'（自動修正）")
            card[field] = fallback

    # その他のenum
    for field in ["scene_type", "emotion", "narrative_role"]:
        val = card.get(field)
        if val and val not in valid_enums.get(field, []):
            fallback = defaults.get(field, valid_enums[field][0])
            fixes.append(f"  🔧 {field}: '{val}' → '{fallback}'（自動修正）")
            card[field] = fallback

    return fixes


def auto_fix_timestamps(cards: list[dict], video_duration: float) -> tuple[list[str], list[str]]:
    """タイムスタンプの異常を自動修正する。

    超過したシーンは捨てて、有効なシーンだけ残す方式。

    Returns:
        (fixes, errors): fixes=修正・警告リスト, errors=致命的エラーリスト
        errorsが空でなければ、このデータはリジェクトすべき。
    """
    fixes = []
    errors = []
    dur = video_duration

    # ステップ1: start > 動画長 のシーンを除外
    valid_cards = []
    for card in cards:
        idx = card.get("scene_index", 0)
        s = card.get("start_seconds", 0)
        if s > dur:
            fixes.append(f"  🗑️ シーン{idx}: start={s:.0f}s が動画長{dur:.0f}sを超過 → 除外")
        else:
            valid_cards.append(card)

    # 除外されたシーンがあればリストを差し替え
    removed_count = len(cards) - len(valid_cards)
    if removed_count > 0:
        fixes.append(f"  📋 {removed_count}シーンを動画長超過で除外（{len(valid_cards)}シーン残存）")
        cards.clear()
        cards.extend(valid_cards)

    # ステップ2: start/end逆転の修正
    for card in cards:
        idx = card.get("scene_index", 0)
        s = card.get("start_seconds", 0)
        e = card.get("end_seconds", 0)

        if e < s:
            fixes.append(f"  🔧 シーン{idx}: start/end逆転 ({s:.0f}→{e:.0f}s) → ({e:.0f}→{s:.0f}s)")
            card["start_seconds"], card["end_seconds"] = e, s
            s, e = e, s

        # end が動画長をわずかに超過 → クランプ
        if e > dur:
            fixes.append(f"  🔧 シーン{idx}: end {e:.0f}s → {dur:.0f}s（超過をクランプ）")
            card["end_seconds"] = dur

    # ステップ3: シーンをscene_indexで並べ替え
    sorted_cards = sorted(cards, key=lambda c: c.get("scene_index", 0))

    # ステップ4: シーン間のギャップを連続化
    for i in range(len(sorted_cards) - 1):
        curr = sorted_cards[i]
        nxt = sorted_cards[i + 1]
        if curr["end_seconds"] != nxt["start_seconds"]:
            gap = nxt["start_seconds"] - curr["end_seconds"]
            if abs(gap) > 5:
                fixes.append(
                    f"  🔧 シーン{curr['scene_index']}→{nxt['scene_index']}: "
                    f"ギャップ{gap:+.0f}s → 連続化"
                )
            curr["end_seconds"] = nxt["start_seconds"]

    # ステップ5: 最終シーンが動画末尾に到達していなければ延長
    if sorted_cards:
        last = sorted_cards[-1]
        if last["end_seconds"] < dur * 0.9:
            fixes.append(
                f"  🔧 最終シーン{last['scene_index']}: "
                f"end {last['end_seconds']:.0f}s → {dur:.0f}s（動画末尾まで延長）"
            )
            last["end_seconds"] = dur

    # ステップ6: duration_secondsを再計算
    for card in cards:
        card["duration_seconds"] = card["end_seconds"] - card["start_seconds"]

    # ステップ7: 0秒以下のシーンを除外（エラーではなく除外）
    zero_cards = [c for c in cards if c["duration_seconds"] <= 0]
    if zero_cards:
        for c in zero_cards:
            idx = c.get("scene_index", 0)
            fixes.append(f"  🗑️ シーン{idx}: 0秒シーン（start={c['start_seconds']:.0f}s, end={c['end_seconds']:.0f}s）→ 除外")
        valid = [c for c in cards if c["duration_seconds"] > 0]
        fixes.append(f"  📋 {len(zero_cards)}シーンを0秒で除外（{len(valid)}シーン残存）")
        cards.clear()
        cards.extend(valid)

    # ステップ8: 45秒超過シーンの警告
    for card in cards:
        if card["duration_seconds"] > 45:
            idx = card.get("scene_index", 0)
            fixes.append(f"  ⚠️ シーン{idx}: {card['duration_seconds']:.0f}秒（45秒超過）")

    # ステップ9: scene_indexを振り直す（除外で歯抜けになった場合）
    for i, card in enumerate(sorted(cards, key=lambda c: c.get("scene_index", 0)), 1):
        card["scene_index"] = i

    return fixes, errors


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# メイン処理
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def process_video(conn, video_info: dict, dry_run: bool = False) -> list[dict] | None:
    """1本の動画を処理してscene_cardsを返す"""
    slug = video_info["slug"]
    series = video_info["series"]
    asset_id = video_info["asset_id"]
    title = video_info["title"]

    print(f"\n{'='*60}")
    print(f"📹 {title}")
    print(f"   slug={slug}  series={series}")
    print(f"{'='*60}")

    # Gemini分析を取得
    gemini = get_gemini_analysis(conn, asset_id)
    if not gemini:
        print(f"  ⚠️ Gemini分析が見つかりません。スキップ。")
        return None

    # 動画長を取得
    est_length = get_estimated_video_length(conn, asset_id)
    print(f"  動画長: {est_length:.0f}秒 ({est_length/60:.1f}分)")

    # 最低シーン数を計算（動画長 ÷ 35秒）
    # ÷45だと短い動画で粒度が粗くなるため、÷35で基準を厳しくする
    min_scenes = math.ceil(est_length / 35)

    # プロンプトを構築
    prompt = SCENE_CARD_EXTRACTION_PROMPT.format(
        video_title=title,
        slug=slug,
        series=series,
        estimated_length_seconds=est_length,
        min_scenes=min_scenes,
        gemini_analysis=gemini,
    )

    if dry_run:
        print(f"  [ドライラン] プロンプト長: {len(prompt):,}文字")
        print(f"  [ドライラン] Gemini分析長: {len(gemini):,}文字")
        print(f"  [ドライラン] API呼び出しはスキップ")
        return None

    # API呼び出し
    print(f"  🤖 Anthropic API ({MODEL}) に送信中...")
    start_time = time.time()
    try:
        response_text = call_anthropic_api(SCENE_CARD_EXTRACTION_SYSTEM, prompt)
    except Exception as e:
        print(f"  ❌ API呼び出しエラー: {e}")
        return None
    elapsed = time.time() - start_time
    print(f"  ✅ API応答受信 ({elapsed:.1f}秒)")

    # JSONパース
    try:
        raw_cards = parse_json_response(response_text)
    except json.JSONDecodeError as e:
        print(f"  ❌ JSONパースエラー: {e}")
        print(f"  応答テキスト（最初の500文字）: {response_text[:500]}")
        return None

    if not isinstance(raw_cards, list):
        print(f"  ❌ 応答がJSON配列ではありません: {type(raw_cards)}")
        return None

    print(f"  📋 {len(raw_cards)}イベントを抽出")

    # タイムスタンプ異常を自動修正
    ts_fixes, ts_errors = auto_fix_timestamps(raw_cards, est_length)
    if ts_fixes:
        print(f"  ⏱️ タイムスタンプ修正/警告 {len(ts_fixes)}件:")
        for fix in ts_fixes:
            print(f"    {fix}")
    if ts_errors:
        print(f"  ❌ タイムスタンプ致命的エラー {len(ts_errors)}件（データをリジェクト）:")
        for err in ts_errors:
            print(f"    {err}")
        print(f"  → プロンプトの制約が守られていません。このデータは保存しません。")
        return None

    # バリデーション + 維持率付与
    valid_enums = {
        "scene_type": SCENE_TYPES,
        "comedy_type": COMEDY_TYPES,
        "emotion": EMOTION_TYPES,
        "narrative_role": NARRATIVE_ROLES,
    }

    now_iso = datetime.now(timezone(timedelta(hours=9))).isoformat()
    final_cards = []
    scene_count = len(raw_cards)

    for card in raw_cards:
        idx = card.get("scene_index", 0)

        # v0互換: comedy_type → primary_comedy_type への変換
        if "comedy_type" in card and "primary_comedy_type" not in card:
            card["primary_comedy_type"] = card.pop("comedy_type")
        if "secondary_comedy_type" not in card:
            card["secondary_comedy_type"] = "なし"

        # enum外の値を自動修正
        fixes = auto_fix_enum(card, valid_enums)
        if fixes:
            print(f"  🔧 シーン{idx}のenum値を自動修正:")
            for fix in fixes:
                print(f"    {fix}")

        # バリデーション
        errors = validate_scene_card(card, valid_enums)
        if errors:
            print(f"  ⚠️ シーン{idx}にバリデーションエラー:")
            for err in errors:
                print(f"    {err}")

        # 維持率データを付与
        start_sec = card.get("start_seconds", 0)
        end_sec = card.get("end_seconds", 0)
        retention = get_retention_for_range(conn, asset_id, est_length, start_sec, end_sec)

        # 最終的なscene_card v1を構築
        final_card = {
            "scene_id": f"{slug}_s{idx:02d}",
            "asset_id": asset_id,
            "video_title": title,
            "scene_index": idx,
            "scene_count": scene_count,
            "start_seconds": start_sec,
            "end_seconds": end_sec,
            "duration_seconds": end_sec - start_sec,
            "retention_start_pct": retention["start"],
            "retention_end_pct": retention["end"],
            "retention_avg_pct": retention["avg"],
            "retention_delta_pct": retention["delta"],
            "series": series,
            "scene_type": card.get("scene_type", "設定_状況説明"),
            "primary_comedy_type": card.get("primary_comedy_type", "なし"),
            "secondary_comedy_type": card.get("secondary_comedy_type", "なし"),
            "energy": min(max(card.get("energy", 3), 1), 5),
            "emotion": card.get("emotion", "コミカル"),
            "narrative_role": card.get("narrative_role", "setup"),
            "one_line": card.get("one_line", "")[:60],
            "key_dialogue": card.get("key_dialogue", ""),
            "comedy_mechanism": card.get("comedy_mechanism", ""),
            "characters": card.get("characters", []),
            "scene_driver": card.get("scene_driver", ""),
            "tags": card.get("tags", []),
            "micro_hotspots": card.get("micro_hotspots", []),
            "classification_confidence": card.get("classification_confidence", 1.0),
            "source_model": MODEL,
            "ontology_version": ONTOLOGY_VERSION,
            "scene_card_version": SCENE_CARD_VERSION,
            "generated_at": now_iso,
        }
        final_cards.append(final_card)

        # 確認出力
        sign = "+" if retention["delta"] >= 0 else ""
        dur = end_sec - start_sec
        conf = card.get("classification_confidence", 1.0)
        pct = card.get("primary_comedy_type", "なし")
        print(f"  ✅ {final_card['scene_id']}: {final_card['one_line'][:30]}... "
              f"({dur:.0f}s, {pct}, 維持率{retention['avg']}% {sign}{retention['delta']}%, conf={conf:.2f})")

    return final_cards


def main():
    parser = argparse.ArgumentParser(description="scene_card v1 自動抽出（Opus + イベント単位細分化）")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--test", action="store_true",
                       help="3本テスト（conbini, spy, smashbros）")
    group.add_argument("--slug", type=str,
                       help="カンマ区切りのslug（例: police,conbini）")
    group.add_argument("--all", action="store_true",
                       help="全動画実行")
    parser.add_argument("--dry-run", action="store_true",
                        help="API呼び出しなし（プロンプト確認用）")
    parser.add_argument("--model", type=str, default=MODEL,
                        help=f"使用モデル（デフォルト: {MODEL}）")

    args = parser.parse_args()

    if args.model != MODEL:
        _override_model(args.model)

    conn = get_db()

    videos = get_video_info(conn, slug=args.slug, all_videos=args.all, test=args.test)
    if not videos:
        print("❌ 対象動画が見つかりません")
        sys.exit(1)

    print(f"\n🎬 scene_card v1 抽出 (モデル: {MODEL})")
    print(f"   処理対象: {len(videos)}本")
    for v in videos:
        print(f"  - {v['slug']}: {v['title'][:40]}")

    # 既存のscene_cardsを確認
    cur = conn.cursor()
    for v in videos:
        cur.execute("SELECT COUNT(*) FROM scene_cards WHERE asset_id = ?", (v["asset_id"],))
        count = cur.fetchone()[0]
        if count > 0:
            print(f"  ⚠️ {v['slug']}: 既に{count}件のscene_cardあり（上書きされます）")

    # 処理開始
    total_cards = 0
    success_count = 0
    fail_count = 0

    for i, video in enumerate(videos, 1):
        print(f"\n[{i}/{len(videos)}]")
        cards = process_video(conn, video, dry_run=args.dry_run)

        if cards:
            save_scene_cards(conn, cards)
            total_cards += len(cards)
            success_count += 1
            print(f"  💾 {len(cards)}件をDBに保存")
        else:
            if not args.dry_run:
                fail_count += 1

        # レート制限対策
        if i < len(videos) and not args.dry_run:
            time.sleep(RATE_LIMIT_DELAY)

    # 結果サマリー
    print(f"\n{'='*60}")
    print(f"📊 処理完了サマリー")
    print(f"{'='*60}")
    if args.dry_run:
        print(f"  [ドライラン] API呼び出しなし")
    else:
        print(f"  成功: {success_count}/{len(videos)}本")
        print(f"  失敗: {fail_count}/{len(videos)}本")
        print(f"  保存scene_card数: {total_cards}件")
        if success_count > 0:
            print(f"  平均シーン数: {total_cards / success_count:.1f}件/本")

    cur.execute("SELECT COUNT(*) FROM scene_cards")
    db_total = cur.fetchone()[0]
    print(f"  DB内scene_card総数: {db_total}件")

    conn.close()


if __name__ == "__main__":
    main()
