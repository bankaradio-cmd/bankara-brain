#!/usr/bin/env python3
"""
層2: knowledge_object 自動生成スクリプト

scene_cardsの集合 → Anthropic API → パターン・法則の知識オブジェクト → DB保存

前提: 層1（scene_card抽出）が完了していること

使い方:
  # 全8カテゴリを生成（推奨: scene_card抽出後に実行）
  python scripts/generate_knowledge_objects.py --all

  # 特定カテゴリだけ
  python scripts/generate_knowledge_objects.py --type comedy_patterns_channel

  # チャンネル全体のカテゴリだけ
  python scripts/generate_knowledge_objects.py --scope channel

  # シリーズ別カテゴリだけ
  python scripts/generate_knowledge_objects.py --scope series

  # ドライラン（API呼び出しなし、プロンプト確認用）
  python scripts/generate_knowledge_objects.py --all --dry-run

必要な環境変数:
  ANTHROPIC_API_KEY=sk-ant-xxxxx
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

# プロジェクトルートをパスに追加
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# .env からAPIキーを読み込む（override=True で既存環境変数より .env を優先）
from dotenv import load_dotenv
load_dotenv(str(PROJECT_ROOT / ".env"), override=True)

from bankara_brain.analysis.prompts import (
    KNOWLEDGE_OBJECT_SYSTEM,
    KNOWLEDGE_OBJECT_PROMPT,
    KNOWLEDGE_OBJECT_CHANNEL_PROMPT,
)
from bankara_brain.analysis.schema_design import KNOWLEDGE_OBJECT_TYPES

DB_PATH = PROJECT_ROOT / "bankara_brain.db"

# Anthropic API の設定
MODEL = "claude-opus-4-20250514"
MAX_TOKENS = 16384
RATE_LIMIT_DELAY = 2.0  # API呼び出し間の待機秒数


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DB操作
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def get_db():
    """DB接続を取得"""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def load_all_scene_cards(conn) -> list[dict]:
    """全scene_cardsをDBから読み込む"""
    cur = conn.cursor()
    cur.execute("""
        SELECT scene_id, asset_id, video_title, scene_index, scene_count,
               start_seconds, end_seconds, duration_seconds,
               retention_start_pct, retention_end_pct, retention_avg_pct, retention_delta_pct,
               series, scene_type, comedy_type, energy, emotion, narrative_role,
               one_line, key_dialogue, comedy_mechanism,
               characters, scene_driver, tags,
               primary_comedy_type, secondary_comedy_type,
               classification_confidence, micro_hotspots
        FROM scene_cards
        ORDER BY video_title, scene_index
    """)
    rows = cur.fetchall()
    cards = []
    for row in rows:
        card = dict(row)
        # JSON文字列をパース
        card["characters"] = json.loads(card["characters"]) if card["characters"] else []
        card["tags"] = json.loads(card["tags"]) if card["tags"] else []
        card["micro_hotspots"] = json.loads(card["micro_hotspots"]) if card["micro_hotspots"] else []
        cards.append(card)
    return cards


def filter_scene_cards_by_scope(cards: list[dict], scope: str) -> list[dict]:
    """スコープに基づいてscene_cardsをフィルタリング"""
    if scope == "channel":
        # チャンネル全体 → 全カード
        return cards
    elif scope.startswith("series:"):
        series_name = scope.split(":", 1)[1]
        return [c for c in cards if c["series"] == series_name]
    return cards


def save_knowledge_object(conn, obj: dict):
    """knowledge_objectをDBに保存"""
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO knowledge_objects (
            object_id, object_type, scope, title, content,
            source_scene_ids, source_claim_ids, version,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, '[]', ?, CURRENT_TIMESTAMP)
    """, (
        obj["object_id"],
        obj["object_type"],
        obj["scope"],
        obj["title"],
        json.dumps(obj["content"], ensure_ascii=False, indent=2),
        json.dumps(obj["source_scene_ids"], ensure_ascii=False),
        obj.get("version", 1),
    ))
    conn.commit()


def get_existing_knowledge_objects(conn) -> dict[str, dict]:
    """既存のknowledge_objectsを取得"""
    cur = conn.cursor()
    cur.execute("SELECT object_id, object_type, version FROM knowledge_objects")
    return {row["object_id"]: dict(row) for row in cur.fetchall()}


def load_series_knowledge_objects(conn) -> list[dict]:
    """シリーズ別knowledge_objectsをDBから読み込む（2段階方式用）

    チャンネル全体カテゴリの入力として使う。
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT object_id, object_type, scope, title, content
        FROM knowledge_objects
        WHERE scope LIKE 'series:%'
        ORDER BY object_type
    """)
    results = []
    for row in cur.fetchall():
        results.append({
            "object_type": row["object_type"],
            "scope": row["scope"],
            "title": row["title"],
            "patterns": json.loads(row["content"]),
        })
    return results


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# API呼び出し
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def call_anthropic_api(system: str, prompt: str) -> str:
    """Anthropic APIをストリーミングで呼び出してテキストを返す

    Opusモデルは応答に10分以上かかることがあるため、
    ストリーミングが必須（非ストリーミングだとタイムアウトエラー）。
    """
    try:
        import anthropic
    except ImportError:
        print("  anthropic パッケージが必要です: pip install anthropic")
        sys.exit(1)

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("  ANTHROPIC_API_KEY 環境変数を設定してください")
        sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key)

    # ストリーミングでテキストを収集
    collected_text = []
    with client.messages.stream(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=system,
        messages=[{"role": "user", "content": prompt}],
    ) as stream:
        for text in stream.text_stream:
            collected_text.append(text)

    return "".join(collected_text)


def parse_json_response(text: str) -> dict:
    """API応答からJSONを抽出してパース

    Opusは応答の前後に説明文を付けることがあるため、
    ```json ... ``` ブロックの中身を探す。見つからなければ
    最外側の { ... } を抽出する。
    """
    cleaned = text.strip()

    # ```json ... ``` ブロックを探す（テキスト中のどこにあっても対応）
    import re
    code_block = re.search(r"```(?:json)?\s*\n(.*?)```", cleaned, re.DOTALL)
    if code_block:
        return json.loads(code_block.group(1).strip())

    # コードブロックがない場合、最外側の { ... } を抽出
    brace_start = cleaned.find("{")
    brace_end = cleaned.rfind("}")
    if brace_start != -1 and brace_end != -1 and brace_end > brace_start:
        return json.loads(cleaned[brace_start:brace_end + 1])

    # どちらも見つからなければそのままパース（エラーになる）
    return json.loads(cleaned)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# scene_cards を AI 用の簡潔 JSON に変換
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def compact_scene_cards_for_prompt(cards: list[dict]) -> str:
    """scene_cardsをプロンプト用のコンパクトJSONに変換

    全フィールドを送るとトークン数が膨大になるため、
    パターン抽出に必要なフィールドだけに絞る。
    """
    compact = []
    for c in cards:
        entry = {
            "scene_id": c["scene_id"],
            "video_title": c["video_title"],
            "series": c["series"],
            "scene_type": c["scene_type"],
            "primary_comedy_type": c["primary_comedy_type"],
            "secondary_comedy_type": c["secondary_comedy_type"],
            "classification_confidence": c["classification_confidence"],
            "energy": c["energy"],
            "emotion": c["emotion"],
            "narrative_role": c["narrative_role"],
            "retention_avg_pct": c["retention_avg_pct"],
            "retention_delta_pct": c["retention_delta_pct"],
            "one_line": c["one_line"],
            "comedy_mechanism": c["comedy_mechanism"],
            "characters": c["characters"],
            "scene_driver": c["scene_driver"],
            "tags": c["tags"],
        }
        # micro_hotspots があれば追加（トークン節約のため空なら省略）
        if c.get("micro_hotspots"):
            entry["micro_hotspots"] = c["micro_hotspots"]
        compact.append(entry)
    return json.dumps(compact, ensure_ascii=False, indent=1)


def minimal_scene_cards_summary(cards: list[dict]) -> str:
    """チャンネル全体用の最小限scene_card要約

    2段階方式で使用。scene_idの参照用に最低限のフィールドだけ含む。
    これによりトークン数を大幅に削減（942K→158K文字）。
    """
    minimal = []
    for c in cards:
        minimal.append({
            "id": c["scene_id"],
            "sr": c["series"],
            "st": c["scene_type"],
            "ct": c["primary_comedy_type"],
            "ra": c["retention_avg_pct"],
            "ol": c["one_line"],
        })
    return json.dumps(minimal, ensure_ascii=False, indent=1)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# メイン処理
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def process_knowledge_object_type(
    obj_type: str,
    obj_config: dict,
    all_cards: list[dict],
    existing: dict[str, dict],
    conn=None,
    dry_run: bool = False,
) -> dict | None:
    """1つのknowledge_objectタイプを処理"""
    scope = obj_config["scope"]
    description = obj_config["description"]

    print(f"\n{'='*60}")
    print(f"📚 {obj_type}")
    print(f"   {description}")
    print(f"   スコープ: {scope}")
    print(f"{'='*60}")

    # スコープに基づいてscene_cardsをフィルタリング
    filtered = filter_scene_cards_by_scope(all_cards, scope)
    print(f"  対象scene_cards: {len(filtered)}件 / 全{len(all_cards)}件")

    if len(filtered) < 4:
        print(f"  ⚠️ scene_cardsが少なすぎます（{len(filtered)}件）。スキップ。")
        return None

    # 既存チェック
    object_id = f"ko_{obj_type}"
    if object_id in existing:
        old_version = existing[object_id]["version"]
        new_version = old_version + 1
        print(f"  📝 既存あり（version={old_version}）→ version={new_version}に更新")
    else:
        new_version = 1
        print(f"  🆕 新規作成（version=1）")

    # プロンプト構築（チャンネル全体 vs シリーズ別で異なる方式）
    if scope == "channel" and conn is not None:
        # 2段階方式: シリーズ別知識 + 最小限scene_card要約
        series_kos = load_series_knowledge_objects(conn)
        if not series_kos:
            print(f"  ⚠️ シリーズ別知識オブジェクトが見つかりません。先に --scope series を実行してください。")
            return None

        series_patterns_json = json.dumps(series_kos, ensure_ascii=False, indent=1)
        scene_cards_summary_json = minimal_scene_cards_summary(all_cards)
        prompt = KNOWLEDGE_OBJECT_CHANNEL_PROMPT.format(
            object_type=description,
            series_patterns_json=series_patterns_json,
            scene_cards_summary_json=scene_cards_summary_json,
        )
        print(f"  📊 2段階方式: シリーズ別知識{len(series_kos)}件 + scene_card要約{len(all_cards)}件")
    else:
        # 通常方式: scene_cardsを直接送信
        scene_cards_json = compact_scene_cards_for_prompt(filtered)
        prompt = KNOWLEDGE_OBJECT_PROMPT.format(
            object_type=description,
            scope=scope,
            scene_cards_json=scene_cards_json,
        )

    if dry_run:
        print(f"  [ドライラン] プロンプト長: {len(prompt):,}文字")
        print(f"  [ドライラン] 推定トークン数: ~{len(prompt)//2:,}")
        print(f"  [ドライラン] API呼び出しはスキップ")
        return None

    # API呼び出し
    print(f"  🤖 Anthropic API ({MODEL}) に送信中...")
    start_time = time.time()
    try:
        response_text = call_anthropic_api(KNOWLEDGE_OBJECT_SYSTEM, prompt)
    except Exception as e:
        print(f"  ❌ API呼び出しエラー: {e}")
        return None
    elapsed = time.time() - start_time
    print(f"  ✅ API応答受信 ({elapsed:.1f}秒)")

    # JSONパース
    try:
        content = parse_json_response(response_text)
    except json.JSONDecodeError as e:
        print(f"  ❌ JSONパースエラー: {e}")
        print(f"  応答テキスト（最初の500文字）: {response_text[:500]}")
        return None

    # バリデーション
    patterns = content.get("patterns", [])
    summary = content.get("summary", "")
    print(f"  📋 {len(patterns)}個のパターンを抽出")

    # 根拠が2件未満のパターンを除外
    valid_patterns = [p for p in patterns if p.get("support_count", 0) >= 2]
    if len(valid_patterns) < len(patterns):
        removed = len(patterns) - len(valid_patterns)
        print(f"  🗑️ 根拠2件未満の{removed}パターンを除外")

    # 参照scene_idsを収集
    all_evidence_ids = set()
    for p in valid_patterns:
        for sid in p.get("evidence_scene_ids", []):
            all_evidence_ids.add(sid)

    # パターンのプレビュー表示
    for p in valid_patterns[:3]:
        evidence_count = p.get("support_count", len(p.get("evidence_scene_ids", [])))
        print(f"  📎 [{evidence_count}件] {p['pattern'][:60]}...")

    if len(valid_patterns) > 3:
        print(f"  ... 他{len(valid_patterns) - 3}パターン")

    # knowledge_objectを構築
    result = {
        "object_id": object_id,
        "object_type": obj_type,
        "scope": scope,
        "title": description,
        "content": {
            "patterns": valid_patterns,
            "summary": summary,
        },
        "source_scene_ids": sorted(all_evidence_ids),
        "version": new_version,
    }

    return result


def main():
    global MODEL

    parser = argparse.ArgumentParser(description="knowledge_object自動生成（層2）")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--all", action="store_true",
                       help="全8カテゴリを生成")
    group.add_argument("--type", type=str,
                       help="特定カテゴリだけ（例: comedy_patterns_channel）")
    group.add_argument("--scope", type=str, choices=["channel", "series"],
                       help="チャンネル全体またはシリーズ別だけ")
    parser.add_argument("--dry-run", action="store_true",
                        help="API呼び出しなし（プロンプト確認用）")
    parser.add_argument("--model", type=str, default=MODEL,
                        help=f"使用モデル（デフォルト: {MODEL}）")

    args = parser.parse_args()

    # モデルオーバーライド
    if args.model != MODEL:
        MODEL = args.model

    conn = get_db()

    # scene_cardsを読み込み
    all_cards = load_all_scene_cards(conn)
    if not all_cards:
        print("❌ scene_cardsが見つかりません。先に extract_scene_cards.py を実行してください。")
        sys.exit(1)

    print(f"\n📊 DB内のscene_cards: {len(all_cards)}件")

    # 動画数を確認
    unique_videos = set(c["asset_id"] for c in all_cards)
    print(f"   動画数: {len(unique_videos)}本")

    # シリーズ別の件数
    series_counts = {}
    for c in all_cards:
        s = c["series"]
        series_counts[s] = series_counts.get(s, 0) + 1
    for s, cnt in sorted(series_counts.items(), key=lambda x: -x[1]):
        print(f"   {s}: {cnt}件")

    # 対象カテゴリを決定
    if args.type:
        if args.type not in KNOWLEDGE_OBJECT_TYPES:
            print(f"❌ 不明なカテゴリ: {args.type}")
            print(f"   有効な値: {', '.join(KNOWLEDGE_OBJECT_TYPES.keys())}")
            sys.exit(1)
        target_types = {args.type: KNOWLEDGE_OBJECT_TYPES[args.type]}
    elif args.scope:
        if args.scope == "channel":
            target_types = {k: v for k, v in KNOWLEDGE_OBJECT_TYPES.items()
                          if v["scope"] == "channel"}
        else:
            target_types = {k: v for k, v in KNOWLEDGE_OBJECT_TYPES.items()
                          if v["scope"].startswith("series:")}
    else:
        target_types = KNOWLEDGE_OBJECT_TYPES

    print(f"\n🎯 処理対象: {len(target_types)}カテゴリ")
    for name, cfg in target_types.items():
        print(f"   - {name}: {cfg['description']}")

    # 既存データ確認
    existing = get_existing_knowledge_objects(conn)
    if existing:
        print(f"\n📝 既存knowledge_objects: {len(existing)}件（上書きされます）")

    # 処理開始
    success_count = 0
    fail_count = 0

    for i, (obj_type, obj_config) in enumerate(target_types.items(), 1):
        print(f"\n[{i}/{len(target_types)}]")

        result = process_knowledge_object_type(
            obj_type, obj_config, all_cards, existing, conn=conn, dry_run=args.dry_run
        )

        if result:
            save_knowledge_object(conn, result)
            patterns_count = len(result["content"]["patterns"])
            evidence_count = len(result["source_scene_ids"])
            success_count += 1
            print(f"  💾 保存完了: {patterns_count}パターン, {evidence_count}件のscene_id参照")
        else:
            if not args.dry_run:
                fail_count += 1

        # レート制限対策
        if i < len(target_types) and not args.dry_run:
            time.sleep(RATE_LIMIT_DELAY)

    # 結果サマリー
    print(f"\n{'='*60}")
    print(f"📊 処理完了サマリー")
    print(f"{'='*60}")
    if args.dry_run:
        print(f"  [ドライラン] API呼び出しなし")
    else:
        print(f"  成功: {success_count}/{len(target_types)}カテゴリ")
        print(f"  失敗: {fail_count}/{len(target_types)}カテゴリ")

    # DB内の総数を確認
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM knowledge_objects")
    db_total = cur.fetchone()[0]
    print(f"  DB内knowledge_objects総数: {db_total}件")

    conn.close()


if __name__ == "__main__":
    main()
