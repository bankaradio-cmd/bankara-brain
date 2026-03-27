#!/usr/bin/env python3
"""
Layer 4: 深層分析 v3 生成スクリプト

入力: scene_cards v1 + Gemini分析 + 維持率 + コメント
出力: brain_deep_analysis.opus_analysis の更新（version='v3'またはv2を上書き）

改善点（v2 → v3）:
  1. scene_card v1のcomedy_mechanism / micro_hotspotsを参照して分析を深める
  2. 各シーンにprimary_comedy_type / secondary_comedy_typeを明記
  3. classification_confidenceが0.8未満のシーンは「なぜ分類が難しいか」を深掘り
  4. 分析の最後に「この動画を3行で表すサマリー」を追加
  5. 既存のopus_analysisを加筆（上書きではなく発展）

使い方:
  # テスト3本
  python scripts/generate_deep_analysis_v3.py --test

  # 特定動画
  python scripts/generate_deep_analysis_v3.py --title "コンビニ"

  # 簡易版のみ（5000字未満）を全て再分析
  python scripts/generate_deep_analysis_v3.py --upgrade-short

  # ドライラン
  python scripts/generate_deep_analysis_v3.py --title "コンビニ" --dry-run

必要な環境変数:
  ANTHROPIC_API_KEY=sk-ant-xxxxx
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
import time
from pathlib import Path

# プロジェクトルート
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(str(PROJECT_ROOT / ".env"), override=True)

DB_PATH = PROJECT_ROOT / "bankara_brain.db"

# Anthropic API 設定
MODEL = "claude-opus-4-20250514"
MAX_TOKENS = 32000
RATE_LIMIT_DELAY = 2.0


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DB操作
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def load_video_data(conn, asset_id: str) -> dict:
    """1本の動画の全データを集約して返す"""
    cur = conn.cursor()

    # 基本情報
    cur.execute("SELECT * FROM assets WHERE id = ?", (asset_id,))
    asset = dict(cur.fetchone())

    # scene_cards v1
    cur.execute("""
        SELECT scene_id, scene_index, scene_count,
               start_seconds, end_seconds, duration_seconds,
               retention_start_pct, retention_end_pct, retention_avg_pct, retention_delta_pct,
               series, scene_type, comedy_type, energy, emotion, narrative_role,
               one_line, key_dialogue, comedy_mechanism,
               characters, scene_driver, tags,
               primary_comedy_type, secondary_comedy_type,
               classification_confidence, micro_hotspots
        FROM scene_cards WHERE asset_id = ? ORDER BY scene_index
    """, (asset_id,))
    scene_cards = []
    for row in cur.fetchall():
        card = dict(row)
        # JSON fields
        for field in ('characters', 'tags', 'micro_hotspots'):
            if card[field]:
                try:
                    card[field] = json.loads(card[field])
                except (json.JSONDecodeError, TypeError):
                    pass
        scene_cards.append(card)

    # Gemini分析
    cur.execute("""
        SELECT gemini_analysis, opus_analysis
        FROM brain_deep_analysis WHERE asset_id = ? AND version = 'v2'
    """, (asset_id,))
    analysis_row = cur.fetchone()
    gemini_analysis = analysis_row['gemini_analysis'] if analysis_row else ""
    existing_opus = analysis_row['opus_analysis'] if analysis_row else ""

    # 維持率データ
    cur.execute("""
        SELECT elapsed_video_time_ratio, audience_watch_ratio, relative_retention_performance
        FROM youtube_retention_points WHERE asset_id = ?
        ORDER BY elapsed_video_time_ratio
    """, (asset_id,))
    retention_points = [dict(r) for r in cur.fetchall()]

    # コメント（いいね順上位50件）
    cur.execute("""
        SELECT text_original, like_count, published_at
        FROM youtube_comments WHERE asset_id = ?
        ORDER BY like_count DESC LIMIT 50
    """, (asset_id,))
    comments = [dict(r) for r in cur.fetchall()]

    # YouTube metrics (日次データなのでSUMで累計を算出)
    cur.execute("""
        SELECT SUM(views) as views, SUM(likes) as likes
        FROM youtube_daily_metrics WHERE asset_id = ?
    """, (asset_id,))
    metrics_row = cur.fetchone()
    metrics = dict(metrics_row) if metrics_row else {}

    return {
        'asset': asset,
        'scene_cards': scene_cards,
        'gemini_analysis': gemini_analysis,
        'existing_opus': existing_opus,
        'retention_points': retention_points,
        'comments': comments,
        'metrics': metrics,
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# プロンプト構築
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

SYSTEM_PROMPT = """あなたはYouTubeコメディ動画の脚本分析の専門家です。
バンカラジオというチャンネルの動画を深層分析します。

## 分析の目的
新しい脚本を書くときに「過去のどの手法が効いたか」を参照できる知識ベースを作ること。
つまり「何が起きたか」の記述ではなく「なぜ効いたか」「次にどう使うか」の教訓を導出すること。

## 出力フォーマット
以下の構造で分析を出力してください。情報量を最大化すること。省略・短縮しない。

### ヘッダー
# 深層分析 v3：{タイトル}
# {再生回数} / 平均視聴維持率{X}% / {動画時間}

### シーン別分析（全シーンを記述。1シーンも省略しない）
各シーンについて以下の全項目を記述：

**【内容】** 何が起きているかを、主要なセリフを引用しながら詳しく記述する。
  登場キャラのセリフは「」で引用し、行動描写も入れて読むだけでシーンが頭に浮かぶレベルで書く。
  1-3行程度。
**【視聴維持率】** 開始%→終了%、deltaの数値。なぜその変化が起きたかの分析も。
**【笑いの分類】** primary_comedy_type / secondary_comedy_type（確信度X%）
  - 確信度80%未満の場合：なぜ分類が難しいかを1-2文で説明
**【コメディのメカニズム】** comedy_mechanismを発展させた深い分析。
  「なぜ面白いのか」を認知科学的・構造的に分析。3行以上書く。
  単なる「ギャップが面白い」ではなく、具体的にどんな期待がどう裏切られるかを説明。
**【マイクロホットスポット】** micro_hotspotsの中で最も重要なものを分析（コメントとの対応も）
**【コメント反応】** そのシーンに対応するコメントがあれば引用（いいね数付き）
**【脚本上の役割】** 物語全体の中でこのシーンが果たす構造的機能
**【重要】** （該当シーンのみ）他の動画との共通パターン、シリーズ全体に通じる法則、特筆すべきデータの発見があれば記述
**【教訓】** 次の脚本に使える具体的な法則。数値（秒数、%など）を必ず含める（1シーン1教訓）

### 伏線構造マップ
動画内のフリ→回収の関係を表形式で整理：
| 伏線（フリ） | 回収（オチ） | 効果 |

### 視聴維持率×コンテンツ相関表
時間・維持率・コンテンツ内容を細かく対応させた表：
| 時間 | 維持率 | コンテンツ | 分析 |

### 全体リズム分析
動画全体のエネルギー推移、テンポ変化、維持率の山と谷を表形式で

### コメント傾向分析
コメントから読み取れる視聴者の反応パターンを表形式で（カテゴリ、代表コメント、いいね数、心理分析）

### 脚本テンプレート
この動画の構造を抽象化したテンプレート。各パートに推奨秒数を付ける。他のテーマに転用可能な形にする。

### この動画固有の強み・弱み
具体的なデータを引用して箇条書き。

### 3行サマリー
以下3つを各1行で：
1. この動画の「最強の武器」（最も維持率に貢献した手法）
2. この動画の「最大の弱点」（最も離脱を招いた箇所とその原因）
3. 次の脚本への「持ち帰り教訓」（最も汎用的な1つの法則）

## 重要な注意
- 情報量を最大化する。文字数を気にせず詳しく書く
- 数値は全てデータに基づく。推測しない
- 「面白い」「良い」などの主観は使わない。代わりに「維持率がX%改善」「コメントがY件」など客観指標で語る
- 教訓は具体的に。「テンポが大事」ではなく「挫折シーンは26秒以内」のように数値付きで
- comedy_mechanismフィールドの内容を深堀りして拡張する。そのまま転記しない
- 他の動画（既存分析にシリーズ比較がある場合）との共通パターンに言及する
- シーンの【内容】は必ずセリフ引用を含む詳細な記述にする。1行で済ませない
- **制作手法（CG、実写、エフェクトの種類など）について推測で書かない。** 映像がどう作られたかはデータからは分からないので、「〇〇の演出」「〇〇の映像効果」のように結果として画面に何が映っているかだけを記述する。「CGではなく実写」「手描き」など制作方法を断定しない
- コメントやGemini分析で元ネタ・パロディへの言及があれば必ず取り上げる。パロディの指摘は脚本分析において重要な情報
"""


def build_user_prompt(data: dict) -> str:
    """ユーザープロンプトを構築"""
    asset = data['asset']
    scene_cards = data['scene_cards']
    gemini_analysis = data['gemini_analysis']
    existing_opus = data['existing_opus']
    retention_points = data['retention_points']
    comments = data['comments']
    metrics = data['metrics']

    # 動画基本情報
    title = asset.get('title', asset.get('video_title', '不明'))
    views = metrics.get('views', '不明')
    total_duration = max(sc['end_seconds'] for sc in scene_cards) if scene_cards else 0
    minutes = int(total_duration // 60)
    seconds = int(total_duration % 60)

    # 平均維持率 (audience_watch_ratioは0-1の比率なので%に変換)
    if retention_points:
        ratios = [r['audience_watch_ratio'] for r in retention_points if r['audience_watch_ratio'] is not None]
        avg_ret = (sum(ratios) / len(ratios) * 100) if ratios else 0
    else:
        avg_ret = 0

    prompt_parts = []

    # ── 基本情報 ──
    prompt_parts.append(f"""## 動画情報
- タイトル: {title}
- 再生回数: {views}
- 動画時間: {minutes}分{seconds}秒
- 平均維持率: {avg_ret:.1f}%
- シーン数: {len(scene_cards)}
""")

    # ── Scene Cards v1 ──
    prompt_parts.append("## Scene Cards（v1、全シーン）\n")
    for sc in scene_cards:
        ret_info = ""
        if sc['retention_avg_pct']:
            ret_info = f"維持率={sc['retention_avg_pct']:.1f}% delta={sc['retention_delta_pct']:+.1f}%"

        confidence_note = ""
        if sc['classification_confidence'] and sc['classification_confidence'] < 0.8:
            confidence_note = f" ⚠️確信度{sc['classification_confidence']:.0%}（要深堀り）"

        micro_str = ""
        if sc['micro_hotspots'] and isinstance(sc['micro_hotspots'], list) and len(sc['micro_hotspots']) > 0:
            hotspots = sc['micro_hotspots'][:3]
            micro_str = "\n    マイクロホットスポット: " + " / ".join(
                f"{h.get('event', '?')}({h.get('event_type', '?')})" for h in hotspots
            )

        prompt_parts.append(f"""### シーン{sc['scene_index']+1} [{sc['start_seconds']:.0f}-{sc['end_seconds']:.0f}s] ({sc['duration_seconds']:.0f}秒)
    種別: {sc['scene_type']} | E{sc['energy']} {sc['emotion']} | {sc['narrative_role']}
    {ret_info}
    笑い: {sc['primary_comedy_type']} / {sc['secondary_comedy_type']} (確信{sc['classification_confidence']:.0%}){confidence_note}
    一言: {sc['one_line']}
    セリフ: {sc.get('key_dialogue', 'なし')}
    笑いの仕組み: {sc['comedy_mechanism']}
    キャラ: {sc['characters']} ドライバー: {sc['scene_driver']}
    タグ: {sc['tags']}{micro_str}
""")

    # ── 維持率推移 ──
    if retention_points:
        prompt_parts.append("## 維持率推移（10%刻み）\n")
        for rp in retention_points:
            ratio_pct = rp['elapsed_video_time_ratio'] * 100
            if abs(ratio_pct % 10) < 1.5:  # 10%刻みに近いポイント
                watch_pct = (rp['audience_watch_ratio'] or 0) * 100
                rel_perf = rp['relative_retention_performance'] or 0
                prompt_parts.append(f"  {ratio_pct:.0f}%地点: 視聴{watch_pct:.1f}% (相対パフォーマンス{rel_perf:.2f})")
        prompt_parts.append("")

    # ── コメント上位 ──
    if comments:
        prompt_parts.append("## コメント（いいね順 上位30件）\n")
        for c in comments[:30]:
            text = c['text_original'].replace('\n', ' ')[:100]
            prompt_parts.append(f"  [{c['like_count']}いいね] {text}")
        prompt_parts.append("")

    # ── Gemini分析 ──
    if gemini_analysis:
        # 最長Gemini分析(≈11,000字)の2倍余裕。Opus 4は100万トークンなので十分
        g_text = gemini_analysis[:25000]
        prompt_parts.append(f"## Gemini 3.1 Pro 分析（参考）\n{g_text}\n")

    # ── 既存Opus分析（加筆元） ──
    if existing_opus and len(existing_opus) > 500:
        prompt_parts.append(f"""## 既存の深層分析（v2）— 加筆・改善元
以下は過去に生成された分析です。これを基に、scene_card v1の新フィールド（comedy_mechanism, micro_hotspots, primary/secondary_comedy_type, classification_confidence）を活用して、より深く具体的な分析に改善してください。

{existing_opus}
""")

    # ── 指示 ──
    prompt_parts.append("""## 指示
上記の全データを統合して、深層分析v3を生成してください。
- 全シーンを分析すること（省略しない）
- scene_card v1のcomedy_mechanismを発展させること
- classification_confidenceが0.8未満のシーンは「なぜ分類が難しいか」を説明
- 各シーンに【教訓】を必ず入れること
- 最後に3行サマリーを入れること
""")

    return "\n".join(prompt_parts)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# API呼び出し
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def call_anthropic_api(system: str, prompt: str, max_retries: int = 3) -> str:
    """Anthropic APIをストリーミングで呼び出す（リトライ付き）"""
    import anthropic

    client = anthropic.Anthropic()

    for attempt in range(1, max_retries + 1):
        print(f"  API呼び出し中... (プロンプト {len(prompt):,}文字)" + (f" [リトライ {attempt}/{max_retries}]" if attempt > 1 else ""))
        start_time = time.time()

        try:
            collected_text = []
            with client.messages.stream(
                model=MODEL,
                max_tokens=MAX_TOKENS,
                system=system,
                messages=[{"role": "user", "content": prompt}],
            ) as stream:
                for text in stream.text_stream:
                    collected_text.append(text)
                    # 進捗表示（2000文字ごと）
                    total = sum(len(t) for t in collected_text)
                    if total % 2000 < len(text):
                        elapsed = time.time() - start_time
                        print(f"    ... {total:,}文字生成 ({elapsed:.0f}秒)")

            result = "".join(collected_text)
            elapsed = time.time() - start_time
            print(f"  完了: {len(result):,}文字 ({elapsed:.0f}秒)")
            return result

        except (anthropic.APIStatusError, anthropic.APIConnectionError) as e:
            elapsed = time.time() - start_time
            print(f"  エラー ({elapsed:.0f}秒): {e}")
            if attempt < max_retries:
                wait = 10 * attempt
                print(f"  {wait}秒待ってリトライします...")
                time.sleep(wait)
            else:
                raise


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DB保存
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def save_analysis(conn, asset_id: str, analysis_text: str):
    """分析結果をDBに保存（v2のopus_analysisを更新）"""
    cur = conn.cursor()
    cur.execute("""
        UPDATE brain_deep_analysis
        SET opus_analysis = ?, updated_at = datetime('now')
        WHERE asset_id = ? AND version = 'v2'
    """, (analysis_text, asset_id))
    conn.commit()
    print(f"  DB保存完了 (asset_id={asset_id[:8]}...)")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# メイン処理
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def process_one_video(conn, asset_id: str, title: str, dry_run: bool = False) -> str:
    """1本の動画を深層分析"""
    print(f"\n{'='*60}")
    print(f"分析開始: {title}")
    print(f"{'='*60}")

    # データ集約
    data = load_video_data(conn, asset_id)
    scene_count = len(data['scene_cards'])
    comment_count = len(data['comments'])
    print(f"  データ: {scene_count}シーン, {comment_count}コメント, "
          f"Gemini{len(data['gemini_analysis']):,}字, 既存Opus{len(data['existing_opus']):,}字")

    # プロンプト構築
    prompt = build_user_prompt(data)
    print(f"  プロンプト: {len(prompt):,}文字 (≈{len(prompt)//2:,}トークン)")

    if dry_run:
        print(f"  [DRY RUN] API呼び出しをスキップ")
        # プロンプトの一部を表示
        print(f"\n--- プロンプト先頭1000文字 ---")
        print(prompt[:1000])
        return ""

    # API呼び出し
    result = call_anthropic_api(SYSTEM_PROMPT, prompt)

    # DB保存
    save_analysis(conn, asset_id, result)

    return result


def get_test_targets(conn) -> list[tuple[str, str]]:
    """テスト対象3本を取得"""
    cur = conn.cursor()
    cur.execute("""
        SELECT a.id, a.title
        FROM brain_deep_analysis bda
        JOIN assets a ON bda.asset_id = a.id
        WHERE a.title LIKE '%中華料理%'
           OR a.title LIKE '%強盗犯%'
           OR a.title LIKE '%医者%'
        ORDER BY a.title
    """)
    return [(row['id'], row['title']) for row in cur.fetchall()]


def get_short_analysis_targets(conn) -> list[tuple[str, str]]:
    """簡易版（5000字未満）の動画を全て取得"""
    cur = conn.cursor()
    cur.execute("""
        SELECT a.id, a.title
        FROM brain_deep_analysis bda
        JOIN assets a ON bda.asset_id = a.id
        WHERE LENGTH(bda.opus_analysis) < 5000
        ORDER BY LENGTH(bda.opus_analysis) ASC
    """)
    return [(row['id'], row['title']) for row in cur.fetchall()]


def main():
    parser = argparse.ArgumentParser(description="Layer 4: 深層分析 v3")
    parser.add_argument("--test", action="store_true", help="テスト3本を実行")
    parser.add_argument("--title", type=str, help="タイトルキーワードで指定")
    parser.add_argument("--upgrade-short", action="store_true", help="簡易版(5000字未満)を全て再分析")
    parser.add_argument("--dry-run", action="store_true", help="API呼び出しなし")
    parser.add_argument("--model", type=str, default=None, help="モデル指定")
    args = parser.parse_args()

    global MODEL
    if args.model:
        MODEL = args.model

    conn = get_db()

    if args.test:
        targets = get_test_targets(conn)
        print(f"テスト対象: {len(targets)}本")
    elif args.title:
        cur = conn.cursor()
        cur.execute("""
            SELECT a.id, a.title
            FROM brain_deep_analysis bda
            JOIN assets a ON bda.asset_id = a.id
            WHERE a.title LIKE ?
        """, (f"%{args.title}%",))
        targets = [(row['id'], row['title']) for row in cur.fetchall()]
        print(f"「{args.title}」に一致: {len(targets)}本")
    elif args.upgrade_short:
        targets = get_short_analysis_targets(conn)
        print(f"簡易版（5000字未満）: {len(targets)}本")
    else:
        parser.print_help()
        return

    if not targets:
        print("対象動画が見つかりません")
        return

    for i, (asset_id, title) in enumerate(targets):
        print(f"\n[{i+1}/{len(targets)}] {title}")
        result = process_one_video(conn, asset_id, title, dry_run=args.dry_run)
        if result:
            print(f"\n--- 分析結果プレビュー（先頭500文字）---")
            print(result[:500])
            print(f"--- (全{len(result):,}文字) ---")

        if i < len(targets) - 1 and not args.dry_run:
            print(f"\n  待機中... ({RATE_LIMIT_DELAY}秒)")
            time.sleep(RATE_LIMIT_DELAY)

    print(f"\n{'='*60}")
    print(f"全{len(targets)}本の処理が完了しました")
    print(f"{'='*60}")

    conn.close()


if __name__ == "__main__":
    main()
