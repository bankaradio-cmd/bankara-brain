#!/usr/bin/env python3
"""
深層分析 v5 生成スクリプト

入力: Gemini分析（新プロンプト版） + 維持率 + コメント
出力: brain_deep_analysis に version='v5' で新規レコード保存

v4 → v5 の変更点:
  - 削除: 笑いの分類、マイクロホットスポット、全体リズム分析、
          脚本テンプレート、強み弱み、3行サマリー
  - 変更: セリフは箇条書きで独立、メカニズムは番号付き層構造、
          教訓は仮説として記載
  - scene_cardsは使用しない
  - 既存opus_analysisは参照しない（ゼロから生成）

使い方:
  # 特定動画
  python scripts/generate_deep_analysis_v5.py --title "コンビニ"

  # 全動画
  python scripts/generate_deep_analysis_v5.py --all

  # テスト3本
  python scripts/generate_deep_analysis_v5.py --test

  # ドライラン
  python scripts/generate_deep_analysis_v5.py --title "コンビニ" --dry-run

必要な環境変数:
  ANTHROPIC_API_KEY=sk-ant-xxxxx
"""

from __future__ import annotations

import argparse
import os
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
    """1本の動画の全データを集約して返す（scene_cards不使用）"""
    cur = conn.cursor()

    # 基本情報
    cur.execute("SELECT * FROM assets WHERE id = ?", (asset_id,))
    asset = dict(cur.fetchone())

    # Gemini分析（最新のレコードから取得）
    cur.execute("""
        SELECT gemini_analysis
        FROM brain_deep_analysis WHERE asset_id = ?
        ORDER BY updated_at DESC LIMIT 1
    """, (asset_id,))
    analysis_row = cur.fetchone()
    gemini_analysis = analysis_row['gemini_analysis'] if analysis_row else ""

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
        'gemini_analysis': gemini_analysis,
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

### ヘッダー
```
# v5 深層分析：{タイトル}
```

### 基本情報
再生回数、維持率、動画時間、コメント数、シリーズ位置づけ、再生パフォーマンス（他動画との比較）。

### シーン別分析
全シーンを記述する。1シーンも省略しない。1シーンは最大90秒以内。
8分動画→最低10シーン / 10分→最低12シーン / 13分→最低16シーン

各シーンは以下のフォーマットで書く：

```
### 【シーンX】シーン名（開始時間〜終了時間）| 維持率: XX%（delta Y%）

セリフ引用:
- キャラA「セリフ」
- キャラB「セリフ」

コメディのメカニズム（X層構造）:
1. 層の名前 — なぜウケたか、なぜ効いたかの分析
2. 層の名前 — ...
3. ...

コメント反応: 「コメント内容」（Xlikes）— 反応の意味

教訓（仮説）: 次の脚本に活かせるポイント。数値付き。

【重要】（該当する場合のみ）チャンネル全体に関わる発見。
```

#### シーン分析のルール
- セリフは箇条書きで独立させる。内容説明の中に埋めない
- コメディのメカニズムは番号付き層構造で書く。「面白い」で済ませない
- 教訓は「仮説」として記載する（横断分析で確認後に確定に変更する想定）
- 対比が効果的な場合は表を使ってよい
- コメントやGemini分析で元ネタ・パロディへの言及があれば必ず取り上げる

### 伏線構造マップ
動画内のフリ→回収の関係を表形式で整理：
| 伏線（フリ） | 回収（オチ） | 効果 |

### 維持率×コンテンツ相関表
時間・維持率・コンテンツ内容を細かく対応させた表：
| 時間 | 維持率 | コンテンツ | 分析 |

### コメント傾向分析
コメントから読み取れる視聴者の反応パターンを表形式で：
| カテゴリ | 代表コメント | いいね数 | 心理分析 |

## 重要な注意
- 情報量を最大化する。文字数を気にせず詳しく書く
- 数値は全てデータに基づく。推測しない
- 「面白い」「良い」などの主観は使わない。代わりに「維持率がX%改善」「コメントがY件」など客観指標で語る
- 教訓は具体的に。「テンポが大事」ではなく「挫折シーンは26秒以内」のように数値付きで
- **制作手法（CG、実写、エフェクトの種類など）について推測で書かない。** 映像がどう作られたかはデータからは分からないので、「〇〇の演出」「〇〇の映像効果」のように結果として画面に何が映っているかだけを記述する
- コメントやGemini分析で元ネタ・パロディへの言及があれば必ず取り上げる
- キャラクター名は正式名を使う（やねすけ、きいこ、うたゆーと、うど潤、こんじゅり、きいた、はまい先生）
"""


def build_user_prompt(data: dict) -> str:
    """ユーザープロンプトを構築"""
    asset = data['asset']
    gemini_analysis = data['gemini_analysis']
    retention_points = data['retention_points']
    comments = data['comments']
    metrics = data['metrics']

    # 動画基本情報
    title = asset.get('title', '不明')
    views = metrics.get('views', '不明')
    duration = asset.get('duration_seconds', 0) or 0
    minutes = int(duration // 60)
    seconds = int(duration % 60)

    # 平均維持率
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
- コメント数: {len(comments)}件（上位50件を下記に掲載）
""")

    # ── 維持率推移 ──
    if retention_points:
        prompt_parts.append("## 維持率推移\n")
        for rp in retention_points:
            ratio_pct = rp['elapsed_video_time_ratio'] * 100
            if abs(ratio_pct % 10) < 1.5:
                watch_pct = (rp['audience_watch_ratio'] or 0) * 100
                rel_perf = rp['relative_retention_performance'] or 0
                prompt_parts.append(f"  {ratio_pct:.0f}%地点: 視聴{watch_pct:.1f}% (相対パフォーマンス{rel_perf:.2f})")
        prompt_parts.append("")

    # ── コメント上位 ──
    if comments:
        prompt_parts.append("## コメント（いいね順 上位50件）\n")
        for c in comments[:50]:
            text = c['text_original'].replace('\n', ' ')[:120]
            prompt_parts.append(f"  [{c['like_count']}いいね] {text}")
        prompt_parts.append("")

    # ── Gemini分析 ──
    if gemini_analysis:
        g_text = gemini_analysis[:25000]
        prompt_parts.append(f"## Gemini分析（映像を見て生成された分析）\n{g_text}\n")

    # ── 指示 ──
    prompt_parts.append("""## 指示
上記の全データを統合して、v5深層分析を生成してください。
- 全シーンを分析すること（省略しない）
- セリフは箇条書きで独立させること
- コメディのメカニズムは番号付き層構造で書くこと
- 教訓は「仮説」として記載すること
- 伏線構造マップ、維持率×コンテンツ相関表、コメント傾向分析を含めること
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

def save_analysis(conn, asset_id: str, gemini_text: str, analysis_text: str):
    """v5分析結果をDBに新規レコードとして保存"""
    cur = conn.cursor()

    # 既存のv5レコードがあるか確認
    cur.execute("""
        SELECT id FROM brain_deep_analysis
        WHERE asset_id = ? AND version = 'v5'
    """, (asset_id,))
    existing = cur.fetchone()

    if existing:
        # 既存v5を更新
        cur.execute("""
            UPDATE brain_deep_analysis
            SET opus_analysis = ?, updated_at = datetime('now')
            WHERE asset_id = ? AND version = 'v5'
        """, (analysis_text, asset_id))
    else:
        # 新規v5レコードをINSERT
        cur.execute("""
            INSERT INTO brain_deep_analysis (asset_id, version, gemini_analysis, opus_analysis, created_at, updated_at)
            VALUES (?, 'v5', ?, ?, datetime('now'), datetime('now'))
        """, (asset_id, gemini_text, analysis_text))

    conn.commit()
    print(f"  DB保存完了 (version=v5, asset_id={asset_id[:8]}...)")


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
    comment_count = len(data['comments'])
    gemini_len = len(data['gemini_analysis'])
    print(f"  データ: {comment_count}コメント, Gemini{gemini_len:,}字")

    # プロンプト構築
    prompt = build_user_prompt(data)
    print(f"  プロンプト: {len(prompt):,}文字 (≈{len(prompt)//2:,}トークン)")

    if dry_run:
        print(f"  [DRY RUN] API呼び出しをスキップ")
        print(f"\n--- プロンプト先頭1000文字 ---")
        print(prompt[:1000])
        return ""

    # API呼び出し
    result = call_anthropic_api(SYSTEM_PROMPT, prompt)

    # DB保存
    save_analysis(conn, asset_id, data['gemini_analysis'], result)

    return result


def get_test_targets(conn) -> list[tuple[str, str]]:
    """テスト対象3本を取得"""
    cur = conn.cursor()
    cur.execute("""
        SELECT a.id, a.title
        FROM brain_deep_analysis bda
        JOIN assets a ON bda.asset_id = a.id
        WHERE a.title LIKE '%コンビニ%'
           OR a.title LIKE '%ラーメン%'
           OR a.title LIKE '%焼肉%'
        GROUP BY a.id
        ORDER BY a.title
    """)
    return [(row['id'], row['title']) for row in cur.fetchall()]


def get_all_targets(conn) -> list[tuple[str, str]]:
    """全動画を取得"""
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT a.id, a.title
        FROM brain_deep_analysis bda
        JOIN assets a ON bda.asset_id = a.id
        ORDER BY (
            SELECT COALESCE(SUM(m.views), 0)
            FROM youtube_daily_metrics m
            WHERE m.video_id = a.youtube_video_id AND m.views > 0
        ) DESC
    """)
    return [(row['id'], row['title']) for row in cur.fetchall()]


def main():
    parser = argparse.ArgumentParser(description="深層分析 v5 生成")
    parser.add_argument("--test", action="store_true", help="テスト3本を実行")
    parser.add_argument("--title", type=str, help="タイトルキーワードで指定")
    parser.add_argument("--all", action="store_true", help="全動画を対象に実行")
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
            SELECT DISTINCT a.id, a.title
            FROM brain_deep_analysis bda
            JOIN assets a ON bda.asset_id = a.id
            WHERE a.title LIKE ?
        """, (f"%{args.title}%",))
        targets = [(row['id'], row['title']) for row in cur.fetchall()]
        print(f"「{args.title}」に一致: {len(targets)}本")
    elif args.all:
        targets = get_all_targets(conn)
        print(f"全動画対象: {len(targets)}本")
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
