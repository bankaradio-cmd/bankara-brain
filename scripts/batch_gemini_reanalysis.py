#!/usr/bin/env python3
"""バンカラブレイン — gemini_analysisが空の動画をGemini 3.1 Proで再分析

使い方:
    cd バンカラブレイン
    python3 scripts/batch_gemini_reanalysis.py
"""

from __future__ import annotations

import os
import sqlite3
import sys
import time
import traceback
from pathlib import Path

from google import genai
from google.genai import types


# ============================================================
# 設定
# ============================================================

DB_PATH = Path(__file__).resolve().parent.parent / "bankara_brain.db"
MODEL_NAME = "gemini-3.1-pro-preview"
MAX_RETRIES = 4
BASE_DELAY = 15.0

# ============================================================
# 改善版プロンプト
# ============================================================

ANALYSIS_PROMPT = """あなたはYouTubeコメディチャンネル「バンカラジオ」の動画を分析するエキスパートです。
この動画を視聴し、以下の指示に従って詳細に分析してください。

## 重要: キャラクター辞典（正式名で記載すること）

バンカラジオには以下の主要キャラクターが登場します。動画内のキャラクターを必ずこの正式名で記載してください。
「緑の服の男」「金髪の男」「ピンクの女の子」のような外見描写での代替は禁止です。

- **やねすけ（天才小学生）**: テーマカラー黄色。黄色い帽子がトレードマーク。白ワイシャツ、紺ハーフパンツ、ランドセル。決め台詞「僕、天才ですから」
- **きいこ（最恐の母）**: やねすけの母親。茶髪ロングヘア。ボーダーシャツに黄色いスカート。口癖「あちゃぱ〜」「マニー！」。怪力、口から破壊光線。きいたと同一演者
- **うたゆーと（貧乏小学生）**: テーマカラー緑。緑の服。語尾「〜なんよ」。段ボールハウス住まい。大食い
- **うどじゅん（金持ち小学生）**: テーマカラー金。金髪、ベスト、赤いネクタイ。口癖「僕の家お金持ちだから」
- **こんじゅり（美少女小学生）**: テーマカラーピンク。ピンクの衣装。マイメロ好き。冷静なツッコミ役
- **きいた**: テーマカラー青。青い服。きいこと同一演者。口癖「〜すぎぃ！」。バイプレイヤーとして複数の役を演じる
- **はまい先生（クズ教師）**: スーツ、メガネ。パチンコ好き。口癖「ぴえ〜」。学校シーンの権威キャラ

## 分析の指示

### 1. 登場キャラクター一覧
各キャラクターについて以下を記載:
- **正式名**（上記辞典を参照し、テーマカラーや見た目から識別すること）
- **役柄**: この動画での立ち位置
- **特徴・性格**: この動画で見せた性格や行動パターン
- **外見の特徴**: 衣装、メイク、小道具を具体的に記述
- **他キャラとの関係性**: 敵対/協力/無関心等

### 2. シーン分割とセリフ帰属（最重要）
**1シーンは最大90秒（1分30秒）以内にすること。シーンを勝手にまとめない。**

目安: 8分動画→最低10シーン / 10分→最低12シーン / 13分→最低16シーン / 15分→最低18シーン

各シーンについて:
- **タイムスタンプ**: 開始〜終了（例: 0:00〜1:30）
- **状況**: 場所、時間帯、直前に何があったか
- **セリフ・テロップ**: キャラクターごとにセリフを帰属させて具体的に書き起こす。テロップの内容も逐一記録する。「やねすけ:「僕、天才ですから」」のように書く
- **コメディのメカニズム**: フリ→オチの構造、ギャップ、天丼、パロディ等を具体的に分析

### 3. 映像演出メモ
- **テロップ**: フォント、色、サイズ、タイミング、内容を具体的に
- **効果音・BGM**: どのシーンでどんな音楽/SEが使われているか。BGMの切り替えタイミング
- **カメラワーク・編集**: ズーム、手ブレ、スローモーション、ドローン、一人称視点等の具体的な使用箇所
- **小道具・衣装の変化**: シーンごとの衣装変更、手作り感のある小道具の記録
- **VFX・CG合成**: 爆発、ビーム、合成の使用箇所と品質感

### 4. コメディのポイント
- **笑いのパターン**: 天丼（繰り返し）、ギャップ、パロディ、スケール飛躍、不条理、論破、セリフ完コピ仕返し等のパターンを特定
- **ボケとツッコミの構造**: 誰がボケで誰がツッコミか。ツッコミ不在の場合はその効果
- **伏線と回収**: 仕込みと回収の対応関係を表形式で列挙

日本語で、具体的かつ詳細に分析してください。セリフは可能な限り正確に書き起こしてください。
分析の文字数は最低5,000字以上を目指してください。"""


def load_env():
    """環境変数を読み込む"""
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                os.environ.setdefault(key.strip(), val.strip())


def get_empty_gemini_videos(db_path):
    """gemini_analysisが空の動画リストを返す"""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT a.id as asset_id, a.youtube_video_id, a.title, a.duration_seconds
        FROM assets a
        JOIN brain_deep_analysis d ON a.id = d.asset_id
        WHERE d.gemini_analysis IS NULL OR length(d.gemini_analysis) < 100
        ORDER BY (
            SELECT COALESCE(SUM(m.views), 0)
            FROM youtube_daily_metrics m
            WHERE m.video_id = a.youtube_video_id AND m.views > 0
        ) DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def analyze_video(client, video_id, title, duration):
    """YouTube動画をGeminiに直接渡して分析"""
    youtube_url = f"https://www.youtube.com/watch?v={video_id}"
    minutes = duration / 60
    min_scenes = max(10, int(minutes * 1.2))

    prompt = f"""動画タイトル: 「{title}」
動画の尺: {minutes:.1f}分
最低シーン数: {min_scenes}シーン以上に分割すること

{ANALYSIS_PROMPT}"""

    for attempt in range(MAX_RETRIES):
        try:
            response = client.models.generate_content(
                model=MODEL_NAME,
                contents=[
                    types.Content(
                        role="user",
                        parts=[
                            types.Part.from_uri(
                                file_uri=youtube_url,
                                mime_type="video/mp4",
                            ),
                            types.Part.from_text(text=prompt),
                        ],
                    )
                ],
            )

            if response.text:
                return response.text
            else:
                print(f"    空の応答")
                return None

        except Exception as e:
            err_str = str(e).lower()
            transient = any(k in err_str for k in [
                "429", "500", "502", "503", "504",
                "resource_exhausted", "rate_limit", "too many",
                "timeout", "connection", "unavailable",
            ])
            if transient and attempt < MAX_RETRIES - 1:
                delay = BASE_DELAY * (2 ** attempt)
                print(f"    一時エラー (attempt {attempt+1}/{MAX_RETRIES}), {delay:.0f}秒待機...")
                print(f"    エラー内容: {str(e)[:200]}")
                time.sleep(delay)
            else:
                print(f"    分析エラー: {str(e)[:300]}")
                traceback.print_exc()
                return None
    return None


def save_gemini_analysis(db_path, asset_id, gemini_text):
    """Gemini分析結果をDBに保存（既存レコードを更新）"""
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        UPDATE brain_deep_analysis
        SET gemini_analysis = ?,
            updated_at = datetime('now')
        WHERE asset_id = ?
    """, (gemini_text, asset_id))
    conn.commit()
    conn.close()


def main():
    load_env()

    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("エラー: GEMINI_API_KEY が設定されていません")
        print("  .envファイルに GEMINI_API_KEY=xxx を設定してください")
        sys.exit(1)

    client = genai.Client(api_key=api_key)

    videos = get_empty_gemini_videos(DB_PATH)
    total = len(videos)

    print(f"\n{'='*60}")
    print(f"バンカラブレイン — Gemini 再分析（改善版プロンプト）")
    print(f"{'='*60}")
    print(f"対象動画: {total}本（gemini_analysisが空）")
    print(f"モデル: {MODEL_NAME}")
    print(f"方式: YouTube URL直接渡し（映像分析）")
    print(f"改善点:")
    print(f"  - キャラクター辞典を含むプロンプト")
    print(f"  - 1シーン90秒以内の分割指示")
    print(f"  - セリフ書き起こし・テロップ記録の指示")
    print(f"  - 最低5,000字以上の指示")
    print(f"{'='*60}\n")

    if total == 0:
        print("全動画のGemini分析が完了しています。")
        return

    for i, video in enumerate(videos, 1):
        print(f"\n[{i}/{total}] {video['title']}")
        print(f"    尺: {video['duration_seconds']/60:.1f}分")
        print(f"    YouTube: https://www.youtube.com/watch?v={video['youtube_video_id']}")

    print(f"\n上記{total}本を分析します。")

    succeeded = 0
    failed = 0
    failed_list = []

    for i, video in enumerate(videos, 1):
        vid = video["youtube_video_id"]
        title = video["title"]
        asset_id = video["asset_id"]
        duration = video["duration_seconds"]

        print(f"\n[{i}/{total}] 分析中: {title}")
        print(f"    YouTube: https://www.youtube.com/watch?v={vid}")

        analysis = analyze_video(client, vid, title, duration)

        if analysis:
            save_gemini_analysis(DB_PATH, asset_id, analysis)
            succeeded += 1
            print(f"    ✓ 完了 ({len(analysis)}文字) → DB保存済み")
        else:
            failed += 1
            failed_list.append(f"{title} ({vid})")
            print(f"    ✗ 失敗")

        # レート制限対策（Geminiは動画分析が重いので長めに待つ）
        if i < total:
            wait = 30
            print(f"    {wait}秒待機...")
            time.sleep(wait)

    # サマリー
    print(f"\n{'='*60}")
    print(f"Gemini再分析 完了！")
    print(f"  成功: {succeeded}/{total}本")
    print(f"  失敗: {failed}/{total}本")
    if failed_list:
        print(f"\n  失敗した動画:")
        for f in failed_list:
            print(f"    - {f}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
