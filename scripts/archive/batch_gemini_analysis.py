#!/usr/bin/env python3
"""バンカラブレイン — 未分析動画のGemini 3.1 Pro一括分析スクリプト

使い方:
    python3 batch_gemini_analysis.py

やること:
    1. DBから未分析の動画を取得
    2. YouTube URLをGeminiに直接渡して分析（ダウンロード不要）
    3. 結果をDBのbrain_deep_analysisテーブルに保存
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

DB_PATH = Path(__file__).parent / "bankara_brain.db"
MODEL_NAME = "gemini-3.1-pro-preview"
MAX_RETRIES = 4
BASE_DELAY = 10.0

# Gemini分析プロンプト
ANALYSIS_PROMPT = """あなたはYouTubeコメディチャンネル「バンカラジオ」の動画分析エキスパートです。
この動画を視聴し、以下の観点から詳細に分析してください。

## 1. 登場キャラクター一覧
各キャラクターについて:
- 名前・役柄
- 特徴・性格
- 外見の特徴
- 他キャラとの関係性

## 2. シーン構成（時系列順）
各シーンについて:
- 時間帯（おおよそ）
- 場所・状況
- 何が起きたか（具体的に）
- コメディとしてのメカニズム（フリ→オチの構造、ギャップ、天丼など）
- エネルギーレベル（1-5）
- 感情トーン（コミカル/怒り/緊張/感動/カオス/ほのぼの/ドヤ/悲しみ/衝撃）

## 3. 脚本構造の分析
- 導入フック（最初の30秒で視聴者を掴む仕組み）
- エスカレーション（どう盛り上げているか）
- クライマックス（最大の見せ場）
- オチ・締め方
- コメディのリズム（テンポの緩急）

## 4. 演出・編集の特徴
- テロップの使い方
- カメラワーク
- SE（効果音）の使い方
- BGMの使い方（どんな場面でどんな雰囲気の曲が使われているか）
- 編集テンポ

## 5. 動画の強みと弱み
- 特に面白いポイント
- 視聴者が離脱しそうなポイント
- シリーズ他作品と比較した特徴

日本語で、具体的かつ詳細に分析してください。"""


def load_env():
    """環境変数を読み込む"""
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                os.environ.setdefault(key.strip(), val.strip())


def get_unanalyzed_videos(db_path):
    """未分析動画のリストを返す"""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT a.id as asset_id, a.youtube_video_id, a.title
        FROM assets a
        LEFT JOIN brain_deep_analysis d ON a.id = d.asset_id AND d.version = 'v2'
        WHERE d.id IS NULL
        ORDER BY a.title
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def analyze_video(client, video_id, title):
    """YouTube動画をGeminiに直接渡して分析"""
    youtube_url = f"https://www.youtube.com/watch?v={video_id}"

    for attempt in range(MAX_RETRIES):
        try:
            prompt = f"動画タイトル: 「{title}」\n\n{ANALYSIS_PROMPT}"
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
                return None
    return None


def save_gemini_analysis(db_path, asset_id, gemini_text):
    """Gemini分析結果をDBに保存"""
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        INSERT INTO brain_deep_analysis (asset_id, version, gemini_analysis, opus_analysis, created_at, updated_at)
        VALUES (?, 'v2', ?, '', datetime('now'), datetime('now'))
        ON CONFLICT(asset_id, version) DO UPDATE SET
            gemini_analysis=excluded.gemini_analysis,
            updated_at=datetime('now')
    """, (asset_id, gemini_text))
    conn.commit()
    conn.close()


def main():
    load_env()

    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("エラー: GEMINI_API_KEY が設定されていません")
        sys.exit(1)

    client = genai.Client(api_key=api_key)

    videos = get_unanalyzed_videos(DB_PATH)
    total = len(videos)

    print(f"\n{'='*60}")
    print(f"バンカラブレイン — Gemini 3.1 Pro 一括分析")
    print(f"{'='*60}")
    print(f"未分析動画: {total}本")
    print(f"モデル: {MODEL_NAME}")
    print(f"方式: YouTube URL直接渡し（ダウンロード不要）")
    print(f"{'='*60}\n")

    if total == 0:
        print("全動画分析済み。完了。")
        return

    succeeded = 0
    failed = 0
    failed_list = []

    for i, video in enumerate(videos, 1):
        vid = video["youtube_video_id"]
        title = video["title"]
        asset_id = video["asset_id"]

        print(f"\n[{i}/{total}] {title}")
        print(f"    YouTube: https://www.youtube.com/watch?v={vid}")

        analysis = analyze_video(client, vid, title)

        if analysis:
            save_gemini_analysis(DB_PATH, asset_id, analysis)
            succeeded += 1
            print(f"    ✓ 完了 ({len(analysis)}文字) → DB保存済み")
        else:
            failed += 1
            failed_list.append(f"{title} ({vid})")
            print(f"    ✗ 失敗")

        # レート制限対策
        if i < total:
            wait = 15
            print(f"    {wait}秒待機...")
            time.sleep(wait)

    # サマリー
    print(f"\n{'='*60}")
    print(f"Gemini分析 完了！")
    print(f"  成功: {succeeded}/{total}本")
    print(f"  失敗: {failed}/{total}本")
    if failed_list:
        print(f"\n  失敗した動画:")
        for f in failed_list:
            print(f"    - {f}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
