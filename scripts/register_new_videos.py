#!/usr/bin/env python3
"""新規動画をDBに登録するスクリプト

やること:
  1. assetsテーブルに新規レコード作成
  2. video_slugsテーブルにslugを登録
  3. YouTube字幕をダウンロードしてfull_transcriptに保存
  4. YouTubeコメントを取得
  5. YouTube Analytics（日次メトリクス・維持率）を取得

使い方:
  python3 scripts/register_new_videos.py
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

DB_PATH = PROJECT_ROOT / "bankara_brain.db"

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 登録する動画リスト
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

NEW_VIDEOS = [
    {
        "youtube_video_id": "3PW0V1Ixs5o",
        "title": "もしも天才小学生がファミレスを開いたら",
        "slug": "famiresu",
        "series": "天才小学生_お店",
        "duration_seconds": 678,  # 11:18
        "published_at": "2025-09-12",
    },
    {
        "youtube_video_id": "lQUTahoXhlQ",
        "title": "貧乏小学生うたゆーとの一日ルーティン",
        "slug": "utayu_routine",
        "series": "キャラ企画",
        "duration_seconds": 855,  # 14:15
        "published_at": "2025-05-30",
    },
    {
        "youtube_video_id": "KQ9QY09bTdY",
        "title": "天才小学生の休日ルーティン！のはずが・・・",
        "slug": "holiday_routine",
        "series": "キャラ企画",
        "duration_seconds": 1013,  # 16:53
        "published_at": "2025-02-10",
    },
    {
        "youtube_video_id": "EP59RaOdITM",
        "title": "最恐の母による息子がいない日の1日ルーティン",
        "slug": "mother_alone_routine",
        "series": "最恐の母",
        "duration_seconds": 614,  # 10:14
        "published_at": "2025-08-19",
    },
    {
        "youtube_video_id": "t3OaZYXNhLk",
        "title": "もしも小学生が殺し屋だったら４",
        "slug": "hitman4",
        "series": "殺し屋",
        "duration_seconds": 806,  # 13:26
        "published_at": "2023-06-23",
    },
    {
        "youtube_video_id": "wNOs8M58_m0",
        "title": "息子で荒稼ぎしたい母親２｜天才小学生VS最恐の母シリーズ【みそきん】",
        "slug": "moneymaker_mother2",
        "series": "天才小学生VS最恐の母",
        "duration_seconds": 556,  # 9:16
        "published_at": "2023-06-16",
    },
    {
        "youtube_video_id": "tggaRkRn340",
        "title": "きいたときいこ、似てない？",
        "slug": "kiita_kiiko",
        "series": "キャラ企画",
        "duration_seconds": 629,  # 10:29
        "published_at": "2023-07-15",
    },
    {
        "youtube_video_id": "eCbFVp-XP2Q",
        "title": "もしも最恐の母が幼稚園の先生になったら",
        "slug": "mother_kindergarten",
        "series": "最恐の母",
        "duration_seconds": 665,  # 11:05
        "published_at": "2023-05-29",
    },
    {
        "youtube_video_id": "-Aug_-3Vv2I",
        "title": "最恐の母のクズすぎる1日ルーティン｜ストレスが溜まりすぎた日",
        "slug": "mother_kuzu_routine",
        "series": "最恐の母",
        "duration_seconds": 711,  # 11:51
        "published_at": "2023-05-22",
    },
    {
        "youtube_video_id": "0BshfxQ0XSY",
        "title": "実はやねすけには妹がいました",
        "slug": "yanesuke_sister",
        "series": "キャラ企画",
        "duration_seconds": 813,  # 13:33
        "published_at": "2023-07-02",
    },
]


def get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def register_assets(conn):
    """assetsテーブルとvideo_slugsテーブルに新規登録"""
    cur = conn.cursor()
    registered = []
    skipped = []

    for v in NEW_VIDEOS:
        # 既存チェック（youtube_video_idで）
        existing = cur.execute(
            "SELECT id FROM assets WHERE youtube_video_id = ?",
            (v["youtube_video_id"],),
        ).fetchone()

        if existing:
            asset_id = existing[0]
            # video_slugsが未登録なら追加
            slug_exists = cur.execute(
                "SELECT 1 FROM video_slugs WHERE asset_id = ?", (asset_id,)
            ).fetchone()
            if not slug_exists:
                cur.execute(
                    "INSERT INTO video_slugs (asset_id, slug, series) VALUES (?, ?, ?)",
                    (asset_id, v["slug"], v["series"]),
                )
                print(f"  📎 {v['slug']}: video_slugsのみ追加 (asset既存)")
                registered.append(v["slug"])
            else:
                skipped.append(v["slug"])
                print(f"  ⏭️ {v['slug']}: 既に登録済み")
            continue

        # 新規asset作成
        asset_id = str(uuid.uuid4())
        yt_url = f"https://www.youtube.com/watch?v={v['youtube_video_id']}"
        relative_path = f"{v['published_at'].replace('-', '')}_{v['youtube_video_id']}_{v['title'][:60]}.mp4"
        fingerprint = f"yt:{v['youtube_video_id']}"
        metadata = json.dumps({"title": v["title"], "source": "youtube"}, ensure_ascii=False)
        cur.execute(
            """INSERT INTO assets (
                id, relative_path, source_path, storage_path, title,
                fingerprint, sha256, size_bytes, modified_time_ns,
                notes, transcript_excerpt, metadata_json,
                youtube_video_id, duration_seconds,
                media_type, channel, published_at, source_url,
                created_at, updated_at
            ) VALUES (
                ?, ?, ?, '', ?,
                ?, ?, 0, 0,
                '', '', ?,
                ?, ?,
                'video', 'バンカラジオ', ?, ?,
                datetime('now'), datetime('now')
            )""",
            (
                asset_id,
                relative_path,
                yt_url,
                v["title"],
                fingerprint,
                f"yt_{v['youtube_video_id']}",  # sha256 placeholder
                metadata,
                v["youtube_video_id"],
                v["duration_seconds"],
                v["published_at"],
                yt_url,
            ),
        )

        # video_slugs登録
        cur.execute(
            "INSERT INTO video_slugs (asset_id, slug, series) VALUES (?, ?, ?)",
            (asset_id, v["slug"], v["series"]),
        )

        registered.append(v["slug"])
        print(f"  ✅ {v['slug']}: 新規登録 ({v['title'][:30]})")

    conn.commit()
    return registered, skipped


def download_subtitles(conn):
    """yt-dlpで字幕をダウンロードしてfull_transcriptに保存"""
    cur = conn.cursor()
    slugs = [v["slug"] for v in NEW_VIDEOS]
    placeholders = ",".join("?" * len(slugs))

    rows = cur.execute(f"""
        SELECT vs.slug, a.id, a.youtube_video_id, a.full_transcript
        FROM video_slugs vs
        JOIN assets a ON vs.asset_id = a.id
        WHERE vs.slug IN ({placeholders})
    """, slugs).fetchall()

    for row in rows:
        slug, asset_id, yt_id, transcript = row
        if transcript and len(transcript) > 100:
            print(f"  ⏭️ {slug}: 字幕既存 ({len(transcript)}文字)")
            continue

        print(f"  📝 {slug}: 字幕ダウンロード中...")
        with tempfile.TemporaryDirectory() as tmpdir:
            try:
                result = subprocess.run(
                    [
                        "yt-dlp",
                        "--skip-download",
                        "--write-auto-subs",
                        "--sub-langs", "ja",
                        "--convert-subs", "srt",
                        "--output", f"{tmpdir}/%(id)s.%(ext)s",
                        f"https://www.youtube.com/watch?v={yt_id}",
                    ],
                    capture_output=True, text=True, timeout=60,
                )

                # SRTファイルを探す
                srt_files = list(Path(tmpdir).glob("*.srt"))
                if srt_files:
                    srt_text = srt_files[0].read_text(encoding="utf-8")
                    # SRTからテキストだけ抽出
                    lines = []
                    for line in srt_text.splitlines():
                        line = line.strip()
                        # 数字行（インデックス）やタイムスタンプ行をスキップ
                        if not line or line.isdigit() or "-->" in line:
                            continue
                        # HTMLタグ除去
                        clean = re.sub(r"<[^>]+>", "", line)
                        if clean:
                            lines.append(clean)

                    transcript_text = "\n".join(lines)
                    if transcript_text:
                        cur.execute(
                            "UPDATE assets SET full_transcript = ? WHERE id = ?",
                            (transcript_text, asset_id),
                        )
                        conn.commit()
                        print(f"    ✅ {len(transcript_text)}文字の字幕を保存")
                    else:
                        print(f"    ⚠️ 字幕が空")
                else:
                    print(f"    ⚠️ SRTファイルなし")
            except subprocess.TimeoutExpired:
                print(f"    ⚠️ タイムアウト")
            except FileNotFoundError:
                print(f"    ❌ yt-dlpが見つかりません。pip install yt-dlp")
                return
            except Exception as e:
                print(f"    ❌ エラー: {e}")


def fetch_youtube_comments(conn):
    """YouTubeコメントを取得"""
    try:
        from bankara_brain.youtube.comments import sync_comments_for_asset
    except ImportError:
        print("  ⚠️ コメント取得モジュールが見つかりません。スキップ。")
        return

    cur = conn.cursor()
    slugs = [v["slug"] for v in NEW_VIDEOS]
    placeholders = ",".join("?" * len(slugs))
    rows = cur.execute(f"""
        SELECT vs.slug, a.id, a.youtube_video_id
        FROM video_slugs vs
        JOIN assets a ON vs.asset_id = a.id
        WHERE vs.slug IN ({placeholders})
    """, slugs).fetchall()

    for slug, asset_id, yt_id in rows:
        existing = cur.execute(
            "SELECT COUNT(*) FROM youtube_comments WHERE asset_id = ?", (asset_id,)
        ).fetchone()[0]
        if existing > 0:
            print(f"  ⏭️ {slug}: コメント既存 ({existing}件)")
            continue
        print(f"  💬 {slug}: コメント取得中...")
        try:
            sync_comments_for_asset(conn, asset_id, yt_id)
            count = cur.execute(
                "SELECT COUNT(*) FROM youtube_comments WHERE asset_id = ?", (asset_id,)
            ).fetchone()[0]
            print(f"    ✅ {count}件")
        except Exception as e:
            print(f"    ⚠️ エラー: {e}")


def fetch_analytics(conn):
    """YouTube Analyticsデータを取得"""
    slugs = [v["slug"] for v in NEW_VIDEOS]
    placeholders = ",".join("?" * len(slugs))
    cur = conn.cursor()
    rows = cur.execute(f"""
        SELECT vs.slug, a.youtube_video_id
        FROM video_slugs vs
        JOIN assets a ON vs.asset_id = a.id
        WHERE vs.slug IN ({placeholders})
    """, slugs).fetchall()

    video_ids = []
    for slug, yt_id in rows:
        # 維持率データがあるかチェック
        has_retention = cur.execute("""
            SELECT COUNT(*) FROM youtube_retention_points rp
            JOIN assets a ON rp.asset_id = a.id
            WHERE a.youtube_video_id = ?
        """, (yt_id,)).fetchone()[0]
        if has_retention > 0:
            print(f"  ⏭️ {slug}: Analytics既存")
            continue
        video_ids.append((slug, yt_id))

    if not video_ids:
        print("  全動画のAnalytics取得済み")
        return

    for slug, yt_id in video_ids:
        print(f"  📊 {slug}: Analytics取得中...")
        try:
            result = subprocess.run(
                [
                    sys.executable, "-m", "bankara_brain.cli",
                    "sync-youtube-analytics",
                    "--video-id", yt_id,
                    "--start-date", "2022-01-01",
                    "--end-date", "2026-03-16",
                ],
                capture_output=True, text=True, timeout=60,
                cwd=str(PROJECT_ROOT),
            )
            if result.returncode == 0:
                print(f"    ✅ 取得完了")
            else:
                stderr = result.stderr.strip().split("\n")[-1] if result.stderr else ""
                print(f"    ⚠️ エラー: {stderr[:100]}")
        except Exception as e:
            print(f"    ❌ {e}")


def main():
    print(f"\n{'='*60}")
    print(f"🎬 新規動画登録スクリプト")
    print(f"{'='*60}")
    print(f"対象: {len(NEW_VIDEOS)}本\n")

    conn = get_db()

    # Step 1: アセット登録
    print("📦 Step 1: アセット登録")
    registered, skipped = register_assets(conn)
    print(f"  → 新規{len(registered)}本, スキップ{len(skipped)}本\n")

    # Step 2: 字幕ダウンロード
    print("📝 Step 2: 字幕ダウンロード")
    download_subtitles(conn)
    print()

    # Step 3: YouTubeコメント
    print("💬 Step 3: YouTubeコメント取得")
    fetch_youtube_comments(conn)
    print()

    # Step 4: Analytics（メトリクス・維持率）
    print("📊 Step 4: YouTube Analytics取得")
    fetch_analytics(conn)
    print()

    # 結果確認
    print(f"{'='*60}")
    print(f"📊 登録状況確認")
    print(f"{'='*60}")
    cur = conn.cursor()
    slugs = [v["slug"] for v in NEW_VIDEOS]
    for slug in slugs:
        row = cur.execute("""
            SELECT a.title,
                   LENGTH(a.full_transcript) as transcript_len,
                   (SELECT COUNT(*) FROM youtube_comments c WHERE c.asset_id = a.id) as comments,
                   (SELECT COUNT(*) FROM youtube_retention_points r WHERE r.asset_id = a.id) as retention,
                   (SELECT COUNT(*) FROM youtube_daily_metrics m WHERE m.asset_id = a.id) as metrics,
                   (SELECT COUNT(*) FROM brain_deep_analysis d WHERE d.asset_id = a.id) as gemini
            FROM video_slugs vs
            JOIN assets a ON vs.asset_id = a.id
            WHERE vs.slug = ?
        """, (slug,)).fetchone()
        if row:
            t = row[0][:25]
            print(f"  {slug:25s} 字幕={row[1] or 0:5d}字 "
                  f"コメ={row[2]:3d} 維持={row[3]:3d}pt "
                  f"メト={row[4]:3d}d Gemini={'✅' if row[5] else '❌'}")
        else:
            print(f"  {slug:25s} ❌ 未登録")

    conn.close()
    print(f"\n次のステップ: python3 batch_gemini_analysis.py でGemini分析を実行")


if __name__ == "__main__":
    main()
