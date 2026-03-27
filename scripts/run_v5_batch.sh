#!/bin/bash
# v5深層分析バッチ実行スクリプト
# 各動画を独立した claude -p セッションで分析し、コンテキスト汚染を防ぐ
#
# 使い方:
#   テスト（3本）: bash scripts/run_v5_batch.sh
#   本番（全件）:  bash scripts/run_v5_batch.sh --all

cd ~/バンカラジオ/制作部/バンカラブレイン

# --all フラグで全件モード
if [ "$1" = "--all" ]; then
  LIMIT_CLAUSE=""
  echo "モード: 全件"
else
  LIMIT_CLAUSE="LIMIT 3"
  echo "モード: テスト（3本）。全件は --all で実行"
fi

# 対象動画のasset_idとtitleをDBから取得
# v5が未生成の動画のみ対象（再生数順）
TMPFILE=$(mktemp)
sqlite3 bankara_brain.db "
  SELECT a.id || '|' || REPLACE(a.title, '|', '／')
  FROM assets a
  JOIN brain_deep_analysis d ON a.id = d.asset_id
  WHERE NOT EXISTS (
    SELECT 1 FROM brain_deep_analysis d2
    WHERE d2.asset_id = a.id AND d2.version = 'v5'
  )
  GROUP BY a.id
  ORDER BY (
    SELECT COALESCE(SUM(m.views), 0)
    FROM youtube_daily_metrics m
    WHERE m.video_id = a.youtube_video_id
  ) DESC
  ${LIMIT_CLAUSE}
" > "$TMPFILE"

TOTAL=$(wc -l < "$TMPFILE" | tr -d ' ')

if [ "$TOTAL" -eq 0 ]; then
  echo "対象動画がありません（全てv5分析済み）"
  rm -f "$TMPFILE"
  exit 0
fi

echo "対象動画: ${TOTAL}本"
echo "========================================"

COUNT=0
SUCCESS=0
FAIL=0
FAILED_LIST=""
START_TIME=$(date +%s)

while IFS= read -r LINE; do
  ASSET_ID=$(echo "$LINE" | cut -d'|' -f1)
  TITLE=$(echo "$LINE" | cut -d'|' -f2-)
  COUNT=$((COUNT + 1))

  echo ""
  echo "[$COUNT/$TOTAL] 分析中: $TITLE"
  echo "  asset_id: $ASSET_ID"

  RESULT=$(claude -p "
~/雑談/バンカラブレイン_v5分析仕様書.md を読んでから、
~/バンカラジオ/制作部/バンカラブレイン/bankara_brain.db の以下のデータを取得して:
- assets テーブル (asset_id='$ASSET_ID') から基本情報
- brain_deep_analysis から gemini_analysis（最新のレコード）
- youtube_retention_points から維持率データ
- youtube_comments からコメント上位50件（いいね順）
- youtube_daily_metrics から再生回数の累計

仕様書のフォーマットに従ってv5深層分析を生成し、
brain_deep_analysis テーブルに version='v5' で新規INSERTしてください。
既存のv2/v4レコードは上書きしないでください。

重要:
- APIやスクリプトは使わないでください。あなた自身がデータを読んで分析を書いてください
- 教訓は「仮説」として記載してください
- セリフは箇条書きで独立させてください
- コメディのメカニズムは番号付き層構造で書いてください
- 全シーンを分析してください（省略しない）

完了したら、生成した分析の文字数だけ報告してください。
" 2>&1)

  if echo "$RESULT" | grep -qiE "文字|chars|完了|保存"; then
    echo "  ✓ 完了"
    echo "$RESULT" | tail -3
    SUCCESS=$((SUCCESS + 1))
  else
    echo "  ✗ 失敗"
    echo "$RESULT" | tail -5
    FAIL=$((FAIL + 1))
    FAILED_LIST="${FAILED_LIST}
  - ${TITLE}"
  fi

  echo "---"
done < "$TMPFILE"

rm -f "$TMPFILE"

END_TIME=$(date +%s)
ELAPSED=$(( (END_TIME - START_TIME) / 60 ))

echo ""
echo "========================================"
echo "v5バッチ処理 完了"
echo "  成功: $SUCCESS/$TOTAL"
echo "  失敗: $FAIL/$TOTAL"
echo "  所要時間: ${ELAPSED}分"
if [ -n "$FAILED_LIST" ]; then
  echo ""
  echo "  失敗した動画:$FAILED_LIST"
fi
echo "========================================"
