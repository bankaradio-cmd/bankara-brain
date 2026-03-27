"""
4層分析アーキテクチャ — スキーマ定義

層1: scene_card   — 各シーンの確定データ（事実のみ）
層2: knowledge_object — クロスビデオのパターン知識
層3: claim_card   — 根拠付き主張（仮説〜法則）
層4: 最終分析     — Opus 4.6 が上記を参照して生成
"""

from __future__ import annotations

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 列挙型（フィルタリング・分類に使う固定値）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# ── シリーズ分類 ─────────────────────────────────────
SERIES_TYPES = [
    "天才小学生_お店",      # 寿司屋、焼肉屋、ハンバーガー屋、コンビニ etc.
    "最恐の母",             # 警察官、教師、スパイ、消防士 etc.
    "学校イベント",          # 調理実習、遠足、夏休み、マラソン大会 etc.
    "ゲーム_バトル",         # 逃走中、スマブラ、マイクラ、バトルロワイヤル etc.
    "バンカレンジャー",      # 戦隊シリーズ
    "パロディ",             # 鬼滅の刃、トトロ、のび太 etc.
    "キャラ企画",           # ポケカ転売、うたゆーと貧乏、Switch2 etc.
    "その他",
]

# ── シーンの種類 ─────────────────────────────────────
SCENE_TYPES = [
    "導入_事件発生",    # 冒頭で問題・事件が起きる
    "設定_状況説明",    # 世界観やルールを説明する
    "展開_エスカレーション",  # 状況がどんどん悪化/加速する
    "転換点",          # 流れが大きく変わる瞬間
    "クライマックス",   # 最大の見せ場
    "オチ_結末",       # 最後のパンチライン・締め
    "日常_ほのぼの",    # 穏やかなシーン
    "バトル_アクション", # 戦闘・アクションシーン
    "感動_シリアス",    # 泣かせ・真面目なシーン
]

# ── コメディの仕組み ─────────────────────────────────
COMEDY_TYPES = [
    "逆転",           # 期待と真逆のことが起きる（犯罪者→警察官）
    "エスカレーション", # 同じ方向にどんどん過激に
    "天丼",           # 同じネタの繰り返し（ATM破壊2回目）
    "落差",           # 感動→台無し、シリアス→ギャグ
    "テンプレ破壊",    # 「普通こうなる」の裏切り（親子愛→見捨てる）
    "ツッコミ不在",    # 誰も止めない異常事態
    "物理ギャグ",      # 殴る蹴る爆発などの身体コメディ
    "ブラックユーモア", # 倫理的にアウトなネタ
    "お前が言うな",    # 最もやってる本人が正論を言う
    "なし",           # コメディ要素なし
]

# ── 感情トーン（BGM自動化にも流用） ─────────────────
EMOTION_TYPES = [
    "コミカル",
    "怒り",
    "緊張",
    "感動",
    "カオス",
    "ほのぼの",
    "ドヤ",
    "悲しみ",
    "衝撃",
]

# ── 物語上の役割 ─────────────────────────────────────
NARRATIVE_ROLES = [
    "hook",         # 開始フック（視聴者を掴む）
    "setup",        # フリ（後のオチのための仕込み）
    "escalation",   # 展開・加速
    "turning_point", # 転換点
    "climax",       # 最大の見せ場
    "resolution",   # オチ・解決
    "epilogue",     # エピローグ
]

# ── キャラクター（チャンネル共通） ───────────────────
KNOWN_CHARACTERS = [
    "やねすけ",      # 天才小学生（主人公）
    "きいこ",        # 最恐の母
    "ゆーと",        # やねすけの友人
    "うど潤",        # 金持ちキャラ
    "店長",          # お店系で毎回出る
    "先生",          # 学校イベント系
    "署長",          # 警察系
    # コラボ相手
    "アイルトンモカ",
    "ニューポテトパーティー",
    # モブ・ゲストは自由入力
]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 層1: scene_card（各シーンの事実データ）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# ── 旧フォーマット（v0: Sonnet + Gemini分割準拠）──
# SCENE_CARD_EXAMPLE は後方互換のため残す
SCENE_CARD_EXAMPLE = {
    "scene_id": "police_s01", "asset_id": "uuid", "video_title": "title",
    "scene_index": 1, "scene_count": 6,
    "start_seconds": 24, "end_seconds": 108, "duration_seconds": 84,
    "retention_start_pct": 70.6, "retention_end_pct": 59.4,
    "retention_avg_pct": 63.3, "retention_delta_pct": -11.2,
    "series": "最恐の母", "scene_type": "導入_事件発生", "comedy_type": "逆転",
    "energy": 4, "emotion": "カオス", "narrative_role": "hook",
    "one_line": "", "key_dialogue": "", "comedy_mechanism": "",
    "characters": [], "scene_driver": "", "tags": [],
}

# ── 新フォーマット（v1: Opus + イベント単位細分化）──
SCENE_CARD_V1 = {
    # ── 識別 ──
    "scene_id": "police_s01",
    "asset_id": "uuid-here",
    "video_title": "もしも最恐の母が警察官になったら",
    "scene_index": 1,
    "scene_count": 20,  # 細分化により増加

    # ── 時間 ──
    "start_seconds": 24,
    "end_seconds": 58,
    "duration_seconds": 34,

    # ── 維持率 ──
    "retention_start_pct": 70.6,
    "retention_end_pct": 65.2,
    "retention_avg_pct": 67.9,
    "retention_delta_pct": -5.4,

    # ── 分類（primary + secondary） ──
    "series": "最恐の母",
    "scene_type": "導入_事件発生",
    "primary_comedy_type": "逆転",
    "secondary_comedy_type": "落差",
    "energy": 4,
    "emotion": "カオス",
    "narrative_role": "hook",

    # ── 内容 ──
    "one_line": "ATM破壊犯が警察官にスカウトされる",
    "key_dialogue": "この身体能力…超法規的措置で警察官に任命する",
    "comedy_mechanism": "犯罪者を罰する側が犯罪能力を才能として評価する正義の定義の転倒",

    # ── キャラ ──
    "characters": ["きいこ", "やねすけ", "警察署長"],
    "scene_driver": "きいこ",

    # ── タグ ──
    "tags": ["資格逆転", "権力側の腐敗", "巻き込まれ型導入"],

    # ── ホットスポット ──
    "micro_hotspots": [
        {
            "relative_position": 0.2,
            "event": "ATMを素手で破壊",
            "event_type": "action_peak",
            "note": "超人的身体能力の初提示",
        },
        {
            "relative_position": 0.8,
            "event": "署長のスカウト宣言",
            "event_type": "reversal",
            "note": "逮捕→スカウトの逆転ポイント",
        },
    ],

    # ── 信頼度 ──
    "classification_confidence": 0.92,

    # ── 出自 ──
    "source_model": "claude-opus-4-20250514",
    "ontology_version": "v1",
    "scene_card_version": "v1",
    "generated_at": "2026-03-16T19:00:00+09:00",
}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 層3: claim_card（根拠付き主張）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CLAIM_TYPES = [
    "comedy_pattern",       # コメディの法則
    "retention_pattern",    # 維持率の法則
    "character_pattern",    # キャラクターの法則
    "structure_pattern",    # 構成・脚本の法則
    "audience_pattern",     # 視聴者反応の法則
]

CONFIDENCE_LEVELS = [
    "hypothesis",     # 根拠1-2件: 仮説レベル
    "emerging",       # 根拠3-5件: 浮上中
    "established",    # 根拠6-10件: 確立
    "law",            # 根拠11件以上: 法則レベル
]

CLAIM_CARD_EXAMPLE = {
    # ── 識別 ──
    "claim_id": "comedy_001",
    "claim_type": "comedy_pattern",

    # ── 主張 ──
    "claim": "感動シーンの直後0〜2秒でギャグに切り替えると維持率が維持される",
    "scope": "channel",                          # channel / series:最恐の母 / video

    # ── 根拠 ──
    "evidence_scene_ids": [
        "police_s04",     # 警察官: 面会室の涙→「眠いから」で強制終了
        "conbini_s09",    # コンビニ: 「価値はあるよ」→Z賞の景品に
    ],
    "support_count": 2,

    # ── 反例 ──
    "counterexample_scene_ids": [],
    "counterexample_count": 0,

    # ── 信頼度 ──
    "confidence": "hypothesis",                  # support_count から自動計算
    "confidence_score": 1.0,                     # support / (support + counter)

    # ── メタ ──
    "created_at": "2026-03-16",
    "updated_at": "2026-03-16",
}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 層2: knowledge_object（パターン知識のカテゴリ）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

KNOWLEDGE_OBJECT_TYPES = {
    # ── チャンネル全体 ──
    "comedy_patterns_channel": {
        "description": "チャンネル全体のコメディパターン法則",
        "scope": "channel",
        "example_topics": [
            "エスカレーションの最適段数",
            "落差ギャグの成功条件",
            "天丼の繰り返し最適回数",
        ],
    },
    "retention_rules_channel": {
        "description": "維持率に影響する全体的な法則",
        "scope": "channel",
        "example_topics": [
            "冒頭30秒の維持率を上げるフック構造",
            "中盤ダレを防ぐ方法",
            "終盤の維持率V字回復パターン",
        ],
    },
    "character_dictionary": {
        "description": "キャラクターの性質・関係性・定番ネタ辞書",
        "scope": "channel",
        "example_topics": [
            "きいこの行動パターンと視聴者反応",
            "やねすけの天才設定の使い方",
            "ゆーとのバカキャラとしての役割",
        ],
    },
    "composition_templates": {
        "description": "繰り返し使われる脚本構成テンプレート",
        "scope": "channel",
        "example_topics": [
            "3幕構成のバリエーション",
            "伏線→回収の間隔",
            "オチのパターン分類",
        ],
    },

    # ── シリーズ別 ──
    "patterns_shop_series": {
        "description": "お店シリーズ固有のパターン",
        "scope": "series:天才小学生_お店",
        "example_topics": [
            "起業動機のパターン（巻き込まれ型 vs 自発型）",
            "うまい棒ネタの使い方",
            "店の成功→お母さん襲来の構造",
        ],
    },
    "patterns_mother_series": {
        "description": "最恐の母シリーズ固有のパターン",
        "scope": "series:最恐の母",
        "example_topics": [
            "きいこの暴走エスカレーション段数",
            "きいこvs権力者の対立構造",
            "爆発オチの使い方",
        ],
    },
    "patterns_school_series": {
        "description": "学校イベント系固有のパターン",
        "scope": "series:学校イベント",
        "example_topics": [
            "季節イベントの活かし方",
            "先生キャラの役割",
            "クラスメイトの巻き込み方",
        ],
    },
    "patterns_game_battle_series": {
        "description": "ゲーム・バトル系固有のパターン",
        "scope": "series:ゲーム_バトル",
        "example_topics": [
            "ゲームルールの現実化の仕方",
            "バトルのテンポ感",
            "勝敗のオチパターン",
        ],
    },
}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# RAG 取得ルール
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

RAG_RETRIEVAL_RULES = {
    "step1_filter": {
        "description": "まず構造化フィルタで候補を絞る（ベクトル検索の前）",
        "filters": [
            "series（同じシリーズのシーンを優先）",
            "scene_type（同じ種類のシーンを比較）",
            "characters（同じキャラが出るシーンを比較）",
            "narrative_role（同じ物語上の役割を比較）",
        ],
    },
    "step2_semantic_search": {
        "description": "絞った候補の中でベクトル検索（意味的に近いものを探す）",
        "search_fields": [
            "one_line（シーン要約）",
            "comedy_mechanism（コメディの仕組み）",
            "tags（タグ）",
        ],
    },
    "step3_rank": {
        "description": "検索結果をランキング",
        "ranking_criteria": [
            "維持率の特徴が似ている（retention_deltaが近い）",
            "同シリーズは加点",
            "evidence件数が多いclaim_cardは加点",
        ],
    },
    "max_scene_cards_per_analysis": 30,    # 1本の分析に使うscene_card最大数
    "max_claim_cards_per_analysis": 15,     # 1本の分析に使うclaim_card最大数
}
