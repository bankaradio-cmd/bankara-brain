"""Embedding configuration: constants, tag dictionaries, and data classes."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

INDEX_DIMENSION = 3072
INDEX_METRIC = "cosine"
EMBEDDING_MODEL = "gemini-embedding-2-preview"
INLINE_REQUEST_LIMIT_BYTES = 100 * 1024 * 1024
VIDEO_DURATION_LIMIT_SECONDS = 120.0
AUDIO_DURATION_LIMIT_SECONDS = 80.0
STATE_VERSION = 1
DEFAULT_STATE_FILE = ".multimodal_ingest_state.json"
TRANSCRIPT_EXCERPT_CHARS = 1500
FILE_READY_TIMEOUT_SECONDS = 300
TRANSIENT_RETRY_ATTEMPTS = 3
TRANSIENT_RETRY_BASE_SECONDS = 2.0
FEEDBACK_SCORE_FIELDS = (
    "feedback_score_v1",
    "watch_ratio_avg",
    "relative_retention_avg",
    "hook_watch_ratio_avg",
)
SUMMARY_TEXT_KEY = "brain_summary_text_v1"
SUMMARY_JSON_KEY = "brain_summary_v1"
QUERY_FACET_MODEL = "gemini-2.5-flash"
SEGMENT_KIND_PRIORITY = {"hook": 3, "beat": 2, "payoff": 1}
FACET_CONFLICT_COMBINED_WEIGHT = 0.12
LANE_CONFLICT_COMBINED_WEIGHT = 0.14


CANONICAL_MATCH_TAGS: dict[str, tuple[str, ...]] = {
    "天才小学生": ("天才小学生", "天才", "やねすけ", "genius-kid", "genius kid", "geniuskid"),
    "最恐の母": ("最恐の母", "最恐の母親", "母親", "母", "きいこ", "mother", "monster mom", "monstermom"),
    "ヒーロー小学生": ("ヒーロー", "戦隊", "バンカレンジャー", "hero"),
    "名探偵小学生": ("名探偵", "コナン", "推理", "detective"),
    "犯罪小学生": ("強盗犯", "殺し屋", "犯罪者", "crime", "killer"),
    "スパイ任務": ("スパイ", "国家機密", "危険任務", "潜入", "工作員", "諜報", "エージェント", "spy", "mission", "espionage"),
    "飲食店": ("寿司屋", "焼肉屋", "ハンバーガー屋", "ラーメン屋", "中華料理屋", "飲食", "レストラン", "料理屋", "food-retail", "food retail"),
    "小売店": ("おもちゃ屋", "コンビニ", "スーパー", "店長", "小売", "売り場", "general-retail", "general retail"),
    "学校イベント": ("運動会", "マラソン大会", "遠足", "夏休み", "調理実習", "学校行事", "学校イベント", "school-event", "school event"),
    "運動イベント": ("運動会", "マラソン大会", "野球", "体育", "athletic"),
    "遠足イベント": ("遠足", "夏休み", "宿泊体験", "outing"),
    "教室イベント": ("調理実習", "授業", "教室", "先生", "教師", "校長", "classroom"),
    "法執行": ("警察官", "逮捕", "捜査", "事件", "取り締まり", "law-authority", "law authority", "law"),
    "医療救助": ("医者", "消防士", "病院", "救助", "救急", "消防", "emergency-authority", "emergency authority", "medical"),
    "国家権力": ("総理大臣", "総理", "首相", "政治", "国家権力", "national-authority", "national authority", "government"),
    "ゲーム世界": ("逃走中", "ロブロックス", "roblox", "バトルロワイヤル", "マインクラフト", "スマブラ", "game-world", "game world"),
    "支配": ("支配", "暴走", "命令", "権力", "振り回す"),
    "高速": ("高速", "ハイテンポ", "テンポ", "回転"),
}

CANONICAL_TAG_GROUPS: dict[str, tuple[str, ...]] = {
    "character_lane": ("天才小学生", "最恐の母", "ヒーロー小学生", "名探偵小学生", "犯罪小学生"),
    "retail_lane": ("飲食店", "小売店"),
    "school_event_lane": ("運動イベント", "遠足イベント", "教室イベント"),
    "authority_lane": ("法執行", "医療救助", "国家権力"),
    "mission_lane": ("スパイ任務",),
    "world_lane": ("ゲーム世界",),
}

QUERY_TARGET_LANE_HINTS: dict[str, tuple[str, ...]] = {
    "mother-profession-school-authority": ("教師", "先生", "校長", "生徒", "学園", "学校", "teacher"),
    "mother-profession-law-authority": ("警察官", "警察", "警官", "刑事", "逮捕", "捜査", "law authority"),
    "mother-profession-emergency-authority": ("消防士", "医者", "病院", "救急", "救助", "emergency authority", "medical"),
    "mother-profession-national-authority": ("総理大臣", "総理", "首相", "大統領", "政治", "government", "national authority"),
    "mother-crime": ("スパイ", "国家機密", "危険任務", "潜入", "工作", "諜報", "ミッション", "spy", "espionage", "mission"),
    "mother-shop": ("レストラン", "料理屋", "飲食店", "店を開", "店長", "スーパー", "コンビニ", "shop"),
    "school-kid-parody-detective": ("名探偵", "コナン", "推理", "探偵", "detective", "mystery"),
    "school-kid-parody-game-world": ("スマブラ", "マイクラ", "マインクラフト", "大乱闘", "ゲーム世界", "game world", "sandbox"),
    "school-kid-hero": ("ヒーロー", "戦隊", "レンジャー", "変身", "怪人", "hero"),
    "school-kid-crime": ("殺し屋", "強盗犯", "犯罪", "暗殺", "crime", "killer"),
    "genius-kid-shop-food-retail": ("寿司屋", "焼肉屋", "ハンバーガー屋", "ラーメン屋", "中華料理屋", "飲食店", "food retail"),
    "genius-kid-shop-general-retail": ("コンビニ", "スーパー", "おもちゃ屋", "小売", "general retail", "売り場"),
    "genius-kid-school-event-athletic": ("運動会", "マラソン大会", "体育祭", "リレー", "athletic"),
    "genius-kid-school-event-outing": ("遠足", "夏休み", "修学旅行", "林間学校", "宿泊体験", "outing"),
    "genius-kid-school-event-classroom": ("調理実習", "授業", "教室", "学芸会", "文化祭", "classroom"),
    "genius-kid-game-world": ("逃走中", "ロブロックス", "roblox", "バトルロワイヤル", "マインクラフト", "スマブラ", "ゲーム世界", "サバイバル", "game world"),
}

LANE_TARGET_GUARDS: dict[str, tuple[str, ...]] = {
    "mother": ("最恐の母",),
    "genius-kid": ("天才小学生",),
    "school-kid": ("名探偵小学生", "ヒーロー小学生", "犯罪小学生"),
}


@dataclass(frozen=True)
class Settings:
    gemini_api_key: str
    pinecone_api_key: str
    pinecone_index_name: str
    pinecone_namespace: str
    pinecone_cloud: str
    pinecone_region: str

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv(override=False)

        gemini_api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
        pinecone_api_key = os.getenv("PINECONE_API_KEY")

        missing = []
        if not gemini_api_key:
            missing.append("GEMINI_API_KEY (or GOOGLE_API_KEY)")
        if not pinecone_api_key:
            missing.append("PINECONE_API_KEY")

        if missing:
            raise RuntimeError(f"Missing required environment variable(s): {', '.join(missing)}")

        return cls(
            gemini_api_key=gemini_api_key,
            pinecone_api_key=pinecone_api_key,
            pinecone_index_name=os.getenv("PINECONE_INDEX_NAME", "bankara-brain-mvp"),
            pinecone_namespace=os.getenv("PINECONE_NAMESPACE", "bankara-radio"),
            pinecone_cloud=os.getenv("PINECONE_CLOUD", "aws"),
            pinecone_region=os.getenv("PINECONE_REGION", "us-east-1"),
        )


@dataclass(frozen=True)
class PreparedMedia:
    source_path: Path
    embed_path: Path
    media_type: str
    mime_type: str
    source_duration_seconds: float | None
    embed_duration_seconds: float | None
    was_trimmed: bool
