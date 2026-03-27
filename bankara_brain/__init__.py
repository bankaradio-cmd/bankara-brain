"""Bankara Brain — バンカラジオの脳。

純粋なデータベース/分析基盤/API。
動画データの蓄積・分析・提供を行う。
脚本生成やBGM自動化は外部から API 経由で呼び出す。

Usage:
    from bankara_brain import BankaraBrain

    brain = BankaraBrain.from_env()
    asset = brain.get_asset(asset_id)
    metrics = brain.get_daily_metrics("VIDEO_ID")
    segments = brain.get_timeline_segments(asset.id)
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from bankara_brain.models import (  # noqa: F401 — public re-exports
    Asset,
    AssetCuration,
    Base,
    EmbeddingRecord,
    FeedbackScore,
    TextSegment,
    TimelineSegment,
    YoutubeDailyMetric,
    YoutubeRetentionPoint,
    now_utc,
)
from bankara_brain.db import (  # noqa: F401 — public re-exports
    AppConfig,
    BlobStore,
    create_engine_and_sessionmaker,
    init_db,
)


class BankaraBrain:
    """Facade for the Bankara Brain database.

    Provides a clean Python API for reading Brain data.
    External consumers (script assistant, BGM automation, etc.)
    should use this class rather than touching models directly.

    Usage::

        brain = BankaraBrain.from_env()

        # Single asset
        asset = brain.get_asset(asset_id)

        # List assets with filters
        assets = brain.list_assets(channel="バンカラジオ", selection_status="included")

        # YouTube metrics
        metrics = brain.get_daily_metrics("VIDEO_ID")

        # Timeline segments
        segments = brain.get_timeline_segments(asset_id)
    """

    def __init__(self, config: AppConfig, session_factory: sessionmaker[Session]) -> None:
        self.config = config
        self.session_factory = session_factory

    @classmethod
    def from_env(cls) -> "BankaraBrain":
        """Create a BankaraBrain instance from environment variables."""
        config = AppConfig.from_env()
        sf = init_db(config)
        return cls(config=config, session_factory=sf)

    # ── Asset access ─────────────────────────────────────────────────────

    def get_asset(self, asset_id: str) -> Asset | None:
        """Return a single Asset by ID, or None."""
        with self.session_factory() as session:
            return session.get(Asset, asset_id)

    def get_asset_by_video_id(self, youtube_video_id: str) -> Asset | None:
        """Return the Asset linked to a YouTube video ID, or None."""
        with self.session_factory() as session:
            return session.scalar(
                select(Asset).where(Asset.youtube_video_id == youtube_video_id)
            )

    def list_assets(
        self,
        media_type: str | None = None,
        channel: str | None = None,
        selection_status: str | None = None,
        cohort: str | None = None,
        subcohort: str | None = None,
        limit: int | None = None,
    ) -> list[Asset]:
        """Return assets matching filter criteria."""
        from bankara_brain.corpus.query import select_assets_for_filters
        with self.session_factory() as session:
            assets = select_assets_for_filters(
                session=session,
                media_type=media_type,
                channel=channel,
                selection_status=selection_status,
                cohort=cohort,
                subcohort=subcohort,
            )
            if limit is not None:
                assets = assets[:limit]
            # Eagerly detach so they survive session close
            session.expunge_all()
            return assets

    # ── Timeline ─────────────────────────────────────────────────────────

    def get_timeline_segments(self, asset_id: str) -> list[TimelineSegment]:
        """Return all timeline segments for an asset, ordered by index."""
        with self.session_factory() as session:
            segments = session.scalars(
                select(TimelineSegment)
                .where(TimelineSegment.asset_id == asset_id)
                .order_by(TimelineSegment.segment_index)
            ).all()
            session.expunge_all()
            return list(segments)

    # ── Text segments ────────────────────────────────────────────────────

    def get_text_segments(self, asset_id: str) -> list[TextSegment]:
        """Return all text segments (transcript chunks) for an asset."""
        with self.session_factory() as session:
            segments = session.scalars(
                select(TextSegment)
                .where(TextSegment.asset_id == asset_id)
                .order_by(TextSegment.chunk_index)
            ).all()
            session.expunge_all()
            return list(segments)

    # ── YouTube metrics ──────────────────────────────────────────────────

    def get_daily_metrics(self, youtube_video_id: str) -> list[YoutubeDailyMetric]:
        """Return daily YouTube metrics for a video, ordered by date."""
        with self.session_factory() as session:
            rows = session.scalars(
                select(YoutubeDailyMetric)
                .where(YoutubeDailyMetric.video_id == youtube_video_id)
                .order_by(YoutubeDailyMetric.day)
            ).all()
            session.expunge_all()
            return list(rows)

    def get_retention_curve(self, youtube_video_id: str) -> list[YoutubeRetentionPoint]:
        """Return the audience retention curve for a video."""
        with self.session_factory() as session:
            rows = session.scalars(
                select(YoutubeRetentionPoint)
                .where(YoutubeRetentionPoint.video_id == youtube_video_id)
                .order_by(YoutubeRetentionPoint.elapsed_video_time_ratio)
            ).all()
            session.expunge_all()
            return list(rows)

    # ── Feedback ─────────────────────────────────────────────────────────

    def get_feedback_scores(self, asset_id: str) -> list[FeedbackScore]:
        """Return all feedback scores for an asset."""
        with self.session_factory() as session:
            rows = session.scalars(
                select(FeedbackScore)
                .where(FeedbackScore.asset_id == asset_id)
                .order_by(FeedbackScore.score_name)
            ).all()
            session.expunge_all()
            return list(rows)

    # ── Embedding records ────────────────────────────────────────────────

    def get_embedding_records(self, asset_id: str) -> list[EmbeddingRecord]:
        """Return all embedding records for an asset."""
        with self.session_factory() as session:
            rows = session.scalars(
                select(EmbeddingRecord)
                .where(EmbeddingRecord.asset_id == asset_id)
            ).all()
            session.expunge_all()
            return list(rows)

    # ── Feedback patterns (for brief assembly) ────────────────────────────

    def get_top_feedback_patterns(
        self,
        scope_type: str,
        score_name: str,
        media_type: str | None = None,
        limit: int = 5,
        min_score: float | None = None,
        selection_status: str | None = None,
        cohort: str | None = None,
        subcohort: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return top-scoring feedback patterns as serialized dicts.

        Wraps ``collect_feedback_pattern_rows`` +
        ``serialize_feedback_pattern`` so consumers never touch raw sessions.
        """
        from bankara_brain.analysis.feedback import (
            collect_feedback_pattern_rows,
            serialize_feedback_pattern,
        )

        with self.session_factory() as session:
            rows = collect_feedback_pattern_rows(
                session=session,
                scope_type=scope_type,
                score_name=score_name,
                media_type=media_type,
                limit=limit,
                min_score=min_score,
                selection_status=selection_status,
                cohort=cohort,
                subcohort=subcohort,
            )
            return [serialize_feedback_pattern(session, row) for row in rows]

    # ── Semantic search ───────────────────────────────────────────────────

    def run_semantic_search(
        self,
        query: str,
        output_path: Path,
        semantic_limit: int,
        media_type: str | None = None,
        namespace: str | None = None,
        embedding_kind: str | None = None,
        rerank_feedback: bool = False,
        feedback_weight: float = 0.15,
        candidate_k: int | None = None,
        min_feedback_score: float | None = None,
        cross_encoder_rerank: bool = False,
        cross_encoder_top_k: int = 12,
        selection_status: str | None = None,
        cohort: str | None = None,
        subcohort: str | None = None,
    ) -> None:
        """Run Pinecone semantic search and write results to *output_path*.

        Wraps ``run_semantic_search_export`` so consumers get the facade
        rather than a raw session_factory.
        """
        from bankara_brain.corpus.query import run_semantic_search_export

        run_semantic_search_export(
            session_factory=self.session_factory,
            query=query,
            output_path=output_path,
            semantic_limit=semantic_limit,
            media_type=media_type,
            namespace=namespace,
            embedding_kind=embedding_kind,
            rerank_feedback=rerank_feedback,
            feedback_weight=feedback_weight,
            candidate_k=candidate_k,
            min_feedback_score=min_feedback_score,
            cross_encoder_rerank=cross_encoder_rerank,
            cross_encoder_top_k=cross_encoder_top_k,
            selection_status=selection_status,
            cohort=cohort,
            subcohort=subcohort,
        )

    # ── Metadata helpers ─────────────────────────────────────────────────

    @staticmethod
    def parse_metadata(asset: Asset) -> dict[str, Any]:
        """Parse the JSON metadata stored on an asset."""
        import json
        try:
            data = json.loads(asset.metadata_json)
        except (json.JSONDecodeError, TypeError):
            return {}
        return data if isinstance(data, dict) else {}


# ── Re-exports: pure utility functions for consumer convenience ──────────────
from bankara_brain.utils import format_seconds_hms  # noqa: F401
from bankara_brain.corpus.query import (  # noqa: F401
    effective_cohort_label,
    normalize_match_text,
)
from bankara_brain.analysis.structured_summary import (  # noqa: F401
    derive_novelty_constraints,
    extract_structured_summary_text,
    render_cohort_rules_text,
    render_novelty_constraints_text,
    resolve_cohort_rules,
)


__all__ = [
    "AppConfig",
    "Asset",
    "AssetCuration",
    "BankaraBrain",
    "Base",
    "BlobStore",
    "EmbeddingRecord",
    "FeedbackScore",
    "TextSegment",
    "TimelineSegment",
    "YoutubeDailyMetric",
    "YoutubeRetentionPoint",
    "create_engine_and_sessionmaker",
    "derive_novelty_constraints",
    "effective_cohort_label",
    "extract_structured_summary_text",
    "format_seconds_hms",
    "init_db",
    "normalize_match_text",
    "now_utc",
    "render_cohort_rules_text",
    "render_novelty_constraints_text",
    "resolve_cohort_rules",
]
