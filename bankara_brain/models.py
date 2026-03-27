"""Bankara Brain — SQLAlchemy model definitions.

All database table definitions live here. This is the single source of truth
for the Brain's data schema.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Optional

from sqlalchemy import Date, DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class Asset(Base):
    __tablename__ = "assets"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    relative_path: Mapped[str] = mapped_column(String(1024), unique=True, index=True)
    source_path: Mapped[str] = mapped_column(String(2048))
    storage_path: Mapped[str] = mapped_column(String(2048))
    transcript_storage_path: Mapped[Optional[str]] = mapped_column(String(2048), nullable=True)
    media_type: Mapped[str] = mapped_column(String(32), index=True)
    mime_type: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    title: Mapped[str] = mapped_column(String(512))
    fingerprint: Mapped[str] = mapped_column(String(128), index=True)
    sha256: Mapped[str] = mapped_column(String(64), index=True)
    size_bytes: Mapped[int] = mapped_column(Integer)
    modified_time_ns: Mapped[int] = mapped_column(Integer)
    duration_seconds: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    notes: Mapped[str] = mapped_column(Text, default="")
    transcript_excerpt: Mapped[str] = mapped_column(Text, default="")
    channel: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    published_at: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    youtube_video_id: Mapped[Optional[str]] = mapped_column(String(64), index=True, nullable=True)
    source_url: Mapped[Optional[str]] = mapped_column(String(2048), nullable=True)
    metadata_json: Mapped[str] = mapped_column(Text, default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: now_utc())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: now_utc(),
        onupdate=lambda: now_utc(),
    )

    text_segments: Mapped[list["TextSegment"]] = relationship(
        back_populates="asset",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    embedding_records: Mapped[list["EmbeddingRecord"]] = relationship(
        back_populates="asset",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    timeline_segments: Mapped[list["TimelineSegment"]] = relationship(
        back_populates="asset",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    feedback_scores: Mapped[list["FeedbackScore"]] = relationship(
        back_populates="asset",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    curation: Mapped[Optional["AssetCuration"]] = relationship(
        back_populates="asset",
        cascade="all, delete-orphan",
        passive_deletes=True,
        uselist=False,
    )


class AssetCuration(Base):
    __tablename__ = "asset_curations"

    asset_id: Mapped[str] = mapped_column(ForeignKey("assets.id", ondelete="CASCADE"), primary_key=True)
    selection_status: Mapped[str] = mapped_column(String(32), index=True, default="unset")
    cohort: Mapped[str] = mapped_column(String(255), default="")
    reason: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: now_utc())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: now_utc(),
        onupdate=lambda: now_utc(),
    )

    asset: Mapped[Asset] = relationship(back_populates="curation")


class TextSegment(Base):
    __tablename__ = "text_segments"
    __table_args__ = (
        UniqueConstraint("asset_id", "segment_kind", "chunk_index", name="uq_text_segments_asset_kind_chunk"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    asset_id: Mapped[str] = mapped_column(ForeignKey("assets.id", ondelete="CASCADE"), index=True)
    segment_kind: Mapped[str] = mapped_column(String(64))
    chunk_index: Mapped[int] = mapped_column(Integer)
    chunk_count: Mapped[int] = mapped_column(Integer)
    start_seconds: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    end_seconds: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    text: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: now_utc())

    asset: Mapped[Asset] = relationship(back_populates="text_segments")


class EmbeddingRecord(Base):
    __tablename__ = "embedding_records"
    __table_args__ = (
        UniqueConstraint("namespace", "record_id", name="uq_embedding_namespace_record"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    asset_id: Mapped[str] = mapped_column(ForeignKey("assets.id", ondelete="CASCADE"), index=True)
    namespace: Mapped[str] = mapped_column(String(255), index=True)
    record_id: Mapped[str] = mapped_column(String(255))
    media_type: Mapped[str] = mapped_column(String(32))
    embedding_model: Mapped[str] = mapped_column(String(255))
    chunk_index: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    metadata_json: Mapped[str] = mapped_column(Text, default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: now_utc())

    asset: Mapped[Asset] = relationship(back_populates="embedding_records")


class YoutubeDailyMetric(Base):
    __tablename__ = "youtube_daily_metrics"
    __table_args__ = (
        UniqueConstraint("video_id", "day", name="uq_youtube_daily_video_day"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    asset_id: Mapped[Optional[str]] = mapped_column(ForeignKey("assets.id", ondelete="SET NULL"), index=True, nullable=True)
    video_id: Mapped[str] = mapped_column(String(64), index=True)
    day: Mapped[date] = mapped_column(Date, index=True)
    views: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    estimated_minutes_watched: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    average_view_duration_seconds: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    average_view_percentage: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    impressions: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    impressions_ctr: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    likes: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    comments: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    shares: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    subscribers_gained: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    subscribers_lost: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: now_utc())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: now_utc(),
        onupdate=lambda: now_utc(),
    )


class YoutubeRetentionPoint(Base):
    __tablename__ = "youtube_retention_points"
    __table_args__ = (
        UniqueConstraint(
            "video_id",
            "start_date",
            "end_date",
            "elapsed_video_time_ratio",
            name="uq_youtube_retention_slice",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    asset_id: Mapped[Optional[str]] = mapped_column(ForeignKey("assets.id", ondelete="SET NULL"), index=True, nullable=True)
    video_id: Mapped[str] = mapped_column(String(64), index=True)
    start_date: Mapped[date] = mapped_column(Date)
    end_date: Mapped[date] = mapped_column(Date)
    elapsed_video_time_ratio: Mapped[float] = mapped_column(Float)
    audience_watch_ratio: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    relative_retention_performance: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: now_utc())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: now_utc(),
        onupdate=lambda: now_utc(),
    )


class TimelineSegment(Base):
    __tablename__ = "timeline_segments"
    __table_args__ = (
        UniqueConstraint("asset_id", "segment_index", name="uq_timeline_segments_asset_index"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    asset_id: Mapped[str] = mapped_column(ForeignKey("assets.id", ondelete="CASCADE"), index=True)
    segment_index: Mapped[int] = mapped_column(Integer)
    segment_kind: Mapped[str] = mapped_column(String(64), default="shot")
    label: Mapped[str] = mapped_column(String(512), default="")
    start_seconds: Mapped[float] = mapped_column(Float)
    end_seconds: Mapped[float] = mapped_column(Float)
    transcript: Mapped[str] = mapped_column(Text, default="")
    notes: Mapped[str] = mapped_column(Text, default="")
    metadata_json: Mapped[str] = mapped_column(Text, default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: now_utc())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: now_utc(),
        onupdate=lambda: now_utc(),
    )

    asset: Mapped[Asset] = relationship(back_populates="timeline_segments")


class YouTubeComment(Base):
    """A single YouTube comment (top-level or reply)."""
    __tablename__ = "youtube_comments"
    __table_args__ = (
        UniqueConstraint("comment_id", name="uq_youtube_comment_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    asset_id: Mapped[Optional[str]] = mapped_column(ForeignKey("assets.id", ondelete="SET NULL"), index=True, nullable=True)
    video_id: Mapped[str] = mapped_column(String(64), index=True)
    comment_id: Mapped[str] = mapped_column(String(255), index=True)
    parent_comment_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True)
    author_display_name: Mapped[str] = mapped_column(String(512), default="")
    author_channel_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    text_original: Mapped[str] = mapped_column(Text, default="")
    like_count: Mapped[int] = mapped_column(Integer, default=0)
    reply_count: Mapped[int] = mapped_column(Integer, default=0)
    published_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    youtube_updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: now_utc())


class Character(Base):
    """A recurring character in the バンカラジオ universe."""
    __tablename__ = "characters"
    __table_args__ = (
        UniqueConstraint("name", name="uq_character_name"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), index=True)
    role: Mapped[str] = mapped_column(String(255), default="")
    affiliation: Mapped[str] = mapped_column(String(255), default="")
    member_color: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    catchphrases: Mapped[str] = mapped_column(Text, default="")
    appearance: Mapped[str] = mapped_column(Text, default="")
    personality: Mapped[str] = mapped_column(Text, default="")
    abilities: Mapped[str] = mapped_column(Text, default="")
    likes: Mapped[str] = mapped_column(Text, default="")
    notes: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: now_utc())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: now_utc(),
        onupdate=lambda: now_utc(),
    )


class FeedbackScore(Base):
    __tablename__ = "feedback_scores"
    __table_args__ = (
        UniqueConstraint(
            "asset_id",
            "scope_type",
            "scope_key",
            "score_name",
            "start_date",
            "end_date",
            name="uq_feedback_scope_score_window",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    asset_id: Mapped[str] = mapped_column(ForeignKey("assets.id", ondelete="CASCADE"), index=True)
    scope_type: Mapped[str] = mapped_column(String(64), index=True)
    scope_key: Mapped[str] = mapped_column(String(255), index=True)
    score_name: Mapped[str] = mapped_column(String(128), index=True)
    start_date: Mapped[date] = mapped_column(Date)
    end_date: Mapped[date] = mapped_column(Date)
    score_value: Mapped[float] = mapped_column(Float)
    sample_count: Mapped[int] = mapped_column(Integer, default=0)
    details_json: Mapped[str] = mapped_column(Text, default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: now_utc())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: now_utc(),
        onupdate=lambda: now_utc(),
    )

    asset: Mapped[Asset] = relationship(back_populates="feedback_scores")
