"""Bankara Brain — Database engine, session management, and infrastructure.

Handles SQLAlchemy engine creation, session factory, BlobStore for
physical file storage, and application configuration from environment.
"""
from __future__ import annotations

import logging
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from bankara_brain.models import Base, now_utc  # noqa: F401 — re-export now_utc

logger = logging.getLogger(__name__)

DEFAULT_DATABASE_URL = "sqlite:///bankara_brain.db"
DEFAULT_OBJECT_STORE_ROOT = "object_store"

# Keys that should be present in .env for full functionality.
# Missing keys produce warnings (not errors) so read-only usage still works.
_RECOMMENDED_ENV_KEYS = [
    "GEMINI_API_KEY",
    "PINECONE_API_KEY",
    "DATABASE_URL",
]


def _check_recommended_env_keys() -> None:
    """Log warnings for any recommended environment variables that are unset."""
    missing = [k for k in _RECOMMENDED_ENV_KEYS if not os.getenv(k)]
    if missing:
        logger.warning(
            "The following environment variables are not set: %s. "
            "Some features may not work. See .env.example for reference.",
            ", ".join(missing),
        )


@dataclass(frozen=True)
class AppConfig:
    database_url: str
    object_store_root: Path
    youtube_client_secrets_file: Path
    youtube_token_file: Path
    expected_youtube_channel_id: str | None

    @classmethod
    def from_env(cls) -> "AppConfig":
        load_dotenv(override=False)
        _check_recommended_env_keys()
        return cls(
            database_url=os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL),
            object_store_root=Path(
                os.getenv("BANKARA_OBJECT_STORE_ROOT", DEFAULT_OBJECT_STORE_ROOT)
            ).expanduser().resolve(),
            youtube_client_secrets_file=Path(
                os.getenv("YOUTUBE_OAUTH_CLIENT_SECRETS_FILE", ".youtube_client_secrets.json")
            ).expanduser().resolve(),
            youtube_token_file=Path(
                os.getenv("YOUTUBE_OAUTH_TOKEN_FILE", ".youtube_oauth_token.json")
            ).expanduser().resolve(),
            expected_youtube_channel_id=(
                (
                    os.getenv("BANKARA_EXPECTED_YOUTUBE_CHANNEL_ID")
                    or "UCT5BVYrrhS7gD5xzloZ8FhA"
                ).strip()
                or None
            ),
        )


class BlobStore:
    def __init__(self, root: Path) -> None:
        self.root = root

    def stage_file(self, source_path: Path, sha256_hex: str, copy_mode: str) -> Path:
        suffix = source_path.suffix.lower()
        relative_blob_path = Path("blobs") / sha256_hex[:2] / sha256_hex[2:4] / f"{sha256_hex}{suffix}"
        destination_path = self.root / relative_blob_path
        destination_path.parent.mkdir(parents=True, exist_ok=True)

        if destination_path.exists():
            return destination_path

        if copy_mode == "hardlink":
            try:
                os.link(source_path, destination_path)
                return destination_path
            except OSError:
                pass

        if copy_mode == "symlink":
            try:
                os.symlink(source_path, destination_path)
                return destination_path
            except OSError:
                pass

        shutil.copy2(source_path, destination_path)
        return destination_path


def create_engine_and_sessionmaker(database_url: str) -> tuple[Any, sessionmaker[Session]]:
    engine_kwargs: dict[str, Any] = {"future": True}
    if database_url.startswith("sqlite"):
        engine_kwargs["connect_args"] = {"check_same_thread": False}
    engine = create_engine(database_url, **engine_kwargs)
    return engine, sessionmaker(bind=engine, future=True, expire_on_commit=False)


def init_db(config: AppConfig) -> sessionmaker[Session]:
    engine, session_factory = create_engine_and_sessionmaker(config.database_url)
    Base.metadata.create_all(engine)
    return session_factory
