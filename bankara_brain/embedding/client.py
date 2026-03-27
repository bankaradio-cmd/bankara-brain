"""Gemini and Pinecone client initialization, retry logic."""

from __future__ import annotations

import logging
import os
import time
from typing import Any

logger = logging.getLogger(__name__)

from dotenv import load_dotenv
from google import genai
from pinecone import Pinecone, ServerlessSpec

from bankara_brain.embedding.config import (
    Settings,
    INDEX_DIMENSION,
    INDEX_METRIC,
    TRANSIENT_RETRY_ATTEMPTS,
    TRANSIENT_RETRY_BASE_SECONDS,
    FILE_READY_TIMEOUT_SECONDS,
)


def default_namespace_from_env() -> str:
    load_dotenv(override=False)
    return os.getenv("PINECONE_NAMESPACE", "bankara-radio")


def create_genai_client(settings: Settings) -> genai.Client:
    return genai.Client(api_key=settings.gemini_api_key)


def create_pinecone_client(settings: Settings) -> Pinecone:
    return Pinecone(api_key=settings.pinecone_api_key)


def ensure_pinecone_index(settings: Settings) -> Any:
    pc = create_pinecone_client(settings)

    if not pc.has_index(settings.pinecone_index_name):
        pc.create_index(
            name=settings.pinecone_index_name,
            dimension=INDEX_DIMENSION,
            metric=INDEX_METRIC,
            spec=ServerlessSpec(
                cloud=settings.pinecone_cloud,
                region=settings.pinecone_region,
            ),
        )

    index_description = pc.describe_index(settings.pinecone_index_name)
    while not _index_ready(index_description):
        print("Waiting for Pinecone index to become ready...", flush=True)
        time.sleep(2)
        index_description = pc.describe_index(settings.pinecone_index_name)

    return pc.Index(host=_get_attr(index_description, "host"))


def _index_ready(index_description: Any) -> bool:
    status = _get_attr(index_description, "status", {}) or {}
    if isinstance(status, dict):
        return bool(status.get("ready"))
    return bool(getattr(status, "ready", False))


def _get_attr(value: Any, key: str, default: Any | None = None) -> Any:
    if isinstance(value, dict):
        return value.get(key, default)
    return getattr(value, key, default)


def is_transient_error(exc: Exception) -> bool:
    message = str(exc).upper()
    transient_markers = (
        "429",
        "500",
        "502",
        "503",
        "504",
        "INTERNAL",
        "UNAVAILABLE",
        "RESOURCE_EXHAUSTED",
        "RATE_LIMIT",
        "TOO MANY REQUESTS",
        "TIMEOUT",
        "TIMED OUT",
        "CONNECTION RESET",
        "TEMPORAR",
        "BAD GATEWAY",
        "SERVICE UNAVAILABLE",
    )
    return any(marker in message for marker in transient_markers)


def with_transient_retries(action_label: str, operation: Any) -> Any:
    attempt = 0
    while True:
        try:
            return operation()
        except Exception as exc:
            attempt += 1
            if attempt > TRANSIENT_RETRY_ATTEMPTS or not is_transient_error(exc):
                raise

            delay_seconds = TRANSIENT_RETRY_BASE_SECONDS * (2 ** (attempt - 1))
            logger.warning(
                "Transient error during %s; retrying in %.1fs (%d/%d): %s",
                action_label, delay_seconds, attempt, TRANSIENT_RETRY_ATTEMPTS, exc,
            )
            time.sleep(delay_seconds)
