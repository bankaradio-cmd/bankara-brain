"""Core embedding generation: text and media document embedding."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

from google import genai
from google.genai import types

from bankara_brain.embedding.config import (
    EMBEDDING_MODEL,
    INDEX_DIMENSION,
    INLINE_REQUEST_LIMIT_BYTES,
    PreparedMedia,
)
from bankara_brain.embedding.client import with_transient_retries, _get_attr


def _single_embedding_values(response: Any) -> list[float]:
    embeddings = getattr(response, "embeddings", None)
    if not embeddings:
        raise RuntimeError("Gemini embedding response did not contain embeddings.")

    values = embeddings[0].values
    if len(values) != INDEX_DIMENSION:
        raise RuntimeError(
            f"Expected {INDEX_DIMENSION} dimensions, but received {len(values)} dimensions."
        )
    return list(values)


def embed_text(client: genai.Client, text: str, task_type: str) -> list[float]:
    response = with_transient_retries(
        action_label=f"text embedding ({task_type})",
        operation=lambda: client.models.embed_content(
            model=EMBEDDING_MODEL,
            contents=text,
            config=types.EmbedContentConfig(
                task_type=task_type,
                output_dimensionality=INDEX_DIMENSION,
            ),
        ),
    )
    return _single_embedding_values(response)


def embed_text_document(client: genai.Client, title: str, text: str) -> list[float]:
    payload = f"title: {title}\n\nbody:\n{text}"
    return embed_text(client, payload, task_type="RETRIEVAL_DOCUMENT")


def embed_media_document(
    client: genai.Client,
    title: str,
    prepared_media: PreparedMedia,
    notes: str = "",
    use_files_api: bool = False,
) -> list[float]:
    def _operation() -> list[float]:
        from bankara_brain.embedding.media import wait_for_uploaded_file_ready

        uploaded_file_name: str | None = None
        media_part: types.Part

        embed_path = prepared_media.embed_path
        mime_type = prepared_media.mime_type

        if use_files_api or embed_path.stat().st_size > INLINE_REQUEST_LIMIT_BYTES:
            uploaded_file = client.files.upload(file=str(embed_path))
            uploaded_file = wait_for_uploaded_file_ready(client, uploaded_file)
            uploaded_file_name = uploaded_file.name
            media_part = types.Part.from_uri(
                file_uri=uploaded_file.uri,
                mime_type=uploaded_file.mime_type or mime_type,
            )
        else:
            media_part = types.Part.from_bytes(
                data=embed_path.read_bytes(),
                mime_type=mime_type,
            )

        # A short text scaffold is embedded together with the raw media part so the vector carries
        # both editorial intent and audio/video semantics in one retrieval document.
        context_lines = [
            f"title: {title}",
            f"media_type: {prepared_media.media_type}",
        ]
        if prepared_media.was_trimmed and prepared_media.source_duration_seconds is not None:
            context_lines.append(
                f"embedded_clip: first {prepared_media.embed_duration_seconds:.1f}s of "
                f"{prepared_media.source_duration_seconds:.1f}s source"
            )
        if notes.strip():
            context_lines.append(notes.strip())

        try:
            response = client.models.embed_content(
                model=EMBEDDING_MODEL,
                contents=[
                    types.Content(
                        parts=[
                            types.Part(text="\n".join(context_lines)),
                            media_part,
                        ]
                    )
                ],
                config=types.EmbedContentConfig(
                    task_type="RETRIEVAL_DOCUMENT",
                    output_dimensionality=INDEX_DIMENSION,
                ),
            )
        finally:
            if uploaded_file_name:
                try:
                    client.files.delete(name=uploaded_file_name)
                except Exception:
                    logger.debug("Failed to delete uploaded file %s", uploaded_file_name, exc_info=True)

        return _single_embedding_values(response)

    return with_transient_retries(
        action_label=f"{prepared_media.media_type} embedding",
        operation=_operation,
    )
