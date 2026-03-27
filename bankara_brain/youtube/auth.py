"""YouTube OAuth credential management and API service builders."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from bankara_brain.db import AppConfig

YOUTUBE_SCOPES = [
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/youtube.force-ssl",
]


def load_google_api_dependencies() -> tuple[Any, Any, Any, Any]:
    """Lazily import Google API client libraries. Raises RuntimeError if missing."""
    try:
        from google.auth.transport.requests import Request as google_auth_request
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from googleapiclient.discovery import build as build_google_api
    except ImportError as exc:
        raise RuntimeError(
            "YouTube integration requires google-api-python-client and google-auth-oauthlib to be installed."
        ) from exc
    return google_auth_request, Credentials, InstalledAppFlow, build_google_api


def get_youtube_credentials(config: AppConfig, force_reauth: bool = False) -> Any:
    """Obtain valid YouTube OAuth credentials, refreshing or re-authenticating as needed."""
    google_auth_request, Credentials, InstalledAppFlow, _ = load_google_api_dependencies()

    if force_reauth and config.youtube_token_file.exists():
        config.youtube_token_file.unlink()

    creds = None
    if config.youtube_token_file.exists():
        creds = Credentials.from_authorized_user_file(str(config.youtube_token_file), YOUTUBE_SCOPES)

    if creds and creds.expired and creds.refresh_token:
        creds.refresh(google_auth_request())
        config.youtube_token_file.write_text(creds.to_json(), encoding="utf-8")

    if not creds or not creds.valid:
        if not config.youtube_client_secrets_file.exists():
            raise FileNotFoundError(
                f"YouTube OAuth client secrets file not found: {config.youtube_client_secrets_file}"
            )
        flow = InstalledAppFlow.from_client_secrets_file(
            str(config.youtube_client_secrets_file),
            YOUTUBE_SCOPES,
        )
        creds = flow.run_local_server(port=0)
        config.youtube_token_file.write_text(creds.to_json(), encoding="utf-8")

    return creds


def build_youtube_data_service(config: AppConfig, force_reauth: bool = False) -> Any:
    """Build a YouTube Data API v3 service object."""
    _, _, _, build_google_api = load_google_api_dependencies()
    creds = get_youtube_credentials(config, force_reauth=force_reauth)
    return build_google_api("youtube", "v3", credentials=creds, cache_discovery=False)


def build_youtube_analytics_service(config: AppConfig) -> Any:
    """Build a YouTube Analytics API v2 service object."""
    _, _, _, build_google_api = load_google_api_dependencies()
    creds = get_youtube_credentials(config, force_reauth=False)
    return build_google_api("youtubeAnalytics", "v2", credentials=creds, cache_discovery=False)


def auth_youtube(config: AppConfig, force_reauth: bool) -> None:
    """Authenticate with YouTube and confirm the token is ready."""
    get_youtube_credentials(config, force_reauth=force_reauth)
    print(f"YouTube OAuth token ready: {config.youtube_token_file}")
