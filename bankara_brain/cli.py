"""Bankara Brain CLI -- command-line interface for Brain-only operations.

This module owns the argparse parser, command dispatcher, and main entry point
for Brain data-accumulation / analysis / provision commands (37 commands).

Sub-commands are organised into feature modules under
``bankara_brain.cli_commands``:

* ``db_and_ingest`` -- init-db, stage-dataset, run-ingest-pipeline,
  run-maintenance-pipeline
* ``corpus`` -- run-corpus-cycle, list-assets, corpus-status, curate-assets,
  auto-curate-bankara, audit-assets, quarantine-assets, auto-assign-cohorts
* ``youtube`` -- auth-youtube, youtube-whoami, list-youtube-videos,
  link-youtube-assets, list-public-youtube-videos,
  download-public-youtube-videos, import-analytics-csv,
  sync-youtube-analytics, sync-youtube-comments
* ``embedding`` -- purge-embeddings, sync-embedding-metadata,
  export-embedding-manifest, import-embedding-results,
  run-retrieval-benchmark
* ``analysis`` -- enrich-structured-summaries, enrich-visual-audio-summaries,
  doctor, repair-assets
* ``feedback`` -- score-feedback, run-feedback-pipeline, list-feedback,
  recommend-feedback, feedback-diagnostics
* ``timeline`` -- import-shot-timeline, list-shot-timeline,
  bootstrap-shot-timeline

Consumer commands (script-assistant: brief, draft, review, etc.) live in
``bankara_script_assistant.cli`` and are **not** imported here.

For backward-compatible access to all 45 commands, use
``bankara_brain_control_plane.py`` which calls ``build_parser()`` /
``run()`` with the ``extra_commands`` / ``fallback_dispatcher`` callbacks.
"""
from __future__ import annotations

import argparse
import logging

from bankara_brain.logging_config import setup_logging
from bankara_brain.cli_commands import (
    db_and_ingest,
    corpus,
    youtube,
    embedding,
    analysis,
    feedback,
    timeline,
)

COMMAND_MODULES = [
    db_and_ingest,
    corpus,
    youtube,
    embedding,
    analysis,
    feedback,
    timeline,
]


def build_parser(
    *,
    extra_commands: "Callable[[argparse._SubParsersAction], None] | None" = None,
) -> argparse.ArgumentParser:
    """Build the Brain CLI argument parser.

    Parameters
    ----------
    extra_commands:
        Optional callback that receives the *subparsers* action and may
        register additional sub-commands (e.g. consumer commands from
        ``bankara_script_assistant``).
    """
    parser = argparse.ArgumentParser(
        description="Bankara Brain CLI: relational catalog, object store, analytics",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    for mod in COMMAND_MODULES:
        mod.register(subparsers)

    # Allow external callers to register extra sub-commands
    if extra_commands is not None:
        extra_commands(subparsers)

    return parser


def run(
    args: argparse.Namespace,
    *,
    fallback_dispatcher: "Callable[..., bool] | None" = None,
) -> None:
    """Dispatch a parsed CLI command.

    Parameters
    ----------
    fallback_dispatcher:
        Optional callback ``(args, *, brain) -> bool`` tried when
        no Brain command matches.  Used by ``bankara_brain_control_plane``
        to delegate consumer commands.
    """
    from bankara_brain.db import AppConfig, BlobStore, init_db
    from bankara_brain import BankaraBrain

    config = AppConfig.from_env()
    session_factory = init_db(config)
    blob_store = BlobStore(config.object_store_root)
    brain = BankaraBrain(config=config, session_factory=session_factory)

    for mod in COMMAND_MODULES:
        if mod.dispatch(
            args,
            config=config,
            session_factory=session_factory,
            blob_store=blob_store,
            brain=brain,
        ):
            return

    if fallback_dispatcher is not None and fallback_dispatcher(
        args, brain=brain
    ):
        return

    raise RuntimeError(f"Unsupported command: {args.command}")


def main() -> int:
    setup_logging()
    logger = logging.getLogger(__name__)
    parser = build_parser()
    args = parser.parse_args()
    try:
        run(args)
        return 0
    except Exception as exc:
        logger.error("Command failed: %s", exc, exc_info=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
