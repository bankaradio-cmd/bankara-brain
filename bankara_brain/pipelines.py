from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session, sessionmaker

from bankara_brain.db import AppConfig, BlobStore

logger = logging.getLogger(__name__)


def run_maintenance_pipeline(
    session_factory: sessionmaker[Session],
    blob_store: BlobStore,
    output_dir: Path,
    asset_selector: str | None,
    media_type: str | None,
    channel: str | None,
    selection_status: str | None,
    require_tags: list[str] | None,
    exclude_tags: list[str] | None,
    title_contains: list[str] | None,
    source_url_contains: list[str] | None,
    limit: int | None,
    skip_duration_repair: bool,
    skip_transcribe: bool,
    force_transcribe: bool,
    transcribe_script: Path | None,
    transcribe_language: str | None,
    transcribe_model: str | None,
    work_dir: Path | None,
    skip_bootstrap_timeline: bool,
    replace_timeline: bool,
    max_segment_seconds: float,
    min_segment_seconds: float,
    gap_seconds: float,
    target_chars: int,
    namespace: str,
    only_missing_embeddings: bool,
    use_files_api: bool,
    allow_trim_long_media: bool,
    embedding_python: Path | None,
    dry_run: bool,
) -> None:
    from bankara_brain.maintenance import repair_assets
    from bankara_brain.corpus.curation import audit_assets
    from bankara_brain.embedding.manifest import export_embedding_manifest
    from bankara_brain.ingest.pipeline import count_jsonl_rows, run_embedding_manifest_ingest

    output_dir.mkdir(parents=True, exist_ok=True)
    repair_report_path = output_dir / "repair_report.jsonl"
    audit_path = output_dir / "remaining_problems.json"
    audit_summary_path = output_dir / "remaining_problems_summary.json"
    manifest_path = output_dir / "embedding_manifest.jsonl"

    repair_assets(
        session_factory=session_factory,
        blob_store=blob_store,
        asset_selector=asset_selector,
        media_type=media_type,
        channel=channel,
        selection_status=selection_status,
        require_tags=require_tags,
        exclude_tags=exclude_tags,
        title_contains=title_contains,
        source_url_contains=source_url_contains,
        limit=limit,
        skip_duration_repair=skip_duration_repair,
        skip_transcribe=skip_transcribe,
        force_transcribe=force_transcribe,
        transcribe_script=transcribe_script,
        transcribe_language=transcribe_language,
        transcribe_model=transcribe_model,
        work_dir=work_dir,
        skip_bootstrap_timeline=skip_bootstrap_timeline,
        replace_timeline=replace_timeline,
        max_segment_seconds=max_segment_seconds,
        min_segment_seconds=min_segment_seconds,
        gap_seconds=gap_seconds,
        target_chars=target_chars,
        dry_run=dry_run,
        report_output=repair_report_path,
    )

    pre_ingest_summary = audit_assets(
        session_factory=session_factory,
        media_type=media_type,
        channel=channel,
        selection_status=selection_status,
        cohort=None,
        subcohort=None,
        require_tags=require_tags,
        exclude_tags=exclude_tags,
        title_contains=title_contains,
        source_url_contains=source_url_contains,
        only_problems=True,
        only_blockers=False,
        only_warnings=False,
        limit=limit or 1000,
        json_output=audit_path,
        summary_output=audit_summary_path,
    )
    print(
        "Maintenance pre-ingest audit: "
        f"blocker_assets={pre_ingest_summary['assets_with_blockers']} "
        f"warning_assets={pre_ingest_summary['assets_with_warnings']}"
    )

    export_embedding_manifest(
        session_factory=session_factory,
        output_path=manifest_path,
        namespace=namespace,
        limit=limit,
        only_missing_embeddings=only_missing_embeddings,
        channel=channel,
        require_tags=require_tags,
        exclude_tags=exclude_tags,
        title_contains=title_contains,
        source_url_contains=source_url_contains,
        selection_status=selection_status,
        cohort=None,
        subcohort=None,
    )
    manifest_entries = count_jsonl_rows(manifest_path)
    print(f"Maintenance manifest rows ready: {manifest_entries}")
    if manifest_entries == 0:
        print("No manifest rows matched the current maintenance filters.")
        return

    run_embedding_manifest_ingest(
        session_factory=session_factory,
        manifest_path=manifest_path,
        output_dir=output_dir,
        namespace=namespace,
        use_files_api=use_files_api,
        allow_trim_long_media=allow_trim_long_media,
        embedding_python=embedding_python,
        dry_run=dry_run,
    )

    final_summary = audit_assets(
        session_factory=session_factory,
        media_type=media_type,
        channel=channel,
        selection_status=selection_status,
        cohort=None,
        subcohort=None,
        require_tags=require_tags,
        exclude_tags=exclude_tags,
        title_contains=title_contains,
        source_url_contains=source_url_contains,
        only_problems=True,
        only_blockers=False,
        only_warnings=False,
        limit=limit or 1000,
        json_output=audit_path,
        summary_output=audit_summary_path,
    )
    if final_summary["assets_with_blockers"]:
        print(f"Maintenance remaining blockers: {final_summary['assets_with_blockers']}")
    elif final_summary["assets_with_warnings"]:
        print(f"Maintenance remaining warnings: {final_summary['assets_with_warnings']}")
    else:
        print("Maintenance audit clean.")
    print(f"Maintenance pipeline completed. output_dir={output_dir}")


def run_corpus_cycle(
    config: AppConfig,
    session_factory: sessionmaker[Session],
    blob_store: BlobStore,
    output_dir: Path,
    asset_selector: str | None,
    media_type: str | None,
    channel: str | None,
    selection_status: str | None,
    require_tags: list[str] | None,
    exclude_tags: list[str] | None,
    title_contains: list[str] | None,
    source_url_contains: list[str] | None,
    limit: int | None,
    quarantine_problem_filters: list[str] | None,
    quarantine_severity_filters: list[str] | None,
    quarantine_cohort: str,
    quarantine_reason_prefix: str,
    skip_duration_repair: bool,
    skip_transcribe: bool,
    force_transcribe: bool,
    transcribe_script: Path | None,
    transcribe_language: str | None,
    transcribe_model: str | None,
    work_dir: Path | None,
    skip_bootstrap_timeline: bool,
    replace_timeline: bool,
    max_segment_seconds: float,
    min_segment_seconds: float,
    gap_seconds: float,
    target_chars: int,
    namespace: str,
    only_missing_embeddings: bool,
    use_files_api: bool,
    allow_trim_long_media: bool,
    embedding_python: Path | None,
    feedback_start_date: date | None,
    feedback_end_date: date | None,
    overwrite_feedback: bool,
    skip_feedback_sync: bool,
    require_feedback: bool,
    skip_metadata_sync: bool,
    auto_link_assets: bool,
    dry_run: bool,
) -> None:
    from bankara_brain.corpus.curation import corpus_status, audit_assets, quarantine_assets
    from bankara_brain.analysis.scoring import run_feedback_pipeline
    from bankara_brain.embedding.sync import sync_embedding_metadata

    if quarantine_problem_filters or quarantine_severity_filters:
        quarantine_assets(
            session_factory=session_factory,
            media_type=media_type,
            channel=channel,
            selection_status=selection_status,
            require_tags=require_tags,
            exclude_tags=exclude_tags,
            title_contains=title_contains,
            source_url_contains=source_url_contains,
            problem_filters=quarantine_problem_filters,
            severity_filters=quarantine_severity_filters,
            cohort=quarantine_cohort,
            reason_prefix=quarantine_reason_prefix,
            limit=limit,
            dry_run=dry_run,
        )

    run_maintenance_pipeline(
        session_factory=session_factory,
        blob_store=blob_store,
        output_dir=output_dir,
        asset_selector=asset_selector,
        media_type=media_type,
        channel=channel,
        selection_status=selection_status,
        require_tags=require_tags,
        exclude_tags=exclude_tags,
        title_contains=title_contains,
        source_url_contains=source_url_contains,
        limit=limit,
        skip_duration_repair=skip_duration_repair,
        skip_transcribe=skip_transcribe,
        force_transcribe=force_transcribe,
        transcribe_script=transcribe_script,
        transcribe_language=transcribe_language,
        transcribe_model=transcribe_model,
        work_dir=work_dir,
        skip_bootstrap_timeline=skip_bootstrap_timeline,
        replace_timeline=replace_timeline,
        max_segment_seconds=max_segment_seconds,
        min_segment_seconds=min_segment_seconds,
        gap_seconds=gap_seconds,
        target_chars=target_chars,
        namespace=namespace,
        only_missing_embeddings=only_missing_embeddings,
        use_files_api=use_files_api,
        allow_trim_long_media=allow_trim_long_media,
        embedding_python=embedding_python,
        dry_run=dry_run,
    )

    if feedback_start_date and feedback_end_date:
        try:
            run_feedback_pipeline(
                config=config,
                session_factory=session_factory,
                asset_selector=asset_selector,
                video_ids=[],
                start_date=feedback_start_date,
                end_date=feedback_end_date,
                overwrite=overwrite_feedback,
                skip_sync=skip_feedback_sync,
                channel=channel,
                require_tags=require_tags,
                exclude_tags=exclude_tags,
                title_contains=title_contains,
                source_url_contains=source_url_contains,
                selection_status=selection_status,
                cohort=None,
                subcohort=None,
                auto_link_assets=auto_link_assets and not dry_run,
            )
        except Exception as exc:
            if require_feedback:
                raise
            logger.warning("Skipping feedback stage: %s", exc)

    if not skip_metadata_sync:
        sync_embedding_metadata(
            session_factory=session_factory,
            asset_selector=asset_selector,
            media_type=media_type,
            channel=channel,
            selection_status=selection_status,
            cohort=None,
            subcohort=None,
            require_tags=require_tags,
            exclude_tags=exclude_tags,
            title_contains=title_contains,
            source_url_contains=source_url_contains,
            namespace=namespace,
            limit=limit,
            dry_run=dry_run,
            report_output=output_dir / "metadata_sync_report.jsonl",
        )

    corpus_status(
        session_factory=session_factory,
        channel=channel,
        selection_status=selection_status,
        cohort=None,
        subcohort=None,
    )
    cycle_audit_summary = audit_assets(
        session_factory=session_factory,
        media_type=media_type,
        channel=channel,
        selection_status=selection_status,
        cohort=None,
        subcohort=None,
        require_tags=require_tags,
        exclude_tags=exclude_tags,
        title_contains=title_contains,
        source_url_contains=source_url_contains,
        only_problems=True,
        only_blockers=False,
        only_warnings=False,
        limit=limit or 1000,
        json_output=output_dir / "cycle_remaining_problems.json",
        summary_output=output_dir / "cycle_remaining_problems_summary.json",
    )
    if cycle_audit_summary["assets_with_blockers"]:
        print(f"Corpus cycle remaining blockers: {cycle_audit_summary['assets_with_blockers']}")
    elif cycle_audit_summary["assets_with_warnings"]:
        print(f"Corpus cycle remaining warnings: {cycle_audit_summary['assets_with_warnings']}")
    else:
        print("Corpus cycle audit clean.")
    print(f"Corpus cycle completed. output_dir={output_dir}")
