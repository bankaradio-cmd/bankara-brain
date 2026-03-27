from __future__ import annotations

import json
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session, sessionmaker

from bankara_brain.db import BlobStore

# Resolve project root from this file's location (bankara_brain/ingest/pipeline.py)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def default_embedding_python_path() -> Path:
    local_venv_python = _PROJECT_ROOT / ".venv" / "bin" / "python"
    if local_venv_python.exists():
        return local_venv_python
    return Path(sys.executable)


def count_jsonl_rows(file_path: Path) -> int:
    if not file_path.exists():
        return 0
    with file_path.open("r", encoding="utf-8") as handle:
        return sum(1 for line in handle if line.strip())


def run_logged_subprocess(command: list[str], cwd: Path) -> None:
    printable = " ".join(shlex.quote(part) for part in command)
    print(f"$ {printable}")
    subprocess.run(command, cwd=str(cwd), check=True)


def write_jsonl_report_row(handle: Any, payload: dict[str, Any]) -> None:
    handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
    handle.flush()


def run_embedding_manifest_ingest(
    session_factory: sessionmaker[Session],
    manifest_path: Path,
    output_dir: Path,
    namespace: str,
    use_files_api: bool,
    allow_trim_long_media: bool,
    embedding_python: Path | None,
    dry_run: bool,
) -> None:
    workspace_dir = _PROJECT_ROOT
    embedding_script_path = workspace_dir / "gemini_pinecone_multimodal_mvp.py"
    if embedding_python:
        embedding_python_path = embedding_python.expanduser()
        if not embedding_python_path.is_absolute():
            embedding_python_path = workspace_dir / embedding_python_path
    else:
        embedding_python_path = default_embedding_python_path()

    results_path = output_dir / "embedding_results.jsonl"
    report_path = output_dir / "embedding_report.jsonl"
    ingest_command = [
        str(embedding_python_path),
        str(embedding_script_path),
        "ingest-manifest",
        "--manifest",
        str(manifest_path),
        "--namespace",
        namespace,
        "--report-output",
        str(report_path),
    ]
    if use_files_api:
        ingest_command.append("--use-files-api")
    if not allow_trim_long_media:
        ingest_command.append("--no-trim-long-media")

    if dry_run:
        ingest_command.append("--dry-run")
        run_logged_subprocess(ingest_command, cwd=workspace_dir)
        print(f"Dry-run pipeline completed. manifest={manifest_path}")
        return

    ingest_command.extend(["--results-output", str(results_path)])
    run_logged_subprocess(ingest_command, cwd=workspace_dir)

    from bankara_brain.embedding.manifest import import_embedding_results

    import_embedding_results(session_factory=session_factory, results_path=results_path)
    print(
        "Ingest pipeline completed. "
        f"manifest={manifest_path} results={results_path} report={report_path}"
    )


def run_ingest_pipeline(
    session_factory: sessionmaker[Session],
    blob_store: BlobStore,
    dataset_dir: Path,
    output_dir: Path,
    recursive: bool,
    copy_mode: str,
    force_stage: bool,
    replace_bootstrap: bool,
    skip_bootstrap_timeline: bool,
    max_segment_seconds: float,
    min_segment_seconds: float,
    gap_seconds: float,
    target_chars: int,
    limit: int | None,
    namespace: str,
    only_missing_embeddings: bool,
    use_files_api: bool,
    allow_trim_long_media: bool,
    embedding_python: Path | None,
    dry_run: bool,
    channel: str | None,
    require_tags: list[str] | None,
    exclude_tags: list[str] | None,
    title_contains: list[str] | None,
    source_url_contains: list[str] | None,
    selection_status: str | None,
) -> None:
    from bankara_brain.ingest.stage import stage_dataset
    from bankara_brain.corpus.timeline import bootstrap_shot_timeline
    from bankara_brain.embedding.manifest import export_embedding_manifest

    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / "embedding_manifest.jsonl"

    stage_dataset(
        session_factory=session_factory,
        blob_store=blob_store,
        dataset_dir=dataset_dir,
        recursive=recursive,
        copy_mode=copy_mode,
        force=force_stage,
        limit=limit,
    )

    if not skip_bootstrap_timeline:
        bootstrap_shot_timeline(
            session_factory=session_factory,
            asset_selector=None,
            replace=replace_bootstrap,
            max_segment_seconds=max_segment_seconds,
            min_segment_seconds=min_segment_seconds,
            gap_seconds=gap_seconds,
            target_chars=target_chars,
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
    print(f"Manifest rows ready: {manifest_entries}")
    if manifest_entries == 0:
        print("No manifest rows matched the current filters.")
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
