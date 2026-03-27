"""Embedding purge and metadata-sync operations against Pinecone.

Extracted from ``bankara_brain_control_plane.py``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from sqlalchemy import delete
from sqlalchemy.orm import Session, sessionmaker

from bankara_brain.models import EmbeddingRecord
from bankara_brain.corpus.query import (
    asset_selection_status, resolve_asset, select_assets_for_filters,
)
from bankara_brain.utils import safe_json_load
from bankara_brain.embedding.manifest import (
    chunk_list, attr_or_key, load_pinecone_index_from_env,
    normalize_index_metadata, build_embedding_record_sync_metadata,
)
from bankara_brain.embedding.manifest import infer_embedding_kind_from_metadata
from bankara_brain.ingest.pipeline import write_jsonl_report_row


# ---------------------------------------------------------------------------
# Purge
# ---------------------------------------------------------------------------


def purge_embeddings(
    session_factory: sessionmaker[Session],
    asset_selector: str | None,
    media_type: str | None,
    channel: str | None,
    selection_status: str | None,
    require_tags: list[str] | None,
    exclude_tags: list[str] | None,
    title_contains: list[str] | None,
    source_url_contains: list[str] | None,
    namespace: str | None,
    limit: int | None,
    dry_run: bool,
    report_output: Path | None,
) -> None:
    rows: list[dict[str, Any]] = []
    with session_factory() as session:
        if asset_selector:
            assets = [resolve_asset(session, asset_selector)]
        else:
            assets = select_assets_for_filters(
                session=session,
                media_type=media_type,
                channel=channel,
                require_tags=require_tags,
                exclude_tags=exclude_tags,
                title_contains=title_contains,
                source_url_contains=source_url_contains,
                selection_status=selection_status,
            )

        if limit is not None:
            assets = assets[:limit]

        for asset in assets:
            for record in asset.embedding_records:
                if namespace and record.namespace != namespace:
                    continue
                metadata = safe_json_load(record.metadata_json)
                rows.append(
                    {
                        "db_record_id": record.id,
                        "asset_id": asset.id,
                        "relative_path": asset.relative_path,
                        "selection_status": asset_selection_status(asset),
                        "namespace": record.namespace,
                        "record_id": record.record_id,
                        "embedding_kind": infer_embedding_kind_from_metadata(
                            metadata,
                            record.media_type,
                            record.chunk_index,
                        ),
                        "media_type": record.media_type,
                        "status": "pending",
                    }
                )

    if not rows:
        print("No embedding records matched purge filters.")
        return

    if dry_run:
        for row in rows:
            row["status"] = "would_delete"
            print(
                f"purge   {row['relative_path']} namespace={row['namespace']} "
                f"record_id={row['record_id']} embedding_kind={row['embedding_kind']}"
            )
    else:
        index, index_name = load_pinecone_index_from_env()
        print(f"Purging Pinecone embeddings from index={index_name}")
        deleted_db_record_ids: list[int] = []
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault(row["namespace"], []).append(row)

        for row_namespace, namespace_rows in grouped.items():
            for batch in chunk_list(namespace_rows, 100):
                record_ids = [row["record_id"] for row in batch]
                index.delete(namespace=row_namespace, ids=record_ids)
                deleted_db_record_ids.extend(int(row["db_record_id"]) for row in batch)
                for row in batch:
                    row["status"] = "deleted"
                    print(
                        f"deleted {row['relative_path']} namespace={row_namespace} "
                        f"record_id={row['record_id']} embedding_kind={row['embedding_kind']}"
                    )

        if deleted_db_record_ids:
            with session_factory() as session:
                session.execute(delete(EmbeddingRecord).where(EmbeddingRecord.id.in_(deleted_db_record_ids)))
                session.commit()

    if report_output:
        report_output.parent.mkdir(parents=True, exist_ok=True)
        with report_output.open("w", encoding="utf-8") as handle:
            for row in rows:
                write_jsonl_report_row(handle, row)
        print(f"Wrote purge report: {report_output}")

    action = "would_delete" if dry_run else "deleted"
    print(
        f"Purge summary: {action}_records={len(rows)} "
        f"assets={len({row['asset_id'] for row in rows})} "
        f"namespaces={len({row['namespace'] for row in rows})}"
    )


# ---------------------------------------------------------------------------
# Metadata sync
# ---------------------------------------------------------------------------


def sync_embedding_metadata(
    session_factory: sessionmaker[Session],
    asset_selector: str | None,
    media_type: str | None,
    channel: str | None,
    selection_status: str | None,
    cohort: str | None,
    subcohort: str | None,
    require_tags: list[str] | None,
    exclude_tags: list[str] | None,
    title_contains: list[str] | None,
    source_url_contains: list[str] | None,
    namespace: str | None,
    limit: int | None,
    dry_run: bool,
    report_output: Path | None,
) -> None:
    rows: list[dict[str, Any]] = []
    with session_factory() as session:
        if asset_selector:
            assets = [resolve_asset(session, asset_selector)]
        else:
            assets = select_assets_for_filters(
                session=session,
                media_type=media_type,
                channel=channel,
                require_tags=require_tags,
                exclude_tags=exclude_tags,
                title_contains=title_contains,
                source_url_contains=source_url_contains,
                selection_status=selection_status,
                cohort=cohort,
                subcohort=subcohort,
            )

        if limit is not None:
            assets = assets[:limit]

        for asset in assets:
            for record in asset.embedding_records:
                if namespace and record.namespace != namespace:
                    continue
                metadata = build_embedding_record_sync_metadata(session, asset, record)
                rows.append(
                    {
                        "db_record_id": record.id,
                        "asset_id": asset.id,
                        "relative_path": asset.relative_path,
                        "namespace": record.namespace,
                        "record_id": record.record_id,
                        "embedding_kind": infer_embedding_kind_from_metadata(
                            metadata,
                            record.media_type,
                            record.chunk_index,
                        ),
                        "selection_status": asset_selection_status(asset),
                        "metadata": metadata,
                        "status": "pending",
                    }
                )

    if not rows:
        print("No embedding records matched metadata sync filters.")
        return

    if dry_run:
        for row in rows:
            row["status"] = "would_sync"
            print(
                f"sync    {row['relative_path']} namespace={row['namespace']} "
                f"record_id={row['record_id']} embedding_kind={row['embedding_kind']} "
                f"selection={row['selection_status']}"
            )
    else:
        index, index_name = load_pinecone_index_from_env()
        print(f"Syncing Pinecone metadata on index={index_name}")
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault(row["namespace"], []).append(row)

        with session_factory() as session:
            for row_namespace, namespace_rows in grouped.items():
                for batch in chunk_list(namespace_rows, 100):
                    record_ids = [row["record_id"] for row in batch]
                    fetch_response = index.fetch(ids=record_ids, namespace=row_namespace)
                    fetched_vectors = attr_or_key(fetch_response, "vectors", {}) or {}
                    upsert_vectors = []
                    batch_db_ids: list[int] = []
                    for row in batch:
                        vector_state = attr_or_key(fetched_vectors, row["record_id"])
                        if not vector_state:
                            row["status"] = "missing_remote"
                            print(
                                f"missing {row['relative_path']} namespace={row_namespace} record_id={row['record_id']}"
                            )
                            continue
                        values = attr_or_key(vector_state, "values")
                        if not values:
                            row["status"] = "missing_remote_values"
                            print(
                                f"missing-values {row['relative_path']} namespace={row_namespace} record_id={row['record_id']}"
                            )
                            continue
                        upsert_vectors.append(
                            {
                                "id": row["record_id"],
                                "values": list(values),
                                "metadata": normalize_index_metadata(row["metadata"]),
                            }
                        )
                        batch_db_ids.append(int(row["db_record_id"]))

                    if upsert_vectors:
                        index.upsert(namespace=row_namespace, vectors=upsert_vectors)
                        for row in batch:
                            if row.get("status") != "pending":
                                continue
                            row["status"] = "synced"
                            print(
                                f"synced  {row['relative_path']} namespace={row_namespace} "
                                f"record_id={row['record_id']} embedding_kind={row['embedding_kind']} "
                                f"selection={row['selection_status']}"
                            )

                    for db_record_id in batch_db_ids:
                        payload = next(row for row in batch if int(row["db_record_id"]) == db_record_id)
                        record = session.get(EmbeddingRecord, db_record_id)
                        if record is None:
                            continue
                        record.metadata_json = json.dumps(payload["metadata"], ensure_ascii=False)
                        session.add(record)

            session.commit()

    if report_output:
        report_output.parent.mkdir(parents=True, exist_ok=True)
        with report_output.open("w", encoding="utf-8") as handle:
            for row in rows:
                report_row = dict(row)
                report_row["metadata"] = normalize_index_metadata(report_row["metadata"])
                write_jsonl_report_row(handle, report_row)
        print(f"Wrote metadata sync report: {report_output}")

    synced_count = sum(1 for row in rows if row["status"] in {"synced", "would_sync"})
    missing_count = sum(1 for row in rows if row["status"].startswith("missing_remote"))
    action = "would_sync" if dry_run else "synced"
    print(
        f"Metadata sync summary: {action}_records={synced_count} "
        f"missing_remote={missing_count} assets={len({row['asset_id'] for row in rows})}"
    )
