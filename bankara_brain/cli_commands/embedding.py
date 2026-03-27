"""CLI commands: purge-embeddings, sync-embedding-metadata,
export-embedding-manifest, import-embedding-results, run-retrieval-benchmark."""
from __future__ import annotations

import argparse
import os
from pathlib import Path


def register(subparsers: "argparse._SubParsersAction") -> None:
    """Register embedding-related sub-commands."""

    purge_parser = subparsers.add_parser(
        "purge-embeddings",
        help="Delete Pinecone vectors and local embedding records for assets that no longer belong in the active corpus.",
    )
    purge_parser.add_argument("--asset", default=None)
    purge_parser.add_argument("--media-type", choices=["text", "audio", "video"], default=None)
    purge_parser.add_argument("--channel", default=None)
    purge_parser.add_argument(
        "--selection-status",
        choices=["included", "excluded", "unset"],
        default="excluded",
        help="Default keeps purge focused on vectors that were intentionally excluded from the corpus.",
    )
    purge_parser.add_argument("--require-tag", action="append", dest="require_tags", default=[])
    purge_parser.add_argument("--exclude-tag", action="append", dest="exclude_tags", default=[])
    purge_parser.add_argument("--title-contains", action="append", default=[])
    purge_parser.add_argument("--source-url-contains", action="append", default=[])
    purge_parser.add_argument("--namespace", default=None, help="Restrict purge to a single Pinecone namespace.")
    purge_parser.add_argument("--limit", type=int, default=None)
    purge_parser.add_argument("--dry-run", action="store_true")
    purge_parser.add_argument("--report-output", type=Path, default=None)

    sync_metadata_parser = subparsers.add_parser(
        "sync-embedding-metadata",
        help="Refresh Pinecone metadata from the latest local asset/feedback/curation state without re-embedding.",
    )
    sync_metadata_parser.add_argument("--asset", default=None)
    sync_metadata_parser.add_argument("--media-type", choices=["text", "audio", "video"], default=None)
    sync_metadata_parser.add_argument("--channel", default=None)
    sync_metadata_parser.add_argument("--selection-status", choices=["included", "excluded", "unset"], default=None)
    sync_metadata_parser.add_argument("--cohort", default=None)
    sync_metadata_parser.add_argument("--subcohort", default=None)
    sync_metadata_parser.add_argument("--require-tag", action="append", dest="require_tags", default=[])
    sync_metadata_parser.add_argument("--exclude-tag", action="append", dest="exclude_tags", default=[])
    sync_metadata_parser.add_argument("--title-contains", action="append", default=[])
    sync_metadata_parser.add_argument("--source-url-contains", action="append", default=[])
    sync_metadata_parser.add_argument("--namespace", default=None)
    sync_metadata_parser.add_argument("--limit", type=int, default=None)
    sync_metadata_parser.add_argument("--dry-run", action="store_true")
    sync_metadata_parser.add_argument("--report-output", type=Path, default=None)

    manifest_parser = subparsers.add_parser(
        "export-embedding-manifest",
        help="Export JSONL entries that the embedding pipeline can ingest.",
    )
    manifest_parser.add_argument("--out", type=Path, required=True, help="Output JSONL file.")
    manifest_parser.add_argument("--namespace", default=os.getenv("PINECONE_NAMESPACE", "bankara-radio"))
    manifest_parser.add_argument("--limit", type=int, default=None)
    manifest_parser.add_argument("--only-missing-embeddings", action="store_true")
    manifest_parser.add_argument("--channel", default=None, help="Only export assets for this channel.")
    manifest_parser.add_argument(
        "--selection-status",
        choices=["included", "excluded", "unset"],
        default=None,
        help="Only export assets with this persisted curation state.",
    )
    manifest_parser.add_argument("--cohort", default=None, help="Only export assets in this curation cohort.")
    manifest_parser.add_argument("--subcohort", default=None, help="Only export assets in this curation subcohort.")
    manifest_parser.add_argument(
        "--require-tag",
        action="append",
        dest="require_tags",
        default=[],
        help="Only export assets whose metadata tags include this value. Repeatable.",
    )
    manifest_parser.add_argument(
        "--exclude-tag",
        action="append",
        dest="exclude_tags",
        default=[],
        help="Skip assets whose metadata tags include this value. Repeatable.",
    )
    manifest_parser.add_argument(
        "--title-contains",
        action="append",
        default=[],
        help="Only export assets whose title contains this text. Repeatable.",
    )
    manifest_parser.add_argument(
        "--source-url-contains",
        action="append",
        default=[],
        help="Only export assets whose source_url contains this text. Repeatable.",
    )

    results_parser = subparsers.add_parser(
        "import-embedding-results",
        help="Import embedding upsert results emitted by the embedding pipeline.",
    )
    results_parser.add_argument("--results", type=Path, required=True, help="Results JSONL file.")

    retrieval_benchmark_parser = subparsers.add_parser(
        "run-retrieval-benchmark",
        help="Evaluate retrieval Hit@1 / Hit@3 / MRR against a curated latest50 benchmark set.",
    )
    retrieval_benchmark_parser.add_argument("--benchmark", type=Path, default=None)
    retrieval_benchmark_parser.add_argument("--out", type=Path, default=None)
    retrieval_benchmark_parser.add_argument("--format", choices=["markdown", "json"], default="markdown")
    retrieval_benchmark_parser.add_argument("--namespace", default=None)
    retrieval_benchmark_parser.add_argument("--semantic-limit", type=int, default=None)
    retrieval_benchmark_parser.add_argument("--media-type", choices=["text", "audio", "video"], default=None)
    retrieval_benchmark_parser.add_argument(
        "--embedding-kind",
        choices=["asset", "text_chunk", "timeline_segment"],
        default=None,
    )
    retrieval_benchmark_parser.add_argument("--rerank-feedback", action="store_true")
    retrieval_benchmark_parser.add_argument("--feedback-weight", type=float, default=None)
    retrieval_benchmark_parser.add_argument("--candidate-k", type=int, default=None)
    retrieval_benchmark_parser.add_argument("--min-feedback-score", type=float, default=None)
    retrieval_benchmark_parser.add_argument("--cross-encoder-rerank", action="store_true")
    retrieval_benchmark_parser.add_argument("--cross-encoder-top-k", type=int, default=None)
    retrieval_benchmark_parser.add_argument("--selection-status", choices=["included", "excluded", "unset"], default=None)
    retrieval_benchmark_parser.add_argument("--cohort", default=None)
    retrieval_benchmark_parser.add_argument("--subcohort", default=None)
    retrieval_benchmark_parser.add_argument("--case", action="append", dest="case_ids", default=[])


_COMMANDS = frozenset([
    "purge-embeddings", "sync-embedding-metadata",
    "export-embedding-manifest", "import-embedding-results",
    "run-retrieval-benchmark",
])


def dispatch(args: argparse.Namespace, *, config, session_factory, blob_store, brain) -> bool:
    """Execute an embedding command. Return True if handled."""
    if args.command not in _COMMANDS:
        return False

    if args.command == "purge-embeddings":
        from bankara_brain.embedding.sync import purge_embeddings
        purge_embeddings(
            session_factory=session_factory,
            asset_selector=args.asset,
            media_type=args.media_type,
            channel=args.channel,
            selection_status=args.selection_status,
            require_tags=args.require_tags,
            exclude_tags=args.exclude_tags,
            title_contains=args.title_contains,
            source_url_contains=args.source_url_contains,
            namespace=args.namespace,
            limit=args.limit,
            dry_run=args.dry_run,
            report_output=args.report_output.expanduser().resolve() if args.report_output else None,
        )
        return True

    if args.command == "sync-embedding-metadata":
        from bankara_brain.embedding.sync import sync_embedding_metadata
        sync_embedding_metadata(
            session_factory=session_factory,
            asset_selector=args.asset,
            media_type=args.media_type,
            channel=args.channel,
            selection_status=args.selection_status,
            cohort=args.cohort,
            subcohort=args.subcohort,
            require_tags=args.require_tags,
            exclude_tags=args.exclude_tags,
            title_contains=args.title_contains,
            source_url_contains=args.source_url_contains,
            namespace=args.namespace,
            limit=args.limit,
            dry_run=args.dry_run,
            report_output=args.report_output.expanduser().resolve() if args.report_output else None,
        )
        return True

    if args.command == "export-embedding-manifest":
        from bankara_brain.embedding.manifest import export_embedding_manifest
        export_embedding_manifest(
            session_factory=session_factory,
            output_path=args.out.expanduser().resolve(),
            namespace=args.namespace,
            limit=args.limit,
            only_missing_embeddings=args.only_missing_embeddings,
            channel=args.channel,
            require_tags=args.require_tags,
            exclude_tags=args.exclude_tags,
            title_contains=args.title_contains,
            source_url_contains=args.source_url_contains,
            selection_status=args.selection_status,
            cohort=args.cohort,
            subcohort=args.subcohort,
        )
        return True

    if args.command == "import-embedding-results":
        from bankara_brain.embedding.manifest import import_embedding_results
        import_embedding_results(session_factory=session_factory, results_path=args.results.expanduser().resolve())
        return True

    if args.command == "run-retrieval-benchmark":
        from bankara_brain.embedding.benchmark import run_retrieval_benchmark
        run_retrieval_benchmark(
            session_factory=session_factory,
            benchmark_path=args.benchmark.expanduser().resolve() if args.benchmark else None,
            output_path=args.out.expanduser().resolve() if args.out else None,
            output_format=args.format,
            namespace=args.namespace,
            semantic_limit=args.semantic_limit,
            media_type=args.media_type,
            embedding_kind=args.embedding_kind,
            rerank_feedback=True if args.rerank_feedback else None,
            feedback_weight=args.feedback_weight,
            candidate_k=args.candidate_k,
            min_feedback_score=args.min_feedback_score,
            cross_encoder_rerank=True if args.cross_encoder_rerank else None,
            cross_encoder_top_k=args.cross_encoder_top_k,
            selection_status=args.selection_status,
            cohort=args.cohort,
            subcohort=args.subcohort,
            case_ids=args.case_ids,
        )
        return True

    return False
