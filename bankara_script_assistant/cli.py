"""CLI helpers for the Bankara Script Assistant consumer package."""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from bankara_brain import BankaraBrain

from bankara_script_assistant.brief import build_query_brief, build_live_query_brief
from bankara_script_assistant.generation import (
    DEFAULT_GENERATION_MODEL,
    generate_draft_from_brief_file,
    generate_live_draft,
    generate_idea_batch,
)
from bankara_script_assistant.review import (
    evaluate_draft_from_files,
    revise_draft_from_files,
    run_generation_cycle,
)


SCRIPT_ASSISTANT_COMMANDS = {
    "build-query-brief",
    "build-live-query-brief",
    "generate-draft-from-brief",
    "generate-live-draft",
    "generate-idea-batch",
    "evaluate-draft",
    "revise-draft",
    "run-cycle",
}


def register_script_assistant_commands(subparsers: argparse._SubParsersAction) -> None:
    """Register consumer-facing script assistant commands."""
    query_brief_parser = subparsers.add_parser(
        "build-query-brief",
        help="Assemble semantic matches and winning feedback patterns into a brief.",
    )
    query_brief_parser.add_argument("--query", required=True)
    query_brief_parser.add_argument("--search-results", type=Path, default=None)
    query_brief_parser.add_argument("--out", type=Path, default=None)
    query_brief_parser.add_argument("--format", choices=["markdown", "json"], default="markdown")
    query_brief_parser.add_argument("--timeline-limit", type=int, default=5)
    query_brief_parser.add_argument("--asset-limit", type=int, default=3)
    query_brief_parser.add_argument("--semantic-limit", type=int, default=5)
    query_brief_parser.add_argument("--media-type", choices=["text", "audio", "video"], default=None)
    query_brief_parser.add_argument("--score-name", default="feedback_score_v1")
    query_brief_parser.add_argument("--min-score", type=float, default=None)
    query_brief_parser.add_argument("--selection-status", choices=["included", "excluded", "unset"], default=None)
    query_brief_parser.add_argument("--cohort", default=None)
    query_brief_parser.add_argument("--subcohort", default=None)

    live_query_brief_parser = subparsers.add_parser(
        "build-live-query-brief",
        help="Run semantic search and assemble a brief in one command.",
    )
    live_query_brief_parser.add_argument("--query", required=True)
    live_query_brief_parser.add_argument("--out", type=Path, default=None)
    live_query_brief_parser.add_argument("--format", choices=["markdown", "json"], default="markdown")
    live_query_brief_parser.add_argument("--timeline-limit", type=int, default=5)
    live_query_brief_parser.add_argument("--asset-limit", type=int, default=3)
    live_query_brief_parser.add_argument("--semantic-limit", type=int, default=5)
    live_query_brief_parser.add_argument("--media-type", choices=["text", "audio", "video"], default=None)
    live_query_brief_parser.add_argument("--score-name", default="feedback_score_v1")
    live_query_brief_parser.add_argument("--min-score", type=float, default=None)
    live_query_brief_parser.add_argument("--namespace", default=None)
    live_query_brief_parser.add_argument(
        "--embedding-kind",
        choices=["asset", "text_chunk", "timeline_segment"],
        default="timeline_segment",
    )
    live_query_brief_parser.add_argument("--rerank-feedback", action="store_true")
    live_query_brief_parser.add_argument("--feedback-weight", type=float, default=0.15)
    live_query_brief_parser.add_argument("--candidate-k", type=int, default=None)
    live_query_brief_parser.add_argument("--min-feedback-score", type=float, default=None)
    live_query_brief_parser.add_argument("--cross-encoder-rerank", action="store_true")
    live_query_brief_parser.add_argument("--cross-encoder-top-k", type=int, default=12)
    live_query_brief_parser.add_argument("--selection-status", choices=["included", "excluded", "unset"], default=None)
    live_query_brief_parser.add_argument("--cohort", default=None)
    live_query_brief_parser.add_argument("--subcohort", default=None)

    generate_draft_parser = subparsers.add_parser(
        "generate-draft-from-brief",
        help="Generate a concept/script draft from an existing brief file.",
    )
    generate_draft_parser.add_argument("--brief", type=Path, required=True)
    generate_draft_parser.add_argument("--out", type=Path, default=None)
    generate_draft_parser.add_argument("--format", choices=["markdown", "json"], default="markdown")
    generate_draft_parser.add_argument("--draft-kind", choices=["concept", "script"], default="concept")
    generate_draft_parser.add_argument(
        "--model",
        default=os.getenv("BANKARA_GENERATION_MODEL", DEFAULT_GENERATION_MODEL),
    )
    generate_draft_parser.add_argument("--temperature", type=float, default=0.6)
    generate_draft_parser.add_argument("--style-notes", default="")

    generate_live_draft_parser = subparsers.add_parser(
        "generate-live-draft",
        help="Run semantic search, build a brief, and generate a draft in one command.",
    )
    generate_live_draft_parser.add_argument("--query", required=True)
    generate_live_draft_parser.add_argument("--out", type=Path, default=None)
    generate_live_draft_parser.add_argument("--format", choices=["markdown", "json"], default="markdown")
    generate_live_draft_parser.add_argument("--draft-kind", choices=["concept", "script"], default="concept")
    generate_live_draft_parser.add_argument(
        "--model",
        default=os.getenv("BANKARA_GENERATION_MODEL", DEFAULT_GENERATION_MODEL),
    )
    generate_live_draft_parser.add_argument("--temperature", type=float, default=0.6)
    generate_live_draft_parser.add_argument("--style-notes", default="")
    generate_live_draft_parser.add_argument("--timeline-limit", type=int, default=5)
    generate_live_draft_parser.add_argument("--asset-limit", type=int, default=3)
    generate_live_draft_parser.add_argument("--semantic-limit", type=int, default=5)
    generate_live_draft_parser.add_argument("--media-type", choices=["text", "audio", "video"], default=None)
    generate_live_draft_parser.add_argument("--score-name", default="feedback_score_v1")
    generate_live_draft_parser.add_argument("--min-score", type=float, default=None)
    generate_live_draft_parser.add_argument("--namespace", default=None)
    generate_live_draft_parser.add_argument(
        "--embedding-kind",
        choices=["asset", "text_chunk", "timeline_segment"],
        default="timeline_segment",
    )
    generate_live_draft_parser.add_argument("--rerank-feedback", action="store_true")
    generate_live_draft_parser.add_argument("--feedback-weight", type=float, default=0.15)
    generate_live_draft_parser.add_argument("--candidate-k", type=int, default=None)
    generate_live_draft_parser.add_argument("--min-feedback-score", type=float, default=None)
    generate_live_draft_parser.add_argument("--cross-encoder-rerank", action="store_true")
    generate_live_draft_parser.add_argument("--cross-encoder-top-k", type=int, default=12)
    generate_live_draft_parser.add_argument("--selection-status", choices=["included", "excluded", "unset"], default=None)
    generate_live_draft_parser.add_argument("--cohort", default=None)
    generate_live_draft_parser.add_argument("--subcohort", default=None)
    generate_live_draft_parser.add_argument("--brief-out", type=Path, default=None)
    generate_live_draft_parser.add_argument("--brief-format", choices=["markdown", "json"], default="json")

    idea_batch_parser = subparsers.add_parser(
        "generate-idea-batch",
        help="Run semantic search, build a brief, and generate multiple new idea candidates in one command.",
    )
    idea_batch_parser.add_argument("--query", required=True)
    idea_batch_parser.add_argument("--out", type=Path, default=None)
    idea_batch_parser.add_argument("--format", choices=["markdown", "json"], default="markdown")
    idea_batch_parser.add_argument("--count", type=int, default=5)
    idea_batch_parser.add_argument(
        "--model",
        default=os.getenv("BANKARA_GENERATION_MODEL", DEFAULT_GENERATION_MODEL),
    )
    idea_batch_parser.add_argument("--temperature", type=float, default=0.7)
    idea_batch_parser.add_argument("--style-notes", default="")
    idea_batch_parser.add_argument("--timeline-limit", type=int, default=5)
    idea_batch_parser.add_argument("--asset-limit", type=int, default=3)
    idea_batch_parser.add_argument("--semantic-limit", type=int, default=5)
    idea_batch_parser.add_argument("--media-type", choices=["text", "audio", "video"], default=None)
    idea_batch_parser.add_argument("--score-name", default="feedback_score_v1")
    idea_batch_parser.add_argument("--min-score", type=float, default=None)
    idea_batch_parser.add_argument("--namespace", default=None)
    idea_batch_parser.add_argument(
        "--embedding-kind",
        choices=["asset", "text_chunk", "timeline_segment"],
        default="timeline_segment",
    )
    idea_batch_parser.add_argument("--rerank-feedback", action="store_true")
    idea_batch_parser.add_argument("--feedback-weight", type=float, default=0.15)
    idea_batch_parser.add_argument("--candidate-k", type=int, default=None)
    idea_batch_parser.add_argument("--min-feedback-score", type=float, default=None)
    idea_batch_parser.add_argument("--cross-encoder-rerank", action="store_true")
    idea_batch_parser.add_argument("--cross-encoder-top-k", type=int, default=12)
    idea_batch_parser.add_argument("--selection-status", choices=["included", "excluded", "unset"], default=None)
    idea_batch_parser.add_argument("--cohort", default=None)
    idea_batch_parser.add_argument("--subcohort", default=None)
    idea_batch_parser.add_argument("--brief-out", type=Path, default=None)
    idea_batch_parser.add_argument("--brief-format", choices=["markdown", "json"], default="json")

    evaluate_draft_parser = subparsers.add_parser(
        "evaluate-draft",
        help="Review a generated draft against a brief using Gemini.",
    )
    evaluate_draft_parser.add_argument("--brief", type=Path, required=True)
    evaluate_draft_parser.add_argument("--draft", type=Path, required=True)
    evaluate_draft_parser.add_argument("--out", type=Path, default=None)
    evaluate_draft_parser.add_argument("--format", choices=["markdown", "json"], default="markdown")
    evaluate_draft_parser.add_argument(
        "--model",
        default=os.getenv("BANKARA_GENERATION_MODEL", DEFAULT_GENERATION_MODEL),
    )
    evaluate_draft_parser.add_argument("--temperature", type=float, default=0.3)

    revise_draft_parser = subparsers.add_parser(
        "revise-draft",
        help="Revise a draft using a brief and review file.",
    )
    revise_draft_parser.add_argument("--brief", type=Path, required=True)
    revise_draft_parser.add_argument("--draft", type=Path, required=True)
    revise_draft_parser.add_argument("--review", type=Path, required=True)
    revise_draft_parser.add_argument("--out", type=Path, default=None)
    revise_draft_parser.add_argument("--format", choices=["markdown", "json"], default="markdown")
    revise_draft_parser.add_argument("--draft-kind", choices=["concept", "script"], default="concept")
    revise_draft_parser.add_argument(
        "--model",
        default=os.getenv("BANKARA_GENERATION_MODEL", DEFAULT_GENERATION_MODEL),
    )
    revise_draft_parser.add_argument("--temperature", type=float, default=0.5)

    run_cycle_parser = subparsers.add_parser(
        "run-cycle",
        help="Run brief -> draft -> review -> revise as one generation cycle.",
    )
    run_cycle_parser.add_argument("--out-dir", type=Path, required=True)
    run_cycle_parser.add_argument("--query", default=None)
    run_cycle_parser.add_argument("--brief", type=Path, default=None)
    run_cycle_parser.add_argument("--search-results", type=Path, default=None)
    run_cycle_parser.add_argument("--iterations", type=int, default=2)
    run_cycle_parser.add_argument("--draft-kind", choices=["concept", "script"], default="concept")
    run_cycle_parser.add_argument("--draft-format", choices=["markdown", "json"], default="markdown")
    run_cycle_parser.add_argument(
        "--model",
        default=os.getenv("BANKARA_GENERATION_MODEL", DEFAULT_GENERATION_MODEL),
    )
    run_cycle_parser.add_argument("--draft-temperature", type=float, default=0.6)
    run_cycle_parser.add_argument("--review-temperature", type=float, default=0.3)
    run_cycle_parser.add_argument("--revision-temperature", type=float, default=0.5)
    run_cycle_parser.add_argument("--style-notes", default="")
    run_cycle_parser.add_argument("--timeline-limit", type=int, default=5)
    run_cycle_parser.add_argument("--asset-limit", type=int, default=3)
    run_cycle_parser.add_argument("--semantic-limit", type=int, default=5)
    run_cycle_parser.add_argument("--media-type", choices=["text", "audio", "video"], default=None)
    run_cycle_parser.add_argument("--score-name", default="feedback_score_v1")
    run_cycle_parser.add_argument("--min-score", type=float, default=None)
    run_cycle_parser.add_argument("--namespace", default=None)
    run_cycle_parser.add_argument(
        "--embedding-kind",
        choices=["asset", "text_chunk", "timeline_segment"],
        default="timeline_segment",
    )
    run_cycle_parser.add_argument("--rerank-feedback", action="store_true")
    run_cycle_parser.add_argument("--feedback-weight", type=float, default=0.15)
    run_cycle_parser.add_argument("--candidate-k", type=int, default=None)
    run_cycle_parser.add_argument("--min-feedback-score", type=float, default=None)
    run_cycle_parser.add_argument("--cross-encoder-rerank", action="store_true")
    run_cycle_parser.add_argument("--cross-encoder-top-k", type=int, default=12)
    run_cycle_parser.add_argument("--selection-status", choices=["included", "excluded", "unset"], default=None)
    run_cycle_parser.add_argument("--cohort", default=None)
    run_cycle_parser.add_argument("--subcohort", default=None)


def build_parser() -> argparse.ArgumentParser:
    """Build a parser that exposes only script-assistant consumer commands."""
    parser = argparse.ArgumentParser(
        description="Bankara Script Assistant: brief, draft, review, and generation-cycle commands.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    register_script_assistant_commands(subparsers)
    return parser


def dispatch_script_assistant_command(
    args: argparse.Namespace,
    *,
    brain: BankaraBrain,
) -> bool:
    """Dispatch script-assistant commands.

    Returns True when the command was handled here.
    """
    if args.command not in SCRIPT_ASSISTANT_COMMANDS:
        return False

    if args.command == "build-query-brief":
        build_query_brief(
            brain=brain,
            query=args.query,
            output_path=args.out.expanduser().resolve() if args.out else None,
            output_format=args.format,
            search_results_path=args.search_results.expanduser().resolve() if args.search_results else None,
            timeline_limit=args.timeline_limit,
            asset_limit=args.asset_limit,
            semantic_limit=args.semantic_limit,
            media_type=args.media_type,
            score_name=args.score_name,
            min_score=args.min_score,
            selection_status=args.selection_status,
            cohort=args.cohort,
            subcohort=args.subcohort,
        )
        return True

    if args.command == "build-live-query-brief":
        build_live_query_brief(
            brain=brain,
            query=args.query,
            output_path=args.out.expanduser().resolve() if args.out else None,
            output_format=args.format,
            timeline_limit=args.timeline_limit,
            asset_limit=args.asset_limit,
            semantic_limit=args.semantic_limit,
            media_type=args.media_type,
            score_name=args.score_name,
            min_score=args.min_score,
            namespace=args.namespace,
            embedding_kind=args.embedding_kind,
            rerank_feedback=args.rerank_feedback,
            feedback_weight=args.feedback_weight,
            candidate_k=args.candidate_k,
            min_feedback_score=args.min_feedback_score,
            cross_encoder_rerank=args.cross_encoder_rerank,
            cross_encoder_top_k=args.cross_encoder_top_k,
            selection_status=args.selection_status,
            cohort=args.cohort,
            subcohort=args.subcohort,
        )
        return True

    if args.command == "generate-draft-from-brief":
        generate_draft_from_brief_file(
            brief_path=args.brief.expanduser().resolve(),
            output_path=args.out.expanduser().resolve() if args.out else None,
            output_format=args.format,
            draft_kind=args.draft_kind,
            model_name=args.model,
            temperature=args.temperature,
            style_notes=args.style_notes,
        )
        return True

    if args.command == "generate-live-draft":
        generate_live_draft(
            brain=brain,
            query=args.query,
            output_path=args.out.expanduser().resolve() if args.out else None,
            output_format=args.format,
            draft_kind=args.draft_kind,
            model_name=args.model,
            temperature=args.temperature,
            style_notes=args.style_notes,
            timeline_limit=args.timeline_limit,
            asset_limit=args.asset_limit,
            semantic_limit=args.semantic_limit,
            media_type=args.media_type,
            score_name=args.score_name,
            min_score=args.min_score,
            namespace=args.namespace,
            embedding_kind=args.embedding_kind,
            rerank_feedback=args.rerank_feedback,
            feedback_weight=args.feedback_weight,
            candidate_k=args.candidate_k,
            min_feedback_score=args.min_feedback_score,
            cross_encoder_rerank=args.cross_encoder_rerank,
            cross_encoder_top_k=args.cross_encoder_top_k,
            selection_status=args.selection_status,
            cohort=args.cohort,
            subcohort=args.subcohort,
            brief_output_path=args.brief_out.expanduser().resolve() if args.brief_out else None,
            brief_output_format=args.brief_format,
        )
        return True

    if args.command == "generate-idea-batch":
        generate_idea_batch(
            brain=brain,
            query=args.query,
            output_path=args.out.expanduser().resolve() if args.out else None,
            output_format=args.format,
            count=args.count,
            model_name=args.model,
            temperature=args.temperature,
            style_notes=args.style_notes,
            timeline_limit=args.timeline_limit,
            asset_limit=args.asset_limit,
            semantic_limit=args.semantic_limit,
            media_type=args.media_type,
            score_name=args.score_name,
            min_score=args.min_score,
            namespace=args.namespace,
            embedding_kind=args.embedding_kind,
            rerank_feedback=args.rerank_feedback,
            feedback_weight=args.feedback_weight,
            candidate_k=args.candidate_k,
            min_feedback_score=args.min_feedback_score,
            cross_encoder_rerank=args.cross_encoder_rerank,
            cross_encoder_top_k=args.cross_encoder_top_k,
            selection_status=args.selection_status,
            cohort=args.cohort,
            subcohort=args.subcohort,
            brief_output_path=args.brief_out.expanduser().resolve() if args.brief_out else None,
            brief_output_format=args.brief_format,
        )
        return True

    if args.command == "evaluate-draft":
        evaluate_draft_from_files(
            brief_path=args.brief.expanduser().resolve(),
            draft_path=args.draft.expanduser().resolve(),
            output_path=args.out.expanduser().resolve() if args.out else None,
            output_format=args.format,
            model_name=args.model,
            temperature=args.temperature,
        )
        return True

    if args.command == "revise-draft":
        revise_draft_from_files(
            brief_path=args.brief.expanduser().resolve(),
            draft_path=args.draft.expanduser().resolve(),
            review_path=args.review.expanduser().resolve(),
            output_path=args.out.expanduser().resolve() if args.out else None,
            output_format=args.format,
            draft_kind=args.draft_kind,
            model_name=args.model,
            temperature=args.temperature,
        )
        return True

    if args.command == "run-cycle":
        run_generation_cycle(
            brain=brain,
            output_dir=args.out_dir.expanduser().resolve(),
            query=args.query,
            brief_path=args.brief.expanduser().resolve() if args.brief else None,
            search_results_path=args.search_results.expanduser().resolve() if args.search_results else None,
            iterations=args.iterations,
            draft_kind=args.draft_kind,
            draft_format=args.draft_format,
            model_name=args.model,
            draft_temperature=args.draft_temperature,
            review_temperature=args.review_temperature,
            revision_temperature=args.revision_temperature,
            style_notes=args.style_notes,
            timeline_limit=args.timeline_limit,
            asset_limit=args.asset_limit,
            semantic_limit=args.semantic_limit,
            media_type=args.media_type,
            score_name=args.score_name,
            min_score=args.min_score,
            namespace=args.namespace,
            embedding_kind=args.embedding_kind,
            rerank_feedback=args.rerank_feedback,
            feedback_weight=args.feedback_weight,
            candidate_k=args.candidate_k,
            min_feedback_score=args.min_feedback_score,
            cross_encoder_rerank=args.cross_encoder_rerank,
            cross_encoder_top_k=args.cross_encoder_top_k,
            selection_status=args.selection_status,
            cohort=args.cohort,
            subcohort=args.subcohort,
        )
        return True

    return False


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        brain = BankaraBrain.from_env()
        if not dispatch_script_assistant_command(args, brain=brain):
            raise RuntimeError(f"Unsupported command: {args.command}")
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
