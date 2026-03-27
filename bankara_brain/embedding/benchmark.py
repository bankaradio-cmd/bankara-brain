"""Retrieval benchmark infrastructure.

Extracted from ``bankara_brain_control_plane.py``.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from bankara_brain.models import Asset, now_utc
from bankara_brain.corpus.query import (
    asset_cohort, asset_selection_status, asset_subcohort,
    effective_cohort_label, normalize_cohort, normalize_match_text,
    normalize_subcohort, resolve_search_match_asset, run_semantic_search_export,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_RETRIEVAL_BENCHMARK_FILE = "retrieval_benchmark_latest50.json"


# ---------------------------------------------------------------------------
# Benchmark helpers
# ---------------------------------------------------------------------------


def retrieval_benchmark_file_path(configured: Path | None = None) -> Path:
    if configured:
        return configured.expanduser().resolve()
    return Path(__file__).resolve().parent.parent.parent / DEFAULT_RETRIEVAL_BENCHMARK_FILE


def normalize_benchmark_case(
    raw_case: dict[str, Any],
    defaults: dict[str, Any],
    index: int,
) -> dict[str, Any]:
    if not isinstance(raw_case, dict):
        raise ValueError(f"Benchmark case #{index + 1} must be an object.")

    query = str(raw_case.get("query") or "").strip()
    if not query:
        raise ValueError(f"Benchmark case #{index + 1} is missing `query`.")

    expected_titles = [
        str(title).strip()
        for title in (raw_case.get("expected_titles") or [])
        if str(title).strip()
    ]
    if not expected_titles:
        raise ValueError(f"Benchmark case #{index + 1} is missing `expected_titles`.")

    resolved_defaults = defaults if isinstance(defaults, dict) else {}
    benchmark_case = {
        "id": str(raw_case.get("id") or f"case-{index + 1:02d}").strip(),
        "query": query,
        "notes": str(raw_case.get("notes") or "").strip(),
        "expected_titles": expected_titles,
        "media_type": str(raw_case.get("media_type") or resolved_defaults.get("media_type") or "video").strip(),
        "selection_status": str(
            raw_case.get("selection_status") or resolved_defaults.get("selection_status") or "included"
        ).strip(),
        "cohort": str(raw_case.get("cohort") or resolved_defaults.get("cohort") or "").strip(),
        "subcohort": str(raw_case.get("subcohort") or resolved_defaults.get("subcohort") or "").strip(),
        "semantic_limit": int(raw_case.get("semantic_limit") or resolved_defaults.get("semantic_limit") or 5),
        "embedding_kind": str(
            raw_case.get("embedding_kind") or resolved_defaults.get("embedding_kind") or "timeline_segment"
        ).strip(),
        "rerank_feedback": bool(
            raw_case.get("rerank_feedback")
            if raw_case.get("rerank_feedback") is not None
            else resolved_defaults.get("rerank_feedback", True)
        ),
        "feedback_weight": float(
            raw_case.get("feedback_weight") or resolved_defaults.get("feedback_weight") or 0.15
        ),
        "candidate_k": raw_case.get("candidate_k", resolved_defaults.get("candidate_k")),
        "min_feedback_score": raw_case.get(
            "min_feedback_score",
            resolved_defaults.get("min_feedback_score"),
        ),
    }
    if benchmark_case["candidate_k"] is not None:
        benchmark_case["candidate_k"] = int(benchmark_case["candidate_k"])
    if benchmark_case["min_feedback_score"] is not None:
        benchmark_case["min_feedback_score"] = float(benchmark_case["min_feedback_score"])
    return benchmark_case


def benchmark_case_matches_filters(
    benchmark_case: dict[str, Any],
    cohort: str | None,
    subcohort: str | None,
    case_ids: list[str] | None,
) -> bool:
    if cohort and normalize_cohort(benchmark_case.get("cohort")) != normalize_cohort(cohort):
        return False
    if subcohort and normalize_subcohort(benchmark_case.get("subcohort")) != normalize_subcohort(subcohort):
        return False
    if case_ids:
        normalized_case_ids = {normalize_match_text(case_id) for case_id in case_ids if case_id.strip()}
        if normalize_match_text(benchmark_case.get("id")) not in normalized_case_ids:
            return False
    return True


def benchmark_title_rank(candidate_titles: list[str], expected_titles: list[str]) -> int | None:
    normalized_expected = [normalize_match_text(title) for title in expected_titles if normalize_match_text(title)]
    if not normalized_expected:
        return None
    for index, title in enumerate(candidate_titles, start=1):
        normalized_title = normalize_match_text(title)
        if not normalized_title:
            continue
        for expected in normalized_expected:
            if normalized_title == expected or expected in normalized_title or normalized_title in expected:
                return index
    return None


def benchmark_title_matches_expected(candidate_title: str, expected_titles: list[str]) -> bool:
    return benchmark_title_rank([candidate_title], expected_titles) == 1


def normalize_effective_lane_label(value: str | None) -> str:
    return (value or "").strip().casefold()


def benchmark_expected_lane_labels(session: Session, expected_titles: list[str]) -> list[str]:
    lanes: list[str] = []
    normalized_expected = [normalize_match_text(title) for title in expected_titles if normalize_match_text(title)]
    if not normalized_expected:
        return lanes

    for asset in session.scalars(select(Asset)).all():
        normalized_title = normalize_match_text(asset.title)
        if not normalized_title:
            continue
        if not any(
            normalized_title == expected or expected in normalized_title or normalized_title in expected
            for expected in normalized_expected
        ):
            continue
        lane = effective_cohort_label(asset_cohort(asset), asset_subcohort(asset))
        if lane and lane not in lanes:
            lanes.append(lane)
    return lanes


def benchmark_title_support_count(session: Session, expected_titles: list[str], media_type: str | None) -> int:
    normalized_expected = {normalize_match_text(title) for title in expected_titles if normalize_match_text(title)}
    if not normalized_expected:
        return 0

    count = 0
    for asset in session.scalars(select(Asset)).all():
        if media_type and asset.media_type != media_type:
            continue
        normalized_title = normalize_match_text(asset.title)
        if not normalized_title:
            continue
        if any(
            normalized_title == expected or expected in normalized_title or normalized_title in expected
            for expected in normalized_expected
        ):
            count += 1
    return count


def benchmark_lane_support_count(
    session: Session,
    expected_lanes: list[str],
    media_type: str | None,
    selection_status: str | None,
) -> int:
    normalized_expected_lanes = {
        normalize_effective_lane_label(lane) for lane in expected_lanes if normalize_effective_lane_label(lane)
    }
    if not normalized_expected_lanes:
        return 0

    count = 0
    for asset in session.scalars(select(Asset)).all():
        if media_type and asset.media_type != media_type:
            continue
        if selection_status and asset_selection_status(asset).casefold() != selection_status.casefold():
            continue
        lane = normalize_effective_lane_label(effective_cohort_label(asset_cohort(asset), asset_subcohort(asset)))
        if lane in normalized_expected_lanes:
            count += 1
    return count


def benchmark_case_effective_lane(
    benchmark_case: dict[str, Any],
    expected_lanes: list[str],
) -> str:
    explicit_lane = effective_cohort_label(benchmark_case.get("cohort"), benchmark_case.get("subcohort"))
    if explicit_lane:
        return explicit_lane
    if len(expected_lanes) == 1:
        return expected_lanes[0]
    return ""


# ---------------------------------------------------------------------------
# Markdown rendering
# ---------------------------------------------------------------------------


def render_retrieval_benchmark_markdown(summary: dict[str, Any]) -> str:
    metrics = summary["metrics"]
    lines = [
        "# Retrieval Benchmark",
        "",
        f"- Benchmark: {summary['benchmark_name']}",
        f"- Cases: {summary['total_cases']}",
        f"- Hit@1: {metrics['hit_at_1']:.3f}",
        f"- Hit@3: {metrics['hit_at_3']:.3f}",
        f"- Hit@{metrics['hit_at_k']}: {metrics['hit_at_k_value']:.3f}",
        f"- MRR: {metrics['mrr']:.3f}",
        f"- Purity@{metrics['purity_window']}: {metrics['purity_at_window']:.3f}",
        f"- Lane Purity@{metrics['purity_window']}: {metrics['lane_purity_at_window']:.3f}",
        f"- Adjusted Purity@{metrics['purity_window']}: {metrics['adjusted_purity_at_window']:.3f}",
        f"- Adjusted Lane Purity@{metrics['purity_window']}: {metrics['adjusted_lane_purity_at_window']:.3f}",
        "",
        "## By Lane",
        "",
        "| Lane | Cases | Hit@1 | Hit@3 | MRR | Purity@3 | Lane Purity@3 | Adj Purity@3 | Adj Lane Purity@3 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for lane in summary["by_lane"]:
        lines.append(
            f"| {lane['lane']} | {lane['cases']} | {lane['hit_at_1']:.3f} | {lane['hit_at_3']:.3f} | {lane['mrr']:.3f} | {lane['purity_at_3']:.3f} | {lane['lane_purity_at_3']:.3f} | {lane['adjusted_purity_at_3']:.3f} | {lane['adjusted_lane_purity_at_3']:.3f} |"
        )

    failing_cases = [case for case in summary["cases"] if case["matched_rank"] is None]
    if failing_cases:
        lines.extend(["", "## Misses", ""])
        for case in failing_cases:
            lines.append(f"### {case['id']}")
            lines.append(f"- Query: {case['query']}")
            lines.append(f"- Lane: {case['effective_cohort'] or '-'}")
            lines.append(f"- Expected: {', '.join(case['expected_titles'])}")
            lines.append(f"- Top titles: {', '.join(case['top_titles']) if case['top_titles'] else '-'}")
            if case["notes"]:
                lines.append(f"- Notes: {case['notes']}")
            lines.append("")

    purity_cases = [
        case
        for case in summary["cases"]
        if case["purity_at_3"] < 1.0 or case["lane_purity_at_3"] < 1.0
    ]
    if purity_cases:
        lines.extend(["## Purity Drops", ""])
        for case in purity_cases:
            lines.append(f"### {case['id']}")
            lines.append(f"- Query: {case['query']}")
            lines.append(f"- Lane: {case['effective_cohort'] or '-'}")
            lines.append(f"- Purity@3: {case['purity_at_3']:.3f}")
            lines.append(f"- Lane Purity@3: {case['lane_purity_at_3']:.3f}")
            lines.append(
                f"- Adjusted Purity@3: {case['adjusted_purity_at_3']:.3f} "
                f"(ceiling={case['purity_ceiling_at_3']:.3f})"
            )
            lines.append(
                f"- Adjusted Lane Purity@3: {case['adjusted_lane_purity_at_3']:.3f} "
                f"(ceiling={case['lane_purity_ceiling_at_3']:.3f})"
            )
            lines.append(f"- Top titles: {', '.join(case['top_titles']) if case['top_titles'] else '-'}")
            if case.get("top_lanes"):
                lines.append(f"- Top lanes: {', '.join(case['top_lanes'])}")
            lines.append("")

    lines.extend(["## Cases", ""])
    for case in summary["cases"]:
        lines.append(f"### {case['id']}")
        lines.append(f"- Query: {case['query']}")
        lines.append(f"- Lane: {case['effective_cohort'] or '-'}")
        if case.get("expected_lanes"):
            lines.append(f"- Expected lanes: {', '.join(case['expected_lanes'])}")
        lines.append(f"- Expected: {', '.join(case['expected_titles'])}")
        lines.append(
            f"- Rank: {case['matched_rank'] if case['matched_rank'] is not None else 'miss'} "
            f"(hit@1={int(case['hit_at_1'])}, hit@3={int(case['hit_at_3'])}, mrr={case['mrr']:.3f})"
        )
        lines.append(
            f"- Purity@3: {case['purity_at_3']:.3f} / Lane Purity@3: {case['lane_purity_at_3']:.3f}"
        )
        lines.append(
            f"- Adjusted Purity@3: {case['adjusted_purity_at_3']:.3f} "
            f"(ceiling={case['purity_ceiling_at_3']:.3f}) / "
            f"Adjusted Lane Purity@3: {case['adjusted_lane_purity_at_3']:.3f} "
            f"(ceiling={case['lane_purity_ceiling_at_3']:.3f})"
        )
        lines.append(f"- Top titles: {', '.join(case['top_titles']) if case['top_titles'] else '-'}")
        if case.get("top_lanes"):
            lines.append(f"- Top lanes: {', '.join(case['top_lanes'])}")
        if case["notes"]:
            lines.append(f"- Notes: {case['notes']}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


# ---------------------------------------------------------------------------
# Main benchmark runner
# ---------------------------------------------------------------------------


def run_retrieval_benchmark(
    session_factory: sessionmaker[Session],
    benchmark_path: Path | None,
    output_path: Path | None,
    output_format: str,
    namespace: str | None,
    semantic_limit: int | None,
    media_type: str | None,
    embedding_kind: str | None,
    rerank_feedback: bool | None,
    feedback_weight: float | None,
    candidate_k: int | None,
    min_feedback_score: float | None,
    cross_encoder_rerank: bool | None,
    cross_encoder_top_k: int | None,
    selection_status: str | None,
    cohort: str | None,
    subcohort: str | None,
    case_ids: list[str] | None,
) -> None:
    resolved_benchmark_path = retrieval_benchmark_file_path(benchmark_path)
    payload = json.loads(resolved_benchmark_path.read_text(encoding="utf-8"))
    defaults = payload.get("defaults") or {}
    raw_cases = payload.get("cases")
    if not isinstance(raw_cases, list) or not raw_cases:
        raise ValueError(f"Benchmark file does not define any cases: {resolved_benchmark_path}")

    benchmark_cases = [
        normalize_benchmark_case(raw_case, defaults=defaults, index=index)
        for index, raw_case in enumerate(raw_cases)
    ]
    benchmark_cases = [
        case
        for case in benchmark_cases
        if benchmark_case_matches_filters(case, cohort=cohort, subcohort=subcohort, case_ids=case_ids)
    ]
    if not benchmark_cases:
        raise ValueError("No benchmark cases matched the requested filters.")

    purity_window = min(3, semantic_limit or max(case["semantic_limit"] for case in benchmark_cases) or 3)
    case_results: list[dict[str, Any]] = []
    for benchmark_case in benchmark_cases:
        case_output_path = Path(
            tempfile.NamedTemporaryFile(prefix="bankara_benchmark_", suffix=".json", delete=False).name
        )
        try:
            run_semantic_search_export(
                session_factory=session_factory,
                query=benchmark_case["query"],
                output_path=case_output_path,
                semantic_limit=semantic_limit or benchmark_case["semantic_limit"],
                media_type=media_type or benchmark_case["media_type"],
                namespace=namespace,
                embedding_kind=embedding_kind or benchmark_case["embedding_kind"],
                rerank_feedback=rerank_feedback if rerank_feedback is not None else benchmark_case["rerank_feedback"],
                feedback_weight=feedback_weight if feedback_weight is not None else benchmark_case["feedback_weight"],
                candidate_k=candidate_k if candidate_k is not None else benchmark_case["candidate_k"],
                min_feedback_score=(
                    min_feedback_score if min_feedback_score is not None else benchmark_case["min_feedback_score"]
                ),
                cross_encoder_rerank=(
                    cross_encoder_rerank
                    if cross_encoder_rerank is not None
                    else bool(benchmark_case.get("cross_encoder_rerank", False))
                ),
                cross_encoder_top_k=(
                    cross_encoder_top_k
                    if cross_encoder_top_k is not None
                    else int(benchmark_case.get("cross_encoder_top_k", 12))
                ),
                selection_status=selection_status or benchmark_case["selection_status"],
                cohort=benchmark_case["cohort"],
                subcohort=benchmark_case["subcohort"],
            )
            search_payload = json.loads(case_output_path.read_text(encoding="utf-8"))
        finally:
            case_output_path.unlink(missing_ok=True)

        matches = search_payload.get("matches", []) if isinstance(search_payload, dict) else []
        with session_factory() as session:
            top_titles: list[str] = []
            top_lanes: list[str] = []
            for match in matches:
                if not isinstance(match, dict):
                    continue
                asset = resolve_search_match_asset(session, match)
                title = asset.title if asset else str((match.get("metadata") or {}).get("title") or "")
                lane = ""
                if asset:
                    lane = effective_cohort_label(asset_cohort(asset), asset_subcohort(asset))
                elif isinstance(match.get("metadata"), dict):
                    metadata = match.get("metadata") or {}
                    lane = effective_cohort_label(
                        str(metadata.get("curation_cohort") or ""),
                        str(metadata.get("curation_subcohort") or ""),
                    )
                top_titles.append(title)
                top_lanes.append(lane)

            expected_lanes = benchmark_expected_lane_labels(session, benchmark_case["expected_titles"])
            title_support_count = benchmark_title_support_count(
                session,
                benchmark_case["expected_titles"],
                media_type=media_type or benchmark_case["media_type"],
            )
            lane_support_count = benchmark_lane_support_count(
                session,
                expected_lanes,
                media_type=media_type or benchmark_case["media_type"],
                selection_status=selection_status or benchmark_case["selection_status"],
            )

        unique_top_titles: list[str] = []
        unique_top_lanes: list[str] = []
        seen_titles: set[str] = set()
        for title, lane in zip(top_titles, top_lanes):
            normalized_title = normalize_match_text(title)
            if not normalized_title or normalized_title in seen_titles:
                continue
            seen_titles.add(normalized_title)
            unique_top_titles.append(title)
            unique_top_lanes.append(lane)

        hit_limit = semantic_limit or benchmark_case["semantic_limit"]
        matched_rank = benchmark_title_rank(unique_top_titles[:hit_limit], benchmark_case["expected_titles"])
        effective_cohort = benchmark_case_effective_lane(benchmark_case, expected_lanes)
        top_titles_for_purity = unique_top_titles[:purity_window]
        top_lanes_for_purity = unique_top_lanes[:purity_window]
        purity_hits = sum(
            1 for title in top_titles_for_purity if benchmark_title_matches_expected(title, benchmark_case["expected_titles"])
        )
        normalized_expected_lanes = {
            normalize_effective_lane_label(lane) for lane in expected_lanes if normalize_effective_lane_label(lane)
        }
        lane_purity_hits = sum(
            1
            for lane in top_lanes_for_purity
            if normalize_effective_lane_label(lane) in normalized_expected_lanes
        )
        purity_denominator = max(len(top_titles_for_purity), 1)
        lane_purity_denominator = max(len(top_lanes_for_purity), 1)
        purity_at_3 = round(purity_hits / purity_denominator, 4)
        lane_purity_at_3 = (
            round(lane_purity_hits / lane_purity_denominator, 4)
            if normalized_expected_lanes
            else 0.0
        )
        purity_ceiling_at_3 = round(min(title_support_count, purity_denominator) / purity_denominator, 4)
        lane_purity_ceiling_at_3 = (
            round(min(lane_support_count, lane_purity_denominator) / lane_purity_denominator, 4)
            if normalized_expected_lanes
            else 0.0
        )
        adjusted_purity_at_3 = (
            round(min(purity_at_3 / purity_ceiling_at_3, 1.0), 4)
            if purity_ceiling_at_3 > 0
            else 0.0
        )
        adjusted_lane_purity_at_3 = (
            round(min(lane_purity_at_3 / lane_purity_ceiling_at_3, 1.0), 4)
            if lane_purity_ceiling_at_3 > 0
            else 0.0
        )
        case_results.append(
            {
                "id": benchmark_case["id"],
                "query": benchmark_case["query"],
                "notes": benchmark_case["notes"],
                "expected_titles": benchmark_case["expected_titles"],
                "cohort": benchmark_case["cohort"],
                "subcohort": benchmark_case["subcohort"],
                "effective_cohort": effective_cohort,
                "matched_rank": matched_rank,
                "hit_at_1": matched_rank == 1,
                "hit_at_3": matched_rank is not None and matched_rank <= 3,
                "hit_at_k": matched_rank is not None and matched_rank <= hit_limit,
                "mrr": round(1.0 / matched_rank, 4) if matched_rank else 0.0,
                "semantic_limit": hit_limit,
                "top_titles": unique_top_titles[:hit_limit],
                "top_lanes": unique_top_lanes[:hit_limit],
                "expected_lanes": expected_lanes,
                "purity_at_3": purity_at_3,
                "lane_purity_at_3": lane_purity_at_3,
                "title_support_count": title_support_count,
                "lane_support_count": lane_support_count,
                "purity_ceiling_at_3": purity_ceiling_at_3,
                "lane_purity_ceiling_at_3": lane_purity_ceiling_at_3,
                "adjusted_purity_at_3": adjusted_purity_at_3,
                "adjusted_lane_purity_at_3": adjusted_lane_purity_at_3,
            }
        )

    total_cases = len(case_results)
    hit_at_1_total = sum(1 for case in case_results if case["hit_at_1"])
    hit_at_3_total = sum(1 for case in case_results if case["hit_at_3"])
    hit_at_k_total = sum(1 for case in case_results if case["hit_at_k"])
    mrr_total = sum(case["mrr"] for case in case_results)
    purity_total = sum(case["purity_at_3"] for case in case_results)
    lane_purity_total = sum(case["lane_purity_at_3"] for case in case_results)
    adjusted_purity_total = sum(case["adjusted_purity_at_3"] for case in case_results)
    adjusted_lane_purity_total = sum(case["adjusted_lane_purity_at_3"] for case in case_results)

    by_lane_rows: list[dict[str, Any]] = []
    grouped_lanes: dict[str, list[dict[str, Any]]] = {}
    for case in case_results:
        grouped_lanes.setdefault(case["effective_cohort"] or "-", []).append(case)
    for lane, lane_cases in sorted(grouped_lanes.items()):
        lane_total = len(lane_cases)
        by_lane_rows.append(
            {
                "lane": lane,
                "cases": lane_total,
                "hit_at_1": round(sum(1 for case in lane_cases if case["hit_at_1"]) / lane_total, 4),
                "hit_at_3": round(sum(1 for case in lane_cases if case["hit_at_3"]) / lane_total, 4),
                "mrr": round(sum(case["mrr"] for case in lane_cases) / lane_total, 4),
                "purity_at_3": round(sum(case["purity_at_3"] for case in lane_cases) / lane_total, 4),
                "lane_purity_at_3": round(sum(case["lane_purity_at_3"] for case in lane_cases) / lane_total, 4),
                "adjusted_purity_at_3": round(sum(case["adjusted_purity_at_3"] for case in lane_cases) / lane_total, 4),
                "adjusted_lane_purity_at_3": round(
                    sum(case["adjusted_lane_purity_at_3"] for case in lane_cases) / lane_total,
                    4,
                ),
            }
        )

    hit_limit = semantic_limit or max(case["semantic_limit"] for case in case_results) or 5
    summary = {
        "benchmark_name": payload.get("name") or resolved_benchmark_path.stem,
        "benchmark_path": str(resolved_benchmark_path),
        "generated_at": now_utc().isoformat(),
        "total_cases": total_cases,
        "metrics": {
            "hit_at_1": round(hit_at_1_total / total_cases, 4),
            "hit_at_3": round(hit_at_3_total / total_cases, 4),
            "hit_at_k": hit_limit,
            "hit_at_k_value": round(hit_at_k_total / total_cases, 4),
            "mrr": round(mrr_total / total_cases, 4),
            "purity_window": purity_window,
            "purity_at_window": round(purity_total / total_cases, 4),
            "lane_purity_at_window": round(lane_purity_total / total_cases, 4),
            "adjusted_purity_at_window": round(adjusted_purity_total / total_cases, 4),
            "adjusted_lane_purity_at_window": round(adjusted_lane_purity_total / total_cases, 4),
        },
        "by_lane": by_lane_rows,
        "cases": case_results,
    }

    if output_format == "json":
        rendered = json.dumps(summary, ensure_ascii=False, indent=2)
    else:
        rendered = render_retrieval_benchmark_markdown(summary)

    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered, encoding="utf-8")
        print(f"Wrote retrieval benchmark: {output_path}")
        return

    print(rendered)
