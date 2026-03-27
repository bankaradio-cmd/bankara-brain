# Contract: `P0-CROSS-ENCODER-RERANK`

## Goal

Improve open retrieval precision by reranking top semantic candidates with Gemini after Pinecone returns them.

This packet is intentionally split into:
- Codex: isolated reranker module
- Claude Code Lead: integration into search CLI and benchmark flow

## New Module

- `bankara_cross_encoder_rerank.py`

## Expected Public API

```python
from typing import Any

def rerank_matches_with_gemini(
    query: str,
    matches: list[dict[str, Any]],
    *,
    model: str = "gemini-2.5-flash",
    top_k: int = 12,
) -> list[dict[str, Any]]:
    ...
```

## Candidate Payload Contract

Each candidate passed into reranking must include enough text to judge relevance:
- title
- cohort / subcohort
- searchable summary text if present
- brain summary text if present
- top transcript excerpt if present
- current combined score

## Output Contract

Each returned candidate must preserve the original fields and add:
- `cross_encoder_score`
- `cross_encoder_reason`
- `reranked_combined_score`

## Behavioral Rules

- rerank only top `k`, not full result sets
- no Pinecone writes inside the reranker
- if Gemini fails, return original ranking without crashing
- reasons should be short and factual

## Codex Acceptance

- isolated reranker module exists
- returned candidates include:
  - `cross_encoder_score`
  - `cross_encoder_reason`
  - `reranked_combined_score`
- fallback on Gemini failure is implemented
- one handoff report explains exactly how Claude Lead should integrate it

## Claude Lead Acceptance

- search CLI can enable reranking with a flag
- JSON output includes cross-encoder fields
- benchmark delta is reported on latest50 open benchmark
