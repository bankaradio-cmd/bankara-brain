# Work Queue

This queue is ordered by expected impact on Bankara Brain precision.

## P0 Now

| Packet | Owner | Goal | Touch Files | Depends On | Done When |
| --- | --- | --- | --- | --- | --- |
| `P0-FEEDBACK-V2` | Claude Code Lead | Replace fixed `feedback_score_v1` weighting with hook/CTR/engagement/recency-aware scoring. | `bankara_brain_control_plane.py`, `README.md`, new `bankara_feedback_v2.py` | none | `feedback_score_v2` is written for asset/timeline, diagnostics show distribution, search can rerank on it. |
| `P0-CROSS-ENCODER-RERANK` | Codex | Add Gemini reranking over semantic candidates to improve open retrieval purity. | new `bankara_cross_encoder_rerank.py`, tests/docs | none | isolated reranker module exists, tested, and is ready for Claude Lead integration. |

## P0 Phase 2

| Packet | Owner | Goal | Touch Files | Depends On | Done When |
| --- | --- | --- | --- | --- | --- |
| `P0-VISUAL-AUDIO-SUMMARY` | Codex or Claude Code Multimodal | Build shot-based visual/audio analysis pipeline for structured summaries. | new `bankara_visual_audio_summary.py`, optional helper modules, docs/contracts | `P0-FEEDBACK-V2`, `P0-CROSS-ENCODER-RERANK` | 5 real videos produce valid structured summary JSON and searchable summary text. |

## P1 Next

| Packet | Owner | Goal | Touch Files | Depends On | Done When |
| --- | --- | --- | --- | --- | --- |
| `P1-DRAFT-OUTCOMES` | Claude Code Lead | Close the loop between generated ideas and published results. | `bankara_brain_control_plane.py`, DB schema docs | `P0-FEEDBACK-V2` | draft outcome table exists and can join generated drafts to real assets and scores. |
| `P1-BGM-CATALOG` | Codex | Build reusable BGM/SE catalog from existing videos. | new `bankara_bgm_catalog.py`, helper scripts/tests | `P0-VISUAL-AUDIO-SUMMARY` | BGM segments can be extracted, embedded, and scored against successful beats. |
| `P1-HYDE` | Codex | Add hypothetical document query expansion for hard open queries. | new `bankara_hyde.py`, `gemini_pinecone_multimodal_mvp.py` | `P0-CROSS-ENCODER-RERANK` | open benchmark improves on hard lexical-gap cases without harming lane-fixed cases. |

## P2 Later

| Packet | Owner | Goal | Touch Files | Depends On | Done When |
| --- | --- | --- | --- | --- | --- |
| `P2-CTR-THUMB-TITLE` | Claude Code + Codex | Learn thumbnail/title patterns that drive CTR. | analyzer modules + reporting docs | `P0-FEEDBACK-V2` | CTR diagnostics produce actionable features for draft generation. |
| `P2-TREND-COMPETITOR-PRIORS` | Codex | Add external priors without polluting core corpus. | new external analysis modules | none | trend/competitor outputs are available as side inputs, not as core retrieval data. |

## Current Recommended Sequence

1. `P0-FEEDBACK-V2`
2. `P0-CROSS-ENCODER-RERANK`
3. Claude Lead integrates reranker and reruns benchmark
4. `P0-VISUAL-AUDIO-SUMMARY`
5. benchmark rerun and integration signoff
6. `P1-DRAFT-OUTCOMES`
7. `P1-BGM-CATALOG`
8. `P1-HYDE`

## Non-Negotiables

- Every packet must point at a contract file.
- Every packet must end with a report.
- No packet may silently mutate score semantics or benchmark logic.
