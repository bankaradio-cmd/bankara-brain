# Bootstrap Prompts

Use these as copy-paste starting prompts for each agent.

## Claude Code Lead

Read:
- `agent_coordination/OPERATING_MODEL.md`
- `agent_coordination/WORK_QUEUE.md`
- `agent_coordination/contracts/feedback_score_v2.md`
- latest file in `agent_coordination/reports/`

Claim `P0-FEEDBACK-V2` in `agent_coordination/checkpoints/ACTIVE_WORK.md`.

Implement the packet as the lead integrator. Prefer a new module `bankara_feedback_v2.py`, then wire it into the control plane. Keep `feedback_score_v1` backward compatible. End with a handoff report in `agent_coordination/reports/`.

## Claude Code Multimodal

Read:
- `agent_coordination/OPERATING_MODEL.md`
- `agent_coordination/WORK_QUEUE.md`
- `agent_coordination/contracts/visual_audio_summary_v2.md`
- latest file in `agent_coordination/reports/`

Do not start this packet until Phase 1 is done and a separate branch/worktree is ready.

Claim `P0-VISUAL-AUDIO-SUMMARY` in `agent_coordination/checkpoints/ACTIVE_WORK.md`.

Implement a shot-based visual/audio summary module. Do not use fixed 2-second windows as the primary segmentation rule. Keep work additive and avoid editing the control plane until the module contract is proven. End with a handoff report in `agent_coordination/reports/`.

## Codex

Read:
- `agent_coordination/OPERATING_MODEL.md`
- `agent_coordination/WORK_QUEUE.md`
- `agent_coordination/contracts/cross_encoder_rerank.md`
- latest file in `agent_coordination/reports/`

Claim `P0-CROSS-ENCODER-RERANK` in `agent_coordination/checkpoints/ACTIVE_WORK.md`.

Implement the reranker as an isolated module. Do not edit `gemini_pinecone_multimodal_mvp.py` in Phase 1. Preserve fallback behavior on API failure. End with a handoff report in `agent_coordination/reports/` that tells Claude Lead exactly how to integrate it.
