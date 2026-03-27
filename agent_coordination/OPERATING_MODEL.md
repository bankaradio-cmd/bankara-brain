# Parallel Agent Operating Model

This folder is the single source of truth for running Bankara Brain with parallel agents.

Goal:
- keep retrieval/feedback quality work moving in parallel
- avoid collisions in giant files
- force every parallel task to have a contract, owner, and handoff report

## Roles

Default operating mode is `2 parallel agents`, not `3`.

Start with:
- Claude Code Lead
- Codex

Only add a second Claude session after Phase 1 is stable and a separate branch or worktree is ready.

### Claude Code Lead
- Owns final integration and signoff
- Owns edits to:
  - `bankara_brain_control_plane.py`
  - `cohort_rules.json`
  - `README.md`
  - retrieval benchmark definitions
- Decides schema changes, CLI changes, and benchmark acceptance

### Claude Code Multimodal
- Optional third agent
- Owns multimodal analysis design and validation when explicitly activated
- Drives shot-based visual/audio understanding
- Must run in a separate branch or worktree from Claude Code Lead
- Should prefer adding isolated modules over editing the control plane directly

### Codex
- Owns isolated module implementation, helper scripts, and tests
- Should avoid editing giant orchestrator files unless the active packet explicitly allows it
- Should package work so Claude Lead can merge it without re-deriving intent

## Shared Files

Always read these before claiming work:

- `agent_coordination/WORK_QUEUE.md`
- `agent_coordination/work_packets.json`
- `agent_coordination/checkpoints/ACTIVE_WORK.md`
- `agent_coordination/contracts/*.md`
- latest report in `agent_coordination/reports/`

## Claim Protocol

1. Read the packet and its contract.
2. Add or update a row in `agent_coordination/checkpoints/ACTIVE_WORK.md`.
3. Touch only the files listed in the packet unless the owner extends scope in a report.
4. If a packet requires a giant-file edit, only one agent may own that file at a time.
5. When done, write a report in `agent_coordination/reports/`.

## File Ownership Rules

### Exclusive to Claude Lead
- `bankara_brain_control_plane.py`
- `cohort_rules.json`
- `README.md`
- `retrieval_benchmark_latest50.json`
- `retrieval_benchmark_latest50_open.json`

### Contract-gated / one packet at a time
- `gemini_pinecone_multimodal_mvp.py`

### Safe parallel area
- new `bankara_*.py` modules
- `tests/`
- `agent_coordination/`
- `ui_mock/`

## Reporting Rules

Every meaningful chunk of work must end with a report.

Use this status vocabulary:
- `ready`
- `in_progress`
- `blocked`
- `handoff`
- `done`

Every report must include:
- packet id
- touched files
- commands/tests run
- outputs generated
- blockers or risks
- exact next owner

## Recommended Cadence

- claim report at start
- checkpoint every 60-90 minutes or at a clean boundary
- handoff report immediately after acceptance criteria are met

## Merge Discipline

- no agent edits `bankara_brain_control_plane.py` in parallel with another agent
- no agent changes score names, JSON field names, or benchmark semantics without updating the matching contract
- new modules should be additive first; integration into core files happens second

## Current Default Split

### Phase 1
- Claude Code Lead:
  - `P0-FEEDBACK-V2`
- Codex:
  - `P0-CROSS-ENCODER-RERANK`
  - module implementation only

### Phase 2
- Claude Code Lead:
  - integrate `P0-CROSS-ENCODER-RERANK`
- Codex:
  - start `P0-VISUAL-AUDIO-SUMMARY` as an isolated module

### Phase 3
- Claude Code Lead:
  - integrate `P0-VISUAL-AUDIO-SUMMARY`
- Claude Code Multimodal:
  - only if a separate branch/worktree is already active
