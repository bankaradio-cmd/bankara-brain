# Bankara Brain UI Mock

## Purpose

This mock is a static concept page for the first usable UI of Bankara Brain.

The intent is to show four things on one surface:

1. corpus health
2. cohort-aware retrieval
3. timeline-level winning patterns
4. idea batch output

The current mock also includes a real video shelf and an interactive episode lens:

- play actual corpus videos in-browser
- switch the inspected episode from the shelf
- jump directly to hook / beat / payoff timings

## Main Screens Combined In This Mock

### 1. Query Studio

The operator starts with a planning query and narrows to a cohort before retrieval.

### 2. Pattern Radar

The UI emphasizes strong hooks, beats, and payoff segments before showing raw transcript.

### 3. Episode Lens

Each episode is treated as a pattern source, not just a file. The first question is:
"which role of this episode worked?"

### 4. Draft Lab

This is the production bridge from retrieval to planning. The team compares generated concepts
instead of looking at one draft in isolation.

### 5. Ops Feed

The backend already has real health signals. The UI should surface them continuously so the team
knows whether the brain is trustworthy before using its output.

## Suggested Build Order

1. Query Studio
2. Pattern Radar
3. Draft Lab
4. Ops Feed
5. Episode Lens

## Current Mock File

- `ui_mock/index.html`
