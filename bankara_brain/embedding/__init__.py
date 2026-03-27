"""Bankara Brain — Embedding infrastructure.

Sub-modules:
    config      Constants, tag dictionaries, and data classes
    client      Gemini & Pinecone client initialization, retry logic
    vectors     Core embedding generation (text & media)
    media       Media file preparation, trimming, clip extraction
    store       Pinecone upsert/delete, metadata processing, state management
    search      Semantic search: facets, matching, ranking, output
    ingestion   Directory and manifest ingestion pipelines
    core        CLI entry point (smoke test + argparse dispatcher)
    manifest    DB ↔ manifest export/import (used by Brain CLI)
    sync        Pinecone metadata synchronization (used by Brain CLI)
    benchmark   Retrieval benchmark (used by Brain CLI)
    rerank      Cross-encoder re-ranking via Gemini
"""
