#!/usr/bin/env python3
"""Shim — delegates to bankara_brain.embedding.core.

This file exists for backward compatibility. All code now lives in
``bankara_brain.embedding.core``.
"""
from bankara_brain.embedding.core import main  # noqa: F401

if __name__ == "__main__":
    raise SystemExit(main())
