"""Bankara Brain — Centralized logging configuration.

Call ``setup_logging()`` once at application startup (CLI entry-point or
test setup) to wire every ``bankara_brain.*`` logger to a consistent
format.  Individual modules obtain their logger the standard way::

    import logging
    logger = logging.getLogger(__name__)
"""
from __future__ import annotations

import logging
import sys


_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logging(level: int = logging.INFO) -> None:
    """Configure the root ``bankara_brain`` logger.

    * Logs go to **stderr** so they never mix with normal data output on
      stdout.
    * Calling this multiple times is safe — the handler is added only on
      the first invocation.
    """
    root_logger = logging.getLogger("bankara_brain")
    if root_logger.handlers:
        return  # already configured
    root_logger.setLevel(level)
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATE_FORMAT))
    root_logger.addHandler(handler)
