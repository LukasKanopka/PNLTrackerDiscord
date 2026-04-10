from __future__ import annotations

import logging

from pnl_analyzer.config import settings


def configure_logging() -> None:
    level_name = (settings.log_level or "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(
            level=level,
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
        )
    else:
        root.setLevel(level)

    logging.getLogger("httpx").setLevel(max(level, logging.WARNING))

