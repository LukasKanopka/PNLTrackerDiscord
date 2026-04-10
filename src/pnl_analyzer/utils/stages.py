from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Iterator


@contextmanager
def stage(logger, name: str, request_id: str, **fields) -> Iterator[None]:
    t0 = time.perf_counter()
    suffix = " ".join([f"{k}={v}" for k, v in fields.items() if v is not None])
    logger.info("[%s] %s:start %s", request_id, name, suffix)
    try:
        yield
    except Exception:
        logger.exception("[%s] %s:error", request_id, name)
        raise
    finally:
        dt_ms = int((time.perf_counter() - t0) * 1000)
        logger.info("[%s] %s:end duration_ms=%s", request_id, name, dt_ms)
