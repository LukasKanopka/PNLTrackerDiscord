from __future__ import annotations

import random
from typing import Any, Callable, Coroutine, TypeVar

from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

T = TypeVar("T")


class UpstreamHTTPError(RuntimeError):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code


async def with_retries(fn: Callable[[], Coroutine[Any, Any, T]]) -> T:
    async for attempt in AsyncRetrying(
        reraise=True,
        stop=stop_after_attempt(6),
        wait=wait_exponential_jitter(initial=0.25, max=10.0),
        retry=retry_if_exception_type((UpstreamHTTPError,)),
    ):
        with attempt:
            return await fn()
    raise RuntimeError("unreachable")

