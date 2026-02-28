from __future__ import annotations

import time
import unittest
from typing import Callable, TypeVar

T = TypeVar("T")


def wait_for_service_or_skip(
    *,
    service_name: str,
    endpoint: str,
    initializer: Callable[[], T],
    retries: int = 20,
    delay_seconds: float = 1.0,
) -> T:
    """Retry service initialization and skip test class when unavailable."""

    last_exc: Exception | None = None
    for _ in range(retries):
        try:
            return initializer()
        except Exception as exc:
            last_exc = exc
            time.sleep(delay_seconds)

    raise unittest.SkipTest(
        f"{service_name} host is not reachable at {endpoint}: {last_exc}"
    ) from last_exc
