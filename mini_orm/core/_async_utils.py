"""Internal async helpers shared by async modules."""

from __future__ import annotations

import inspect
from typing import Any


async def _maybe_await(value: Any) -> Any:
    """Await awaitables and return non-awaitable values unchanged."""
    if inspect.isawaitable(value):
        return await value
    return value
