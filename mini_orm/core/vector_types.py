"""Shared vector entities used by vector ports and repositories."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence


@dataclass(frozen=True)
class VectorRecord:
    """Represents one vector document stored in a vector database."""

    id: str
    vector: Sequence[float]
    payload: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class VectorSearchResult:
    """Represents one scored search hit returned by vector similarity query."""

    id: str
    score: float
    payload: Mapping[str, Any] | None = None
