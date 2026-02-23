"""In-memory vector store adapter for testing and local development."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Sequence

from ...core.vector_metrics import (
    VectorMetric,
    VectorMetricInput,
    normalize_vector_metric,
)
from ...core.vector_types import VectorRecord, VectorSearchResult

SUPPORTED_METRICS = {
    VectorMetric.COSINE,
    VectorMetric.DOT,
    VectorMetric.L2,
}


@dataclass
class _CollectionState:
    dimension: int
    metric: VectorMetric
    records: dict[str, VectorRecord] = field(default_factory=dict)


class InMemoryVectorStore:
    """Simple in-memory implementation of vector database operations."""

    def __init__(self) -> None:
        self._collections: dict[str, _CollectionState] = {}

    def create_collection(
        self,
        name: str,
        dimension: int,
        metric: VectorMetricInput = VectorMetric.COSINE,
        *,
        overwrite: bool = False,
    ) -> None:
        if dimension <= 0:
            raise ValueError("dimension must be > 0")
        normalized_metric = normalize_vector_metric(metric, supported=SUPPORTED_METRICS)
        if name in self._collections and not overwrite:
            raise ValueError(f"Collection already exists: {name}")

        self._collections[name] = _CollectionState(
            dimension=dimension,
            metric=normalized_metric,
        )

    def upsert(self, collection: str, records: Sequence[VectorRecord]) -> None:
        state = self._get_collection(collection)
        for record in records:
            normalized_vector = self._normalize_vector(record.vector, state.dimension)
            payload = dict(record.payload) if record.payload is not None else None
            state.records[record.id] = VectorRecord(
                id=record.id,
                vector=normalized_vector,
                payload=payload,
            )

    def query(
        self,
        collection: str,
        vector: Sequence[float],
        *,
        top_k: int = 10,
        filters: Optional[Mapping[str, Any]] = None,
    ) -> list[VectorSearchResult]:
        if top_k <= 0:
            return []

        state = self._get_collection(collection)
        query_vector = self._normalize_vector(vector, state.dimension)

        scored: list[VectorSearchResult] = []
        for record in state.records.values():
            if not self._match_filters(record.payload, filters):
                continue
            score = self._similarity(state.metric, query_vector, record.vector)
            scored.append(
                VectorSearchResult(id=record.id, score=score, payload=record.payload)
            )

        scored.sort(key=lambda item: item.score, reverse=True)
        return scored[:top_k]

    def fetch(
        self, collection: str, ids: Optional[Sequence[str]] = None
    ) -> list[VectorRecord]:
        state = self._get_collection(collection)
        if ids is None:
            return list(state.records.values())
        return [state.records[item_id] for item_id in ids if item_id in state.records]

    def delete(self, collection: str, ids: Sequence[str]) -> int:
        state = self._get_collection(collection)
        deleted = 0
        for item_id in ids:
            if item_id in state.records:
                del state.records[item_id]
                deleted += 1
        return deleted

    def _get_collection(self, name: str) -> _CollectionState:
        if name not in self._collections:
            raise KeyError(f"Collection does not exist: {name}")
        return self._collections[name]

    @staticmethod
    def _normalize_vector(vector: Sequence[float], dimension: int) -> tuple[float, ...]:
        values = tuple(float(v) for v in vector)
        if len(values) != dimension:
            raise ValueError(
                f"Vector dimension mismatch: expected {dimension}, got {len(values)}"
            )
        return values

    @staticmethod
    def _match_filters(
        payload: Mapping[str, Any] | None,
        filters: Optional[Mapping[str, Any]],
    ) -> bool:
        if not filters:
            return True
        if payload is None:
            return False
        return all(payload.get(key) == value for key, value in filters.items())

    @staticmethod
    def _similarity(
        metric: VectorMetric,
        left: Sequence[float],
        right: Sequence[float],
    ) -> float:
        if metric == VectorMetric.DOT:
            return sum(a * b for a, b in zip(left, right))
        if metric == VectorMetric.L2:
            distance = math.sqrt(sum((a - b) ** 2 for a, b in zip(left, right)))
            return -distance

        # cosine (default)
        dot = sum(a * b for a, b in zip(left, right))
        norm_left = math.sqrt(sum(a * a for a in left))
        norm_right = math.sqrt(sum(b * b for b in right))
        if norm_left == 0.0 or norm_right == 0.0:
            return 0.0
        return dot / (norm_left * norm_right)
