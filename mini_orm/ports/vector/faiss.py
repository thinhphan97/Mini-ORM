"""Faiss adapter implementing vector store operations.

This adapter is optional and requires `faiss-cpu` and `numpy` packages installed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Sequence

from ...core.vector_metrics import (
    VectorMetric,
    VectorMetricInput,
    normalize_vector_metric,
)
from ...core.vector_policies import VectorIdPolicy
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
    index: Any
    ext_to_int: dict[str, int] = field(default_factory=dict)
    int_to_ext: dict[int, str] = field(default_factory=dict)
    records: dict[str, VectorRecord] = field(default_factory=dict)
    next_internal_id: int = 1


class FaissVectorStore:
    """Vector store adapter for Facebook AI Similarity Search (Faiss)."""
    supports_filters = False
    id_policy = VectorIdPolicy.ANY

    def __init__(self) -> None:
        try:
            import faiss  # type: ignore[import-not-found]
            import numpy as np  # type: ignore[import-not-found]
        except ImportError as exc:  # pragma: no cover - env dependent
            raise ImportError(
                "faiss-cpu and numpy are required for FaissVectorStore. "
                "Install with `pip install faiss-cpu numpy`."
            ) from exc

        self._faiss = faiss
        self._np = np
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

        if normalized_metric in {VectorMetric.COSINE, VectorMetric.DOT}:
            base_index = self._faiss.IndexFlatIP(dimension)
        else:
            base_index = self._faiss.IndexFlatL2(dimension)

        index = self._faiss.IndexIDMap2(base_index)
        self._collections[name] = _CollectionState(
            dimension=dimension,
            metric=normalized_metric,
            index=index,
        )

    def upsert(self, collection: str, records: Sequence[VectorRecord]) -> None:
        state = self._get_collection(collection)
        if not records:
            return

        ids_to_remove: list[int] = []
        vectors_to_add: list[list[float]] = []
        int_ids_to_add: list[int] = []

        for record in records:
            vector = self._normalize_vector(record.vector, state.dimension)

            existing_id = state.ext_to_int.get(record.id)
            if existing_id is not None:
                ids_to_remove.append(existing_id)
                internal_id = existing_id
            else:
                internal_id = state.next_internal_id
                state.next_internal_id += 1

            vectors_to_add.append(vector)
            int_ids_to_add.append(internal_id)
            state.ext_to_int[record.id] = internal_id
            state.int_to_ext[internal_id] = record.id
            state.records[record.id] = VectorRecord(
                id=record.id,
                vector=vector,
                payload=dict(record.payload) if record.payload is not None else None,
            )

        if ids_to_remove:
            id_array = self._np.array(ids_to_remove, dtype=self._np.int64)
            state.index.remove_ids(id_array)

        vector_array = self._np.array(vectors_to_add, dtype=self._np.float32)
        if state.metric == VectorMetric.COSINE:
            self._faiss.normalize_L2(vector_array)

        id_array = self._np.array(int_ids_to_add, dtype=self._np.int64)
        state.index.add_with_ids(vector_array, id_array)

    def query(
        self,
        collection: str,
        vector: Sequence[float],
        *,
        top_k: int = 10,
        filters: Optional[Mapping[str, Any]] = None,
    ) -> list[VectorSearchResult]:
        if filters:
            raise NotImplementedError(
                "FaissVectorStore does not support payload filters in query()."
            )

        if top_k <= 0:
            return []

        state = self._get_collection(collection)
        if not state.records:
            return []

        query_vector = self._np.array(
            [self._normalize_vector(vector, state.dimension)],
            dtype=self._np.float32,
        )
        if state.metric == VectorMetric.COSINE:
            self._faiss.normalize_L2(query_vector)

        distances, internal_ids = state.index.search(query_vector, top_k)

        results: list[VectorSearchResult] = []
        for distance, internal_id in zip(distances[0], internal_ids[0]):
            if internal_id == -1:
                continue
            external_id = state.int_to_ext.get(int(internal_id))
            if external_id is None:
                continue
            record = state.records.get(external_id)
            if record is None:
                continue
            results.append(
                VectorSearchResult(
                    id=external_id,
                    score=self._distance_to_score(state.metric, float(distance)),
                    payload=record.payload,
                )
            )
        return results

    def fetch(
        self, collection: str, ids: Optional[Sequence[str]] = None
    ) -> list[VectorRecord]:
        state = self._get_collection(collection)
        if ids is None:
            return list(state.records.values())
        return [state.records[item_id] for item_id in ids if item_id in state.records]

    def delete(self, collection: str, ids: Sequence[str]) -> int:
        state = self._get_collection(collection)
        delete_ids: list[int] = []

        for item_id in ids:
            internal_id = state.ext_to_int.pop(item_id, None)
            if internal_id is None:
                continue
            state.int_to_ext.pop(internal_id, None)
            state.records.pop(item_id, None)
            delete_ids.append(internal_id)

        if delete_ids:
            id_array = self._np.array(delete_ids, dtype=self._np.int64)
            state.index.remove_ids(id_array)

        return len(delete_ids)

    def _get_collection(self, name: str) -> _CollectionState:
        if name not in self._collections:
            raise KeyError(f"Collection does not exist: {name}")
        return self._collections[name]

    @staticmethod
    def _normalize_vector(vector: Sequence[float], dimension: int) -> list[float]:
        values = [float(value) for value in vector]
        if len(values) != dimension:
            raise ValueError(
                f"Vector dimension mismatch: expected {dimension}, got {len(values)}"
            )
        return values

    @staticmethod
    def _distance_to_score(metric: VectorMetric, distance: float) -> float:
        if metric == VectorMetric.L2:
            return -distance
        return distance
