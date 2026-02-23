"""Qdrant adapter implementing vector store operations.

This adapter is optional and requires `qdrant-client` package installed.
"""

from __future__ import annotations

from typing import Any, Mapping, Optional, Sequence

from ...core.vector_metrics import (
    VectorMetric,
    VectorMetricInput,
    normalize_vector_metric,
)
from ...core.vector_types import VectorRecord, VectorSearchResult


class QdrantVectorStore:
    """Vector store adapter for Qdrant."""

    def __init__(
        self,
        *,
        location: str = ":memory:",
        url: str | None = None,
        api_key: str | None = None,
        prefer_grpc: bool = False,
        timeout: float | None = None,
    ) -> None:
        try:
            from qdrant_client import QdrantClient  # type: ignore[import-not-found]
            from qdrant_client.http import models  # type: ignore[import-not-found]
        except ImportError as exc:  # pragma: no cover - env dependent
            raise ImportError(
                "qdrant-client is required for QdrantVectorStore. "
                "Install with `pip install qdrant-client`."
            ) from exc

        self._models = models

        if url:
            self._client = QdrantClient(
                url=url,
                api_key=api_key,
                prefer_grpc=prefer_grpc,
                timeout=timeout,
            )
        elif location == ":memory:":
            self._client = QdrantClient(":memory:")
        else:
            self._client = QdrantClient(path=location)

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

        normalized_metric = normalize_vector_metric(
            metric,
            supported={VectorMetric.COSINE, VectorMetric.DOT, VectorMetric.L2},
            aliases={"euclid": VectorMetric.L2},
        )

        exists = self._collection_exists(name)
        if exists and not overwrite:
            raise ValueError(f"Collection already exists: {name}")
        if exists:
            self._client.delete_collection(collection_name=name)

        self._client.create_collection(
            collection_name=name,
            vectors_config=self._models.VectorParams(
                size=dimension,
                distance=self._metric_to_distance(normalized_metric),
            ),
        )

    def upsert(self, collection: str, records: Sequence[VectorRecord]) -> None:
        self._ensure_collection(collection)
        points = [
            self._models.PointStruct(
                id=record.id,
                vector=[float(value) for value in record.vector],
                payload=dict(record.payload or {}),
            )
            for record in records
        ]
        if points:
            self._client.upsert(
                collection_name=collection,
                points=points,
                wait=True,
            )

    def query(
        self,
        collection: str,
        vector: Sequence[float],
        *,
        top_k: int = 10,
        filters: Optional[Mapping[str, Any]] = None,
    ) -> list[VectorSearchResult]:
        self._ensure_collection(collection)
        if top_k <= 0:
            return []

        payload_filter = self._build_filter(filters)
        query_vector = [float(value) for value in vector]

        search_fn = getattr(self._client, "search", None)
        if callable(search_fn):
            rows = search_fn(
                collection_name=collection,
                query_vector=query_vector,
                query_filter=payload_filter,
                limit=top_k,
                with_payload=True,
            )
        else:
            query_points = self._client.query_points(
                collection_name=collection,
                query=query_vector,
                query_filter=payload_filter,
                limit=top_k,
                with_payload=True,
            )
            rows = getattr(query_points, "points", query_points)

        return [
            VectorSearchResult(
                id=str(getattr(row, "id")),
                score=float(getattr(row, "score", 0.0)),
                payload=getattr(row, "payload", None),
            )
            for row in rows
        ]

    def fetch(
        self, collection: str, ids: Optional[Sequence[str]] = None
    ) -> list[VectorRecord]:
        self._ensure_collection(collection)
        if ids is None:
            points = self._scroll_all_points(collection)
            return [self._point_to_record(point) for point in points]

        if not ids:
            return []

        rows = self._client.retrieve(
            collection_name=collection,
            ids=list(ids),
            with_vectors=True,
            with_payload=True,
        )
        by_id = {str(getattr(row, "id")): row for row in rows}
        return [
            self._point_to_record(by_id[item_id])
            for item_id in ids
            if item_id in by_id
        ]

    def delete(self, collection: str, ids: Sequence[str]) -> int:
        self._ensure_collection(collection)
        if not ids:
            return 0

        self._client.delete(
            collection_name=collection,
            points_selector=self._models.PointIdsList(points=list(ids)),
            wait=True,
        )
        return len(ids)

    def _scroll_all_points(self, collection: str) -> list[Any]:
        points: list[Any] = []
        offset: Any = None

        while True:
            response = self._client.scroll(
                collection_name=collection,
                offset=offset,
                with_vectors=True,
                with_payload=True,
                limit=256,
            )

            if isinstance(response, tuple):
                batch, next_offset = response
            else:
                batch = getattr(response, "points", [])
                next_offset = getattr(response, "next_page_offset", None)

            points.extend(batch)
            if next_offset is None:
                break
            offset = next_offset

        return points

    def _point_to_record(self, point: Any) -> VectorRecord:
        vector = getattr(point, "vector", None)
        if isinstance(vector, dict):
            named_vectors = vector
            vector = named_vectors.get("embedding")
            if vector is None and named_vectors:
                vector = next(iter(named_vectors.values()))

        return VectorRecord(
            id=str(getattr(point, "id")),
            vector=list(vector or []),
            payload=getattr(point, "payload", None),
        )

    def _build_filter(self, filters: Optional[Mapping[str, Any]]) -> Any:
        if not filters:
            return None

        conditions = [
            self._models.FieldCondition(
                key=str(key),
                match=self._models.MatchValue(value=value),
            )
            for key, value in filters.items()
        ]
        return self._models.Filter(must=conditions)

    def _metric_to_distance(self, metric: VectorMetric) -> Any:
        mapping = {
            VectorMetric.COSINE: self._models.Distance.COSINE,
            VectorMetric.DOT: self._models.Distance.DOT,
            VectorMetric.L2: self._models.Distance.EUCLID,
        }
        return mapping[metric]

    def _ensure_collection(self, name: str) -> None:
        if not self._collection_exists(name):
            raise KeyError(f"Collection does not exist: {name}")

    def _collection_exists(self, name: str) -> bool:
        exists_fn = getattr(self._client, "collection_exists", None)
        if callable(exists_fn):
            return bool(exists_fn(collection_name=name))

        try:
            self._client.get_collection(collection_name=name)
            return True
        except Exception:
            return False
