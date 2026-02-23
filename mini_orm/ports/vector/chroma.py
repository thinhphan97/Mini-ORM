"""Chroma adapter implementing vector store operations.

This adapter is optional and requires `chromadb` package installed.
"""

from __future__ import annotations

from typing import Any, Mapping, Optional, Sequence

from ...core.vector_metrics import (
    VectorMetric,
    VectorMetricInput,
    normalize_vector_metric,
)
from ...core.vector_types import VectorRecord, VectorSearchResult


class ChromaVectorStore:
    """Vector store adapter for ChromaDB."""

    def __init__(
        self,
        *,
        path: str = "./.chroma",
        host: str | None = None,
        port: int | None = None,
    ) -> None:
        try:
            import chromadb  # type: ignore[import-not-found]
        except ImportError as exc:  # pragma: no cover - env dependent
            raise ImportError(
                "chromadb is required for ChromaVectorStore. "
                "Install with `pip install chromadb`."
            ) from exc

        self._chroma = chromadb
        if host:
            self._client = chromadb.HttpClient(host=host, port=port or 8000)
        elif path == ":memory:":
            self._client = chromadb.EphemeralClient()
        else:
            self._client = chromadb.PersistentClient(path=path)

        self._collections: dict[str, Any] = {}
        self._dimensions: dict[str, int] = {}
        self._metrics: dict[str, VectorMetric] = {}

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
            self._client.delete_collection(name=name)

        chroma_space = {
            VectorMetric.COSINE: "cosine",
            VectorMetric.DOT: "ip",
            VectorMetric.L2: "l2",
        }[normalized_metric]

        collection = self._client.create_collection(
            name=name,
            metadata={
                "hnsw:space": chroma_space,
                "dimension": dimension,
            },
        )
        self._collections[name] = collection
        self._dimensions[name] = dimension
        self._metrics[name] = normalized_metric

    def upsert(self, collection: str, records: Sequence[VectorRecord]) -> None:
        col = self._get_collection(collection)
        if not records:
            return

        dimension = self._dimensions.get(collection)
        if dimension is None:
            dimension = len(records[0].vector)
            self._dimensions[collection] = dimension

        for record in records:
            if len(record.vector) != dimension:
                raise ValueError(
                    f"Vector dimension mismatch: expected {dimension}, "
                    f"got {len(record.vector)}"
                )
        with_metadata: list[VectorRecord] = []
        without_metadata: list[VectorRecord] = []

        for record in records:
            payload = dict(record.payload) if record.payload is not None else None
            if payload:
                with_metadata.append(
                    VectorRecord(record.id, record.vector, payload)
                )
            else:
                without_metadata.append(record)

        if with_metadata:
            col.upsert(
                ids=[str(record.id) for record in with_metadata],
                embeddings=[
                    [float(value) for value in record.vector]
                    for record in with_metadata
                ],
                metadatas=[dict(record.payload or {}) for record in with_metadata],
            )

        if without_metadata:
            col.upsert(
                ids=[str(record.id) for record in without_metadata],
                embeddings=[
                    [float(value) for value in record.vector]
                    for record in without_metadata
                ],
            )

    def query(
        self,
        collection: str,
        vector: Sequence[float],
        *,
        top_k: int = 10,
        filters: Optional[Mapping[str, Any]] = None,
    ) -> list[VectorSearchResult]:
        col = self._get_collection(collection)
        if top_k <= 0:
            return []

        result = col.query(
            query_embeddings=[[float(value) for value in vector]],
            n_results=top_k,
            where=dict(filters) if filters else None,
            include=["metadatas", "distances"],
        )

        ids = self._first_batch(result.get("ids"))
        distances = self._first_batch(result.get("distances"))
        metadatas = self._first_batch(result.get("metadatas"))

        metric = self._metrics.get(collection, VectorMetric.COSINE)
        parsed: list[VectorSearchResult] = []
        for idx, item_id in enumerate(ids):
            distance = float(distances[idx]) if idx < len(distances) else 0.0
            payload = metadatas[idx] if idx < len(metadatas) else None
            parsed.append(
                VectorSearchResult(
                    id=str(item_id),
                    score=self._distance_to_score(metric, distance),
                    payload=payload,
                )
            )
        return parsed

    def fetch(
        self, collection: str, ids: Optional[Sequence[str]] = None
    ) -> list[VectorRecord]:
        col = self._get_collection(collection)
        rows = col.get(
            ids=list(ids) if ids is not None else None,
            include=["embeddings", "metadatas"],
        )

        row_ids = self._as_list(rows.get("ids"))
        vectors = self._as_list(rows.get("embeddings"))
        metadatas = self._as_list(rows.get("metadatas"))

        by_id: dict[str, VectorRecord] = {}
        for idx, item_id in enumerate(row_ids):
            vector = vectors[idx] if idx < len(vectors) else []
            payload = metadatas[idx] if idx < len(metadatas) else None
            by_id[str(item_id)] = VectorRecord(
                id=str(item_id),
                vector=list(vector),
                payload=payload,
            )

        if ids is None:
            return list(by_id.values())
        return [by_id[item_id] for item_id in ids if item_id in by_id]

    def delete(self, collection: str, ids: Sequence[str]) -> int:

        if not ids:
            return 0
        unique_ids = list(dict.fromkeys(ids))
        existing_ids = [record.id for record in self.fetch(collection, unique_ids)]
        if not existing_ids:
            return 0

        col = self._get_collection(collection)
        col.delete(ids=existing_ids)
        return len(existing_ids)

    def _get_collection(self, name: str) -> Any:
        if name in self._collections:
            return self._collections[name]

        if not self._collection_exists(name):
            raise KeyError(f"Collection does not exist: {name}")

        collection = self._client.get_collection(name=name)
        self._collections[name] = collection

        metadata = getattr(collection, "metadata", None) or {}
        dimension = metadata.get("dimension")
        if isinstance(dimension, int) and dimension > 0:
            self._dimensions[name] = dimension

        hnsw_space = metadata.get("hnsw:space", "cosine")
        self._metrics[name] = {
            "cosine": VectorMetric.COSINE,
            "ip": VectorMetric.DOT,
            "l2": VectorMetric.L2,
        }.get(str(hnsw_space), VectorMetric.COSINE)

        return collection

    def _collection_exists(self, name: str) -> bool:
        collections = self._client.list_collections()
        for item in collections:
            if isinstance(item, str) and item == name:
                return True
            collection_name = getattr(item, "name", None)
            if collection_name == name:
                return True
        return False

    @staticmethod
    def _distance_to_score(metric: VectorMetric, distance: float) -> float:
        if metric == VectorMetric.COSINE:
            return 1.0 - distance
        return -distance

    @staticmethod
    def _as_list(values: Any) -> list[Any]:
        if values is None:
            return []
        if hasattr(values, "tolist"):
            values = values.tolist()
        return list(values)

    @classmethod
    def _first_batch(cls, values: Any) -> list[Any]:
        outer = cls._as_list(values)
        if not outer:
            return []

        first = outer[0]
        if hasattr(first, "tolist"):
            first = first.tolist()
        if isinstance(first, (list, tuple)):
            return list(first)
        return outer
