"""Repository abstraction for vector database operations."""

from __future__ import annotations

from uuid import UUID
from typing import Any, Mapping, Optional, Sequence

from ..contracts import VectorStorePort
from .vector_codecs import IdentityVectorPayloadCodec, VectorPayloadCodec
from .vector_metrics import VectorMetric, VectorMetricInput, normalize_vector_metric
from .vector_policies import VectorIdPolicy
from .vector_types import VectorRecord, VectorSearchResult


class VectorRepository:
    """High-level vector collection operations backed by a vector store port."""

    def __init__(
        self,
        store: VectorStorePort,
        collection: str,
        *,
        dimension: int,
        metric: VectorMetricInput = VectorMetric.COSINE,
        auto_create: bool = True,
        overwrite: bool = False,
        payload_codec: VectorPayloadCodec | None = None,
    ) -> None:
        """Create a vector repository.

        Args:
            store: Concrete vector store adapter.
            collection: Logical collection name.
            dimension: Vector dimension for this collection.
            metric: Distance/similarity metric (`str` or `VectorMetric`).
            auto_create: Auto create collection on repository initialization.
            overwrite: Recreate collection if already exists (used with auto_create).
            payload_codec: Optional codec used to serialize/deserialize payload and
                query filter values.
        """

        if dimension <= 0:
            raise ValueError("dimension must be > 0")

        self.store = store
        self.collection = collection
        self.dimension = dimension
        self.metric = normalize_vector_metric(metric)
        self.id_policy = getattr(store, "id_policy", VectorIdPolicy.ANY)
        self.supports_filters = bool(getattr(store, "supports_filters", True))
        self.payload_codec = payload_codec or IdentityVectorPayloadCodec()

        if auto_create:
            self.store.create_collection(
                collection,
                dimension=dimension,
                metric=self.metric,
                overwrite=overwrite,
            )

    def create_collection(self, *, overwrite: bool = False) -> None:
        """Create or recreate collection explicitly."""

        self.store.create_collection(
            self.collection,
            dimension=self.dimension,
            metric=self.metric,
            overwrite=overwrite,
        )

    def upsert(self, records: Sequence[VectorRecord]) -> None:
        """Insert or update vector records."""

        normalized_records: list[VectorRecord] = []
        for record in records:
            self._validate_vector_dimension(record.vector)
            record_id = self._normalize_id(record.id)
            normalized_records.append(
                VectorRecord(
                    id=record_id,
                    vector=record.vector,
                    payload=self.payload_codec.serialize(record.payload),
                )
            )
        self.store.upsert(self.collection, normalized_records)

    def query(
        self,
        vector: Sequence[float],
        *,
        top_k: int = 10,
        filters: Optional[Mapping[str, Any]] = None,
    ) -> list[VectorSearchResult]:
        """Search nearest vectors in the collection."""
        if filters and not self.supports_filters:
            raise NotImplementedError(
                f"{type(self.store).__name__} does not support payload filters in query()."
            )
        self._validate_vector_dimension(vector)

        serialized_filters = self.payload_codec.serialize_filters(filters) if filters else None
        raw_results = self.store.query(
            self.collection,
            vector,
            top_k=top_k,
            filters=serialized_filters,
        )
        return [
            VectorSearchResult(
                id=item.id,
                score=item.score,
                payload=self.payload_codec.deserialize(item.payload),
            )
            for item in raw_results
        ]

    def fetch(self, ids: Optional[Sequence[str]] = None) -> list[VectorRecord]:
        """Fetch records by ids, or fetch all when ids is None."""

        normalized_ids = (
            [self._normalize_id(item_id) for item_id in ids]
            if ids is not None
            else None
        )
        rows = self.store.fetch(self.collection, normalized_ids)
        return [
            VectorRecord(
                id=row.id,
                vector=row.vector,
                payload=self.payload_codec.deserialize(row.payload),
            )
            for row in rows
        ]

    def delete(self, ids: Sequence[str]) -> int:
        """Delete records by ids and return number of deleted rows."""

        normalized_ids = [self._normalize_id(item_id) for item_id in ids]
        return self.store.delete(self.collection, normalized_ids)

    def _validate_vector_dimension(self, vector: Sequence[float]) -> None:
        if len(vector) != self.dimension:
            raise ValueError(
                f"Vector dimension mismatch: expected {self.dimension}, got {len(vector)}"
            )

    def _normalize_id(self, value: str) -> str:
        if self.id_policy != VectorIdPolicy.UUID:
            return value
        try:
            return str(UUID(str(value)))
        except Exception as exc:
            raise ValueError(
                f"{type(self.store).__name__} requires UUID string ids. "
                f"Invalid id: {value!r}"
            ) from exc
