"""Repository abstraction for vector database operations."""

from __future__ import annotations

from typing import Any, Mapping, Optional, Sequence

from .contracts import VectorStorePort
from .vector_metrics import VectorMetric, VectorMetricInput, normalize_vector_metric
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
    ) -> None:
        """Create a vector repository.

        Args:
            store: Concrete vector store adapter.
            collection: Logical collection name.
            dimension: Vector dimension for this collection.
            metric: Distance/similarity metric (`str` or `VectorMetric`).
            auto_create: Auto create collection on repository initialization.
            overwrite: Recreate collection if already exists (used with auto_create).
        """

        self.store = store
        self.collection = collection
        self.dimension = dimension
        self.metric = normalize_vector_metric(metric)

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

        self.store.upsert(self.collection, records)

    def query(
        self,
        vector: Sequence[float],
        *,
        top_k: int = 10,
        filters: Optional[Mapping[str, Any]] = None,
    ) -> list[VectorSearchResult]:
        """Search nearest vectors in the collection."""

        return self.store.query(
            self.collection,
            vector,
            top_k=top_k,
            filters=filters,
        )

    def fetch(self, ids: Optional[Sequence[str]] = None) -> list[VectorRecord]:
        """Fetch records by ids, or fetch all when ids is None."""

        return self.store.fetch(self.collection, ids)

    def delete(self, ids: Sequence[str]) -> int:
        """Delete records by ids and return number of deleted rows."""

        return self.store.delete(self.collection, ids)
