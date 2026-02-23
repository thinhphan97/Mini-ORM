"""Core port contracts used by adapters and repository."""

from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Any, List, Mapping, Optional, Protocol, Sequence

from .types import MaybeRow, QueryParams, RowMapping
from .vector_metrics import VectorMetric, VectorMetricInput
from .vector_types import VectorRecord, VectorSearchResult


class DialectPort(Protocol):
    """Dialect behavior required by query compilation and CRUD operations."""

    paramstyle: str
    supports_returning: bool

    def q(self, ident: str) -> str: ...

    def placeholder(self, key: str) -> str: ...

    def returning_clause(self, pk_name: str) -> str: ...

    def get_lastrowid(self, cursor: Any) -> Optional[int]: ...


class DatabasePort(Protocol):
    """Database adapter behavior required by the core repository."""

    dialect: DialectPort

    def transaction(self) -> AbstractContextManager[None]: ...

    def execute(self, sql: str, params: QueryParams = None) -> Any: ...

    def fetchone(self, sql: str, params: QueryParams = None) -> MaybeRow: ...

    def fetchall(self, sql: str, params: QueryParams = None) -> List[RowMapping]: ...


class VectorStorePort(Protocol):
    """Vector database behavior required by `VectorRepository`."""

    def create_collection(
        self,
        name: str,
        dimension: int,
        metric: VectorMetricInput = VectorMetric.COSINE,
        *,
        overwrite: bool = False,
    ) -> None: ...

    def upsert(self, collection: str, records: Sequence[VectorRecord]) -> None: ...

    def query(
        self,
        collection: str,
        vector: Sequence[float],
        *,
        top_k: int = 10,
        filters: Optional[Mapping[str, Any]] = None,
    ) -> List[VectorSearchResult]: ...

    def fetch(
        self, collection: str, ids: Optional[Sequence[str]] = None
    ) -> List[VectorRecord]: ...

    def delete(self, collection: str, ids: Sequence[str]) -> int: ...
