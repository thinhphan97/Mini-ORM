"""Async repository facade that composes CRUD and relation coordinators."""

from __future__ import annotations

from typing import Any, Generic, Mapping, Optional, Sequence, Type, TypeVar

from .conditions import OrderBy
from .contracts import AsyncDatabasePort
from .metadata import build_model_metadata
from .models import DataclassModel, require_dataclass_model
from .query_builder import WhereInput
from .repository_crud_async import (
    count_rows,
    delete,
    delete_where,
    exists_rows,
    get,
    get_or_create,
    insert,
    insert_many,
    list_rows,
    update,
    update_where,
)
from .repository_relations_async import AsyncRelatedResult, AsyncRelationCoordinator

T = TypeVar("T", bound=DataclassModel)


class AsyncRepository(Generic[T]):
    """Async CRUD repository backed by an `AsyncDatabasePort` implementation."""

    def __init__(self, db: AsyncDatabasePort, model: Type[T]):
        require_dataclass_model(model)
        self.db = db
        self.model = model
        self.d = db.dialect
        self.meta = build_model_metadata(model)
        self._relations = AsyncRelationCoordinator(self)

    async def insert(self, obj: T) -> T:
        """Insert an object and populate auto primary key when available."""

        return await insert(self, obj)

    async def update(self, obj: T) -> int:
        """Update one row identified by model primary key."""

        return await update(self, obj)

    async def delete(self, obj: T) -> int:
        """Delete one row identified by model primary key."""

        return await delete(self, obj)

    async def get(self, pk_value: Any) -> Optional[T]:
        """Fetch one row by primary key and map it to the model type."""

        return await get(self, pk_value)

    async def list(
        self,
        where: WhereInput = None,
        order_by: Optional[Sequence[OrderBy]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> list[T]:
        """List rows with optional filtering, sorting, and pagination."""

        return await list_rows(
            self,
            where=where,
            order_by=order_by,
            limit=limit,
            offset=offset,
        )

    async def count(self, where: WhereInput = None) -> int:
        """Count rows matching optional conditions."""

        return await count_rows(self, where=where)

    async def exists(self, where: WhereInput = None) -> bool:
        """Return whether at least one row matches optional conditions."""

        return await exists_rows(self, where=where)

    async def insert_many(self, objects: Sequence[T]) -> list[T]:
        """Insert many objects and return inserted objects."""

        return await insert_many(self, objects)

    async def create(self, obj: T, *, relations: Mapping[str, Any] | None = None) -> T:
        """Create one object and optionally create/link related records."""

        return await self._relations.create(obj, relations=relations)

    async def get_related(
        self,
        pk_value: Any,
        *,
        include: Sequence[str],
    ) -> Optional[AsyncRelatedResult[T]]:
        """Get one record and requested relations in one result object."""

        return await self._relations.get_related(pk_value, include=include)

    async def list_related(
        self,
        *,
        include: Sequence[str],
        where: WhereInput = None,
        order_by: Optional[Sequence[OrderBy]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> list[AsyncRelatedResult[T]]:
        """List records with requested related records."""

        return await self._relations.list_related(
            include=include,
            where=where,
            order_by=order_by,
            limit=limit,
            offset=offset,
        )

    async def update_where(
        self,
        values: Mapping[str, Any],
        *,
        where: WhereInput,
    ) -> int:
        """Update rows by conditions and return affected row count."""

        return await update_where(self, values, where=where)

    async def delete_where(self, *, where: WhereInput) -> int:
        """Delete rows by conditions and return affected row count."""

        return await delete_where(self, where=where)

    async def get_or_create(
        self,
        *,
        lookup: Mapping[str, Any],
        defaults: Mapping[str, Any] | None = None,
    ) -> tuple[T, bool]:
        """Get first row by lookup fields or create a new object."""

        return await get_or_create(self, lookup=lookup, defaults=defaults)
