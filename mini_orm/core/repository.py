"""Repository facade that composes CRUD and relation coordinators."""

from __future__ import annotations

from typing import Any, Generic, Mapping, Optional, Sequence, Type, TypeVar

from .conditions import OrderBy
from .contracts import DatabasePort
from .metadata import build_model_metadata
from .models import DataclassModel, require_dataclass_model
from .query_builder import WhereInput
from .repository_crud import (
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
from .repository_relations import RelatedResult, RelationCoordinator

T = TypeVar("T", bound=DataclassModel)


class Repository(Generic[T]):
    """CRUD repository backed by a `DatabasePort` implementation."""

    def __init__(self, db: DatabasePort, model: Type[T]):
        require_dataclass_model(model)
        self.db = db
        self.model = model
        self.d = db.dialect
        self.meta = build_model_metadata(model)
        self._relations = RelationCoordinator(self)

    def insert(self, obj: T) -> T:
        """Insert an object and populate auto primary key when available."""

        return insert(self, obj)

    def update(self, obj: T) -> int:
        """Update one row identified by model primary key."""

        return update(self, obj)

    def delete(self, obj: T) -> int:
        """Delete one row identified by model primary key."""

        return delete(self, obj)

    def get(self, pk_value: Any) -> Optional[T]:
        """Fetch one row by primary key and map it to the model type."""

        return get(self, pk_value)

    def list(
        self,
        where: WhereInput = None,
        order_by: Optional[Sequence[OrderBy]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> list[T]:
        """List rows with optional filtering, sorting, and pagination."""

        return list_rows(
            self,
            where=where,
            order_by=order_by,
            limit=limit,
            offset=offset,
        )

    def count(self, where: WhereInput = None) -> int:
        """Count rows matching optional conditions."""

        return count_rows(self, where=where)

    def exists(self, where: WhereInput = None) -> bool:
        """Return whether at least one row matches optional conditions."""

        return exists_rows(self, where=where)

    def insert_many(self, objects: Sequence[T]) -> list[T]:
        """Insert many objects and return inserted objects."""

        return insert_many(self, objects)

    def create(self, obj: T, *, relations: Mapping[str, Any] | None = None) -> T:
        """Create one object and optionally create/link related records."""

        return self._relations.create(obj, relations=relations)

    def get_related(
        self,
        pk_value: Any,
        *,
        include: Sequence[str],
    ) -> Optional[RelatedResult[T]]:
        """Get one record and requested relations in one result object."""

        return self._relations.get_related(pk_value, include=include)

    def list_related(
        self,
        *,
        include: Sequence[str],
        where: WhereInput = None,
        order_by: Optional[Sequence[OrderBy]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> list[RelatedResult[T]]:
        """List records with requested related records."""

        return self._relations.list_related(
            include=include,
            where=where,
            order_by=order_by,
            limit=limit,
            offset=offset,
        )

    def update_where(
        self,
        values: Mapping[str, Any],
        *,
        where: WhereInput,
    ) -> int:
        """Update rows by conditions and return affected row count."""

        return update_where(self, values, where=where)

    def delete_where(self, *, where: WhereInput) -> int:
        """Delete rows by conditions and return affected row count."""

        return delete_where(self, where=where)

    def get_or_create(
        self,
        *,
        lookup: Mapping[str, Any],
        defaults: Mapping[str, Any] | None = None,
    ) -> tuple[T, bool]:
        """Get first row by lookup fields or create a new object."""

        return get_or_create(self, lookup=lookup, defaults=defaults)
