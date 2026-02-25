"""Repository facade that composes CRUD and relation coordinators."""

from __future__ import annotations

from typing import Any, Generic, Mapping, Optional, Sequence, Type, TypeVar, cast

from ._unified_resolver import resolve_model_and_obj, resolve_model_and_objects
from ..conditions import OrderBy
from ..contracts import DatabasePort
from ..metadata import build_model_metadata
from ..models import DataclassModel, require_dataclass_model
from ..query_builder import WhereInput
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
from ..schemas.schema import (
    ensure_schema as ensure_schema_for_model,
    validate_schema_conflict,
)

T = TypeVar("T", bound=DataclassModel)


class Repository(Generic[T]):
    """CRUD repository backed by a `DatabasePort` implementation."""

    def __init__(
        self,
        db: DatabasePort,
        model: Type[T],
        *,
        auto_schema: bool = False,
        schema_conflict: str = "raise",
        require_registration: bool = False,
        registry: set[type[DataclassModel]] | None = None,
    ):
        require_dataclass_model(model)
        self.db = db
        self.model = model
        self.d = db.dialect
        self.meta = build_model_metadata(model)
        self._relations = RelationCoordinator(self)
        self._auto_schema = auto_schema
        self._schema_conflict = validate_schema_conflict(schema_conflict)
        self._require_registration = require_registration
        self._registry: set[type[DataclassModel]] = registry if registry is not None else set()
        self._schema_ready = False

    def _ensure_schema(self) -> None:
        if self._schema_ready:
            return
        ensure_schema_for_model(
            self.db,
            self.model,
            schema_conflict=self._schema_conflict,
        )
        self._schema_ready = True

    def register(self, *, ensure: bool | None = None) -> None:
        """Register current model and optionally ensure schema."""

        should_ensure = self._auto_schema if ensure is None else ensure
        if self.is_registered():
            if should_ensure:
                self._ensure_schema()
            return
        if should_ensure:
            self._ensure_schema()
        self._registry.add(self.model)

    def register_many(self, *, ensure: bool | None = None) -> None:
        """Single-model alias to mirror unified registration API shape."""

        self.register(ensure=ensure)

    def is_registered(self) -> bool:
        return self.model in self._registry

    def _prepare_for_action(self) -> None:
        if self.is_registered():
            return
        if self._require_registration:
            raise ValueError(
                f"Model {self.model.__name__} is not registered. "
                "Call register() before performing actions."
            )
        self.register()

    def insert(self, obj: T) -> T:
        """Insert an object and populate auto primary key when available."""

        self._prepare_for_action()
        return insert(self, obj)

    def update(self, obj: T) -> int:
        """Update one row identified by model primary key."""

        self._prepare_for_action()
        return update(self, obj)

    def delete(self, obj: T) -> int:
        """Delete one row identified by model primary key."""

        self._prepare_for_action()
        return delete(self, obj)

    def get(self, pk_value: Any) -> Optional[T]:
        """Fetch one row by primary key and map it to the model type."""

        self._prepare_for_action()
        return get(self, pk_value)

    def list(
        self,
        where: WhereInput = None,
        order_by: Optional[Sequence[OrderBy]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> list[T]:
        """List rows with optional filtering, sorting, and pagination."""

        self._prepare_for_action()
        return list_rows(
            self,
            where=where,
            order_by=order_by,
            limit=limit,
            offset=offset,
        )

    def count(self, where: WhereInput = None) -> int:
        """Count rows matching optional conditions."""

        self._prepare_for_action()
        return count_rows(self, where=where)

    def exists(self, where: WhereInput = None) -> bool:
        """Return whether at least one row matches optional conditions."""

        self._prepare_for_action()
        return exists_rows(self, where=where)

    def insert_many(self, objects: Sequence[T]) -> list[T]:
        """Insert many objects and return inserted objects."""

        self._prepare_for_action()
        return insert_many(self, objects)

    def create(self, obj: T, *, relations: Mapping[str, Any] | None = None) -> T:
        """Create one object and optionally create/link related records."""

        self._prepare_for_action()
        return self._relations.create(obj, relations=relations)

    def get_related(
        self,
        pk_value: Any,
        *,
        include: Sequence[str],
    ) -> Optional[RelatedResult[T]]:
        """Get one record and requested relations in one result object."""

        self._prepare_for_action()
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

        self._prepare_for_action()
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

        self._prepare_for_action()
        return update_where(self, values, where=where)

    def delete_where(self, *, where: WhereInput) -> int:
        """Delete rows by conditions and return affected row count."""

        self._prepare_for_action()
        return delete_where(self, where=where)

    def get_or_create(
        self,
        *,
        lookup: Mapping[str, Any],
        defaults: Mapping[str, Any] | None = None,
    ) -> tuple[T, bool]:
        """Get first row by lookup fields or create a new object."""

        self._prepare_for_action()
        return get_or_create(self, lookup=lookup, defaults=defaults)


class UnifiedRepository:
    """Route operations to cached `Repository[T]` instances by model class."""

    def __init__(
        self,
        db: DatabasePort,
        *,
        auto_schema: bool = False,
        schema_conflict: str = "raise",
        require_registration: bool = False,
    ):
        self.db = db
        self._repos: dict[type[DataclassModel], Repository[Any]] = {}
        self._auto_schema = auto_schema
        self._schema_conflict = validate_schema_conflict(schema_conflict)
        self._require_registration = require_registration
        self._registry: set[type[DataclassModel]] = set()

    def repo(self, model: Type[T]) -> Repository[T]:
        """Return cached repository for a dataclass model class."""

        cached = self._repos.get(model)
        if cached is None:
            cached = Repository(
                self.db,
                model,
                auto_schema=self._auto_schema,
                schema_conflict=self._schema_conflict,
                require_registration=self._require_registration,
                registry=self._registry,
            )
            self._repos[model] = cached
        return cast(Repository[T], cached)

    def register(
        self,
        model: Type[T],
        *,
        ensure: bool | None = None,
    ) -> Repository[T]:
        repo = self.repo(model)
        repo.register(ensure=ensure)
        return repo

    def register_many(
        self,
        models: Sequence[Type[DataclassModel]],
        *,
        ensure: bool | None = None,
    ) -> None:
        for model in models:
            self.register(model, ensure=ensure)

    def _resolve_model_and_obj(
        self,
        model_or_object: Type[T] | T,
        obj: T | None = None,
    ) -> tuple[Type[T], T]:
        return resolve_model_and_obj(model_or_object, obj)

    def _resolve_model_and_objects(
        self,
        model_or_list: Type[T] | Sequence[T],
        objects: Sequence[T] | None = None,
    ) -> tuple[Type[T], Sequence[T]]:
        return resolve_model_and_objects(model_or_list, objects)

    def insert(self, model_or_object: Type[T] | T, obj: T | None = None) -> T:
        model, resolved_obj = self._resolve_model_and_obj(model_or_object, obj)
        return self.repo(model).insert(resolved_obj)

    def update(self, model_or_object: Type[T] | T, obj: T | None = None) -> int:
        model, resolved_obj = self._resolve_model_and_obj(model_or_object, obj)
        return self.repo(model).update(resolved_obj)

    def delete(self, model_or_object: Type[T] | T, obj: T | None = None) -> int:
        model, resolved_obj = self._resolve_model_and_obj(model_or_object, obj)
        return self.repo(model).delete(resolved_obj)

    def get(self, model: Type[T], pk_value: Any) -> Optional[T]:
        return self.repo(model).get(pk_value)

    def list(
        self,
        model: Type[T],
        where: WhereInput = None,
        order_by: Optional[Sequence[OrderBy]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> list[T]:
        return self.repo(model).list(
            where=where,
            order_by=order_by,
            limit=limit,
            offset=offset,
        )

    def count(self, model: Type[T], where: WhereInput = None) -> int:
        return self.repo(model).count(where=where)

    def exists(self, model: Type[T], where: WhereInput = None) -> bool:
        return self.repo(model).exists(where=where)

    def insert_many(
        self,
        model_or_list: Type[T] | Sequence[T],
        objects: Sequence[T] | None = None,
    ) -> list[T]:
        model, resolved_objects = self._resolve_model_and_objects(model_or_list, objects)
        return self.repo(model).insert_many(resolved_objects)

    def create(
        self,
        model_or_object: Type[T] | T,
        obj: T | None = None,
        *,
        relations: Mapping[str, Any] | None = None,
    ) -> T:
        model, resolved_obj = self._resolve_model_and_obj(model_or_object, obj)
        return self.repo(model).create(resolved_obj, relations=relations)

    def get_related(
        self,
        model: Type[T],
        pk_value: Any,
        *,
        include: Sequence[str],
    ) -> Optional[RelatedResult[T]]:
        return self.repo(model).get_related(pk_value, include=include)

    def list_related(
        self,
        model: Type[T],
        *,
        include: Sequence[str],
        where: WhereInput = None,
        order_by: Optional[Sequence[OrderBy]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> list[RelatedResult[T]]:
        return self.repo(model).list_related(
            include=include,
            where=where,
            order_by=order_by,
            limit=limit,
            offset=offset,
        )

    def update_where(
        self,
        model: Type[T],
        values: Mapping[str, Any],
        *,
        where: WhereInput,
    ) -> int:
        return self.repo(model).update_where(values, where=where)

    def delete_where(self, model: Type[T], *, where: WhereInput) -> int:
        return self.repo(model).delete_where(where=where)

    def get_or_create(
        self,
        model: Type[T],
        *,
        lookup: Mapping[str, Any],
        defaults: Mapping[str, Any] | None = None,
    ) -> tuple[T, bool]:
        return self.repo(model).get_or_create(lookup=lookup, defaults=defaults)
