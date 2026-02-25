"""Async repository facade that composes CRUD and relation coordinators."""

from __future__ import annotations

from typing import Any, Generic, Mapping, Optional, Sequence, Type, TypeVar, cast

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
from .schema import ensure_schema_async

T = TypeVar("T", bound=DataclassModel)


class AsyncRepository(Generic[T]):
    """Async CRUD repository backed by an `AsyncDatabasePort` implementation."""

    def __init__(
        self,
        db: AsyncDatabasePort,
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
        self._relations = AsyncRelationCoordinator(self)
        self._auto_schema = auto_schema
        self._schema_conflict = schema_conflict
        self._require_registration = require_registration
        self._registry: set[type[DataclassModel]] = registry if registry is not None else set()
        self._schema_ready = not auto_schema

    async def register(self, *, ensure: bool | None = None) -> None:
        """Register current model and optionally ensure schema."""

        if self.is_registered():
            return
        should_ensure = self._auto_schema if ensure is None else ensure
        if should_ensure:
            await self._ensure_schema()
        self._registry.add(self.model)

    def is_registered(self) -> bool:
        return self.model in self._registry

    async def _ensure_schema(self) -> None:
        if self._schema_ready:
            return
        await ensure_schema_async(
            self.db,
            self.model,
            schema_conflict=self._schema_conflict,
        )
        self._schema_ready = True

    async def _prepare_for_action(self) -> None:
        if self.is_registered():
            return
        if self._require_registration:
            raise ValueError(
                f"Model {self.model.__name__} is not registered. "
                "Call register() before performing actions."
            )
        await self.register()

    async def insert(self, obj: T) -> T:
        """Insert an object and populate auto primary key when available."""

        await self._prepare_for_action()
        return await insert(self, obj)

    async def update(self, obj: T) -> int:
        """Update one row identified by model primary key."""

        await self._prepare_for_action()
        return await update(self, obj)

    async def delete(self, obj: T) -> int:
        """Delete one row identified by model primary key."""

        await self._prepare_for_action()
        return await delete(self, obj)

    async def get(self, pk_value: Any) -> Optional[T]:
        """Fetch one row by primary key and map it to the model type."""

        await self._prepare_for_action()
        return await get(self, pk_value)

    async def list(
        self,
        where: WhereInput = None,
        order_by: Optional[Sequence[OrderBy]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> list[T]:
        """List rows with optional filtering, sorting, and pagination."""

        await self._prepare_for_action()
        return await list_rows(
            self,
            where=where,
            order_by=order_by,
            limit=limit,
            offset=offset,
        )

    async def count(self, where: WhereInput = None) -> int:
        """Count rows matching optional conditions."""

        await self._prepare_for_action()
        return await count_rows(self, where=where)

    async def exists(self, where: WhereInput = None) -> bool:
        """Return whether at least one row matches optional conditions."""

        await self._prepare_for_action()
        return await exists_rows(self, where=where)

    async def insert_many(self, objects: Sequence[T]) -> list[T]:
        """Insert many objects and return inserted objects."""

        await self._prepare_for_action()
        return await insert_many(self, objects)

    async def create(self, obj: T, *, relations: Mapping[str, Any] | None = None) -> T:
        """Create one object and optionally create/link related records."""

        await self._prepare_for_action()
        return await self._relations.create(obj, relations=relations)

    async def get_related(
        self,
        pk_value: Any,
        *,
        include: Sequence[str],
    ) -> Optional[AsyncRelatedResult[T]]:
        """Get one record and requested relations in one result object."""

        await self._prepare_for_action()
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

        await self._prepare_for_action()
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

        await self._prepare_for_action()
        return await update_where(self, values, where=where)

    async def delete_where(self, *, where: WhereInput) -> int:
        """Delete rows by conditions and return affected row count."""

        await self._prepare_for_action()
        return await delete_where(self, where=where)

    async def get_or_create(
        self,
        *,
        lookup: Mapping[str, Any],
        defaults: Mapping[str, Any] | None = None,
    ) -> tuple[T, bool]:
        """Get first row by lookup fields or create a new object."""

        await self._prepare_for_action()
        return await get_or_create(self, lookup=lookup, defaults=defaults)


class AsyncUnifiedRepository:
    """Route operations to cached `AsyncRepository[T]` instances by model class."""

    def __init__(
        self,
        db: AsyncDatabasePort,
        *,
        auto_schema: bool = False,
        schema_conflict: str = "raise",
        require_registration: bool = False,
    ):
        self.db = db
        self._repos: dict[type[DataclassModel], AsyncRepository[Any]] = {}
        self._auto_schema = auto_schema
        self._schema_conflict = schema_conflict
        self._require_registration = require_registration
        self._registry: set[type[DataclassModel]] = set()

    def repo(self, model: Type[T]) -> AsyncRepository[T]:
        """Return cached async repository for a dataclass model class."""

        cached = self._repos.get(model)
        if cached is None:
            cached = AsyncRepository(
                self.db,
                model,
                auto_schema=self._auto_schema,
                schema_conflict=self._schema_conflict,
                require_registration=self._require_registration,
                registry=self._registry,
            )
            self._repos[model] = cached
        return cast(AsyncRepository[T], cached)

    async def register(
        self,
        model: Type[T],
        *,
        ensure: bool | None = None,
    ) -> AsyncRepository[T]:
        repo = self.repo(model)
        await repo.register(ensure=ensure)
        return repo

    async def register_many(
        self,
        models: Sequence[Type[DataclassModel]],
        *,
        ensure: bool | None = None,
    ) -> None:
        for model in models:
            await self.register(model, ensure=ensure)

    def _resolve_model_and_obj(
        self,
        model_or_object: Type[T] | T,
        obj: T | None = None,
    ) -> tuple[Type[T], T]:
        if obj is None:
            if isinstance(model_or_object, type):
                raise TypeError("Object instance is required when passing a model class.")
            inferred_model = type(model_or_object)
            require_dataclass_model(inferred_model)
            return cast(Type[T], inferred_model), cast(T, model_or_object)

        if not isinstance(model_or_object, type):
            raise TypeError(
                "First argument must be a model class when second argument is provided."
            )
        require_dataclass_model(model_or_object)
        if not isinstance(obj, model_or_object):
            raise TypeError(
                f"Object type {type(obj).__name__} does not match model "
                f"{model_or_object.__name__}."
            )
        return model_or_object, obj

    def _resolve_model_and_objects(
        self,
        model_or_list: Type[T] | Sequence[T],
        objects: Sequence[T] | None = None,
    ) -> tuple[Type[T], Sequence[T]]:
        if objects is None:
            inferred_objects = cast(Sequence[T], model_or_list)
            if not inferred_objects:
                raise ValueError(
                    "Cannot infer model from an empty objects sequence. "
                    "Pass model explicitly: insert_many(Model, objects)."
                )
            inferred_model = type(inferred_objects[0])
            require_dataclass_model(inferred_model)
            for item in inferred_objects:
                if not isinstance(item, inferred_model):
                    raise TypeError("All objects must share the same model class.")
            return cast(Type[T], inferred_model), inferred_objects

        if not isinstance(model_or_list, type):
            raise TypeError(
                "First argument must be a model class when second argument is provided."
            )
        require_dataclass_model(model_or_list)
        for item in objects:
            if not isinstance(item, model_or_list):
                raise TypeError("All objects must match the provided model class.")
        return model_or_list, objects

    async def insert(self, model_or_object: Type[T] | T, obj: T | None = None) -> T:
        model, resolved_obj = self._resolve_model_and_obj(model_or_object, obj)
        return await self.repo(model).insert(resolved_obj)

    async def update(self, model_or_object: Type[T] | T, obj: T | None = None) -> int:
        model, resolved_obj = self._resolve_model_and_obj(model_or_object, obj)
        return await self.repo(model).update(resolved_obj)

    async def delete(self, model_or_object: Type[T] | T, obj: T | None = None) -> int:
        model, resolved_obj = self._resolve_model_and_obj(model_or_object, obj)
        return await self.repo(model).delete(resolved_obj)

    async def get(self, model: Type[T], pk_value: Any) -> Optional[T]:
        return await self.repo(model).get(pk_value)

    async def list(
        self,
        model: Type[T],
        where: WhereInput = None,
        order_by: Optional[Sequence[OrderBy]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> list[T]:
        return await self.repo(model).list(
            where=where,
            order_by=order_by,
            limit=limit,
            offset=offset,
        )

    async def count(self, model: Type[T], where: WhereInput = None) -> int:
        return await self.repo(model).count(where=where)

    async def exists(self, model: Type[T], where: WhereInput = None) -> bool:
        return await self.repo(model).exists(where=where)

    async def insert_many(
        self,
        model_or_list: Type[T] | Sequence[T],
        objects: Sequence[T] | None = None,
    ) -> list[T]:
        model, resolved_objects = self._resolve_model_and_objects(model_or_list, objects)
        return await self.repo(model).insert_many(resolved_objects)

    async def create(
        self,
        model_or_object: Type[T] | T,
        obj: T | None = None,
        *,
        relations: Mapping[str, Any] | None = None,
    ) -> T:
        model, resolved_obj = self._resolve_model_and_obj(model_or_object, obj)
        return await self.repo(model).create(resolved_obj, relations=relations)

    async def get_related(
        self,
        model: Type[T],
        pk_value: Any,
        *,
        include: Sequence[str],
    ) -> Optional[AsyncRelatedResult[T]]:
        return await self.repo(model).get_related(pk_value, include=include)

    async def list_related(
        self,
        model: Type[T],
        *,
        include: Sequence[str],
        where: WhereInput = None,
        order_by: Optional[Sequence[OrderBy]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> list[AsyncRelatedResult[T]]:
        return await self.repo(model).list_related(
            include=include,
            where=where,
            order_by=order_by,
            limit=limit,
            offset=offset,
        )

    async def update_where(
        self,
        model: Type[T],
        values: Mapping[str, Any],
        *,
        where: WhereInput,
    ) -> int:
        return await self.repo(model).update_where(values, where=where)

    async def delete_where(self, model: Type[T], *, where: WhereInput) -> int:
        return await self.repo(model).delete_where(where=where)

    async def get_or_create(
        self,
        model: Type[T],
        *,
        lookup: Mapping[str, Any],
        defaults: Mapping[str, Any] | None = None,
    ) -> tuple[T, bool]:
        return await self.repo(model).get_or_create(lookup=lookup, defaults=defaults)
