"""Session facade for grouped async SQL repository operations."""

from __future__ import annotations

import contextlib
from contextlib import AbstractAsyncContextManager
from typing import Any, Mapping, Optional, Sequence, Type, TypeVar

from .conditions import OrderBy
from .contracts import AsyncDatabasePort
from .models import DataclassModel
from .query_builder import WhereInput
from .repositories.repository_async import AsyncRepository, AsyncUnifiedRepository
from .repositories.repository_relations_async import AsyncRelatedResult

T = TypeVar("T", bound=DataclassModel)


class AsyncSession:
    """Async session that combines transaction scope with unified repositories."""

    def __init__(
        self,
        db: AsyncDatabasePort,
        *,
        auto_schema: bool = False,
        schema_conflict: str = "raise",
        require_registration: bool = False,
    ):
        self.db = db
        self._hub = AsyncUnifiedRepository(
            db,
            auto_schema=auto_schema,
            schema_conflict=schema_conflict,
            require_registration=require_registration,
        )
        self._active_tx: AbstractAsyncContextManager[None] | None = None

    @property
    def hub(self) -> AsyncUnifiedRepository:
        """Expose the underlying async unified repository instance."""

        return self._hub

    @contextlib.asynccontextmanager
    async def begin(self):
        """Run operations in one commit/rollback transaction block."""

        async with self:
            yield self

    def transaction(self) -> contextlib.AbstractAsyncContextManager[AsyncSession]:
        """Alias for `begin()`."""

        return self.begin()

    def repo(self, model: Type[T]) -> AsyncRepository[T]:
        return self._hub.repo(model)

    async def register(
        self,
        model: Type[T],
        *,
        ensure: bool | None = None,
    ) -> AsyncRepository[T]:
        return await self._hub.register(model, ensure=ensure)

    async def register_many(
        self,
        models: Sequence[Type[DataclassModel]],
        *,
        ensure: bool | None = None,
    ) -> None:
        await self._hub.register_many(models, ensure=ensure)

    async def insert(self, model_or_object: Type[T] | T, obj: T | None = None) -> T:
        return await self._hub.insert(model_or_object, obj)

    async def update(self, model_or_object: Type[T] | T, obj: T | None = None) -> int:
        return await self._hub.update(model_or_object, obj)

    async def delete(self, model_or_object: Type[T] | T, obj: T | None = None) -> int:
        return await self._hub.delete(model_or_object, obj)

    async def get(self, model: Type[T], pk_value: Any) -> Optional[T]:
        return await self._hub.get(model, pk_value)

    async def list(
        self,
        model: Type[T],
        where: WhereInput = None,
        order_by: Optional[Sequence[OrderBy]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> list[T]:
        return await self._hub.list(
            model,
            where=where,
            order_by=order_by,
            limit=limit,
            offset=offset,
        )

    async def count(self, model: Type[T], where: WhereInput = None) -> int:
        return await self._hub.count(model, where=where)

    async def exists(self, model: Type[T], where: WhereInput = None) -> bool:
        return await self._hub.exists(model, where=where)

    async def insert_many(
        self,
        model_or_list: Type[T] | Sequence[T],
        objects: Sequence[T] | None = None,
    ) -> list[T]:
        return await self._hub.insert_many(model_or_list, objects)

    async def create(
        self,
        model_or_object: Type[T] | T,
        obj: T | None = None,
        *,
        relations: Mapping[str, Any] | None = None,
    ) -> T:
        return await self._hub.create(model_or_object, obj, relations=relations)

    async def get_related(
        self,
        model: Type[T],
        pk_value: Any,
        *,
        include: Sequence[str],
    ) -> Optional[AsyncRelatedResult[T]]:
        return await self._hub.get_related(model, pk_value, include=include)

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
        return await self._hub.list_related(
            model,
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
        return await self._hub.update_where(model, values, where=where)

    async def delete_where(self, model: Type[T], *, where: WhereInput) -> int:
        return await self._hub.delete_where(model, where=where)

    async def get_or_create(
        self,
        model: Type[T],
        *,
        lookup: Mapping[str, Any],
        defaults: Mapping[str, Any] | None = None,
    ) -> tuple[T, bool]:
        return await self._hub.get_or_create(model, lookup=lookup, defaults=defaults)

    async def __aenter__(self) -> AsyncSession:
        if self._active_tx is not None:
            raise RuntimeError("session transaction is already active")
        tx = self.db.transaction()
        await tx.__aenter__()
        self._active_tx = tx
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool | None:
        tx = self._active_tx
        self._active_tx = None
        if tx is None:
            return None
        return await tx.__aexit__(exc_type, exc, tb)
