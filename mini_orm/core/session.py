"""Session facade for grouped sync SQL repository operations."""

from __future__ import annotations

import contextlib
from contextlib import AbstractContextManager
from typing import Any, Iterator, Mapping, Optional, Sequence, Type, TypeVar

from .conditions import OrderBy
from .contracts import DatabasePort
from .models import DataclassModel
from .query_builder import WhereInput
from .repositories.repository import RelatedResult, Repository, UnifiedRepository

T = TypeVar("T", bound=DataclassModel)


class Session:
    """Sync session that combines transaction scope with unified repositories."""

    def __init__(
        self,
        db: DatabasePort,
        *,
        auto_schema: bool = False,
        schema_conflict: str = "raise",
        require_registration: bool = False,
    ):
        self.db = db
        self._hub = UnifiedRepository(
            db,
            auto_schema=auto_schema,
            schema_conflict=schema_conflict,
            require_registration=require_registration,
        )
        self._active_tx: AbstractContextManager[None] | None = None

    @property
    def hub(self) -> UnifiedRepository:
        """Expose the underlying unified repository instance."""

        return self._hub

    @contextlib.contextmanager
    def begin(self) -> Iterator[Session]:
        """Run operations in one commit/rollback transaction block."""

        with self.db.transaction():
            yield self

    def transaction(self) -> contextlib.AbstractContextManager[Session]:
        """Alias for `begin()`."""

        return self.begin()

    def repo(self, model: Type[T]) -> Repository[T]:
        return self._hub.repo(model)

    def register(
        self,
        model: Type[T],
        *,
        ensure: bool | None = None,
    ) -> Repository[T]:
        return self._hub.register(model, ensure=ensure)

    def register_many(
        self,
        models: Sequence[Type[DataclassModel]],
        *,
        ensure: bool | None = None,
    ) -> None:
        self._hub.register_many(models, ensure=ensure)

    def insert(self, model_or_object: Type[T] | T, obj: T | None = None) -> T:
        return self._hub.insert(model_or_object, obj)

    def update(self, model_or_object: Type[T] | T, obj: T | None = None) -> int:
        return self._hub.update(model_or_object, obj)

    def delete(self, model_or_object: Type[T] | T, obj: T | None = None) -> int:
        return self._hub.delete(model_or_object, obj)

    def get(self, model: Type[T], pk_value: Any) -> Optional[T]:
        return self._hub.get(model, pk_value)

    def list(
        self,
        model: Type[T],
        where: WhereInput = None,
        order_by: Optional[Sequence[OrderBy]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> list[T]:
        return self._hub.list(
            model,
            where=where,
            order_by=order_by,
            limit=limit,
            offset=offset,
        )

    def count(self, model: Type[T], where: WhereInput = None) -> int:
        return self._hub.count(model, where=where)

    def exists(self, model: Type[T], where: WhereInput = None) -> bool:
        return self._hub.exists(model, where=where)

    def insert_many(
        self,
        model_or_list: Type[T] | Sequence[T],
        objects: Sequence[T] | None = None,
    ) -> list[T]:
        return self._hub.insert_many(model_or_list, objects)

    def create(
        self,
        model_or_object: Type[T] | T,
        obj: T | None = None,
        *,
        relations: Mapping[str, Any] | None = None,
    ) -> T:
        return self._hub.create(model_or_object, obj, relations=relations)

    def get_related(
        self,
        model: Type[T],
        pk_value: Any,
        *,
        include: Sequence[str],
    ) -> Optional[RelatedResult[T]]:
        return self._hub.get_related(model, pk_value, include=include)

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
        return self._hub.list_related(
            model,
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
        return self._hub.update_where(model, values, where=where)

    def delete_where(self, model: Type[T], *, where: WhereInput) -> int:
        return self._hub.delete_where(model, where=where)

    def get_or_create(
        self,
        model: Type[T],
        *,
        lookup: Mapping[str, Any],
        defaults: Mapping[str, Any] | None = None,
    ) -> tuple[T, bool]:
        return self._hub.get_or_create(model, lookup=lookup, defaults=defaults)

    def __enter__(self) -> Session:
        if self._active_tx is not None:
            raise RuntimeError("session transaction is already active")
        tx = self.db.transaction()
        entered = False
        try:
            tx.__enter__()
            entered = True
            self._active_tx = tx
        except BaseException as exc:
            if entered:
                tx.__exit__(type(exc), exc, exc.__traceback__)
            raise
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool | None:
        tx = self._active_tx
        self._active_tx = None
        if tx is None:
            return None
        return tx.__exit__(exc_type, exc, tb)
