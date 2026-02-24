"""Async relation orchestration for repository create/get/list APIs."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence as SequenceABC
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, Mapping, Optional, Sequence, TypeVar

from .conditions import C, OrderBy
from .models import DataclassModel, RelationSpec
from .query_builder import WhereInput

if TYPE_CHECKING:
    from .repository_async import AsyncRepository

T = TypeVar("T", bound=DataclassModel)


@dataclass(frozen=True)
class AsyncRelatedResult(Generic[T]):
    """One record and its requested related records."""

    obj: T
    relations: dict[str, Any]


class AsyncRelationCoordinator(Generic[T]):
    """Encapsulates async relation create/link and eager-load workflows."""

    def __init__(self, repo: "AsyncRepository[T]") -> None:
        self.repo = repo

    async def create(self, obj: T, *, relations: Mapping[str, Any] | None = None) -> T:
        """Create one object and optionally create/link related records."""

        if not relations:
            return await self.repo.insert(obj)

        relation_items = list(relations.items())
        async with self.repo.db.transaction():
            await self._insert_belongs_to_relations(obj, relation_items)
            await self.repo.insert(obj)
            await self._insert_has_many_relations(obj, relation_items)

        return obj

    async def get_related(
        self,
        pk_value: Any,
        *,
        include: Sequence[str],
    ) -> Optional[AsyncRelatedResult[T]]:
        """Get one record and requested relations in one result object."""

        obj = await self.repo.get(pk_value)
        if obj is None:
            return None

        loaded = await self._load_relations_for_objects([obj], include=include)
        return AsyncRelatedResult(obj=obj, relations=loaded[0])

    async def list_related(
        self,
        *,
        include: Sequence[str],
        where: WhereInput = None,
        order_by: Sequence[OrderBy] | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[AsyncRelatedResult[T]]:
        """List records with requested related records."""

        rows = await self.repo.list(
            where=where,
            order_by=order_by,
            limit=limit,
            offset=offset,
        )
        loaded = await self._load_relations_for_objects(rows, include=include)
        return [
            AsyncRelatedResult(obj=row, relations=relations)
            for row, relations in zip(rows, loaded, strict=True)
        ]

    async def _insert_belongs_to_relations(
        self,
        obj: T,
        relation_items: Sequence[tuple[str, Any]],
    ) -> None:
        for relation_name, relation_value in relation_items:
            spec = self._relation_spec(relation_name)
            if spec.many or relation_value is None:
                continue
            if not isinstance(relation_value, spec.model):
                raise TypeError(
                    f"Relation {relation_name!r} expects {spec.model.__name__}."
                )

            related_repo = self._new_repo(spec.model)
            inserted_relation = await related_repo.insert(relation_value)
            setattr(
                obj,
                spec.local_key,
                getattr(inserted_relation, spec.remote_key),
            )

    async def _insert_has_many_relations(
        self,
        obj: T,
        relation_items: Sequence[tuple[str, Any]],
    ) -> None:
        for relation_name, relation_value in relation_items:
            spec = self._relation_spec(relation_name)
            if not spec.many or relation_value is None:
                continue

            self._ensure_relation_sequence(relation_name, relation_value, spec)
            related_repo = self._new_repo(spec.model)
            for child in relation_value:
                if not isinstance(child, spec.model):
                    raise TypeError(
                        f"Relation {relation_name!r} expects {spec.model.__name__}."
                    )
                setattr(child, spec.remote_key, getattr(obj, spec.local_key))
            await related_repo.insert_many(relation_value)

    def _ensure_relation_sequence(
        self,
        relation_name: str,
        value: Any,
        spec: RelationSpec,
    ) -> None:
        if isinstance(value, (str, bytes)) or not isinstance(value, SequenceABC):
            raise TypeError(
                f"Relation {relation_name!r} expects a sequence of "
                f"{spec.model.__name__} objects."
            )

    async def _load_relations_for_objects(
        self,
        objects: Sequence[T],
        *,
        include: Sequence[str],
    ) -> list[dict[str, Any]]:
        include_names = self._normalize_include(include)
        for relation_name in include_names:
            self._relation_spec(relation_name)

        if not objects:
            return []

        results: list[dict[str, Any]] = [dict() for _ in objects]

        tasks: list[Any] = []
        for relation_name in include_names:
            spec = self._relation_spec(relation_name)
            related_repo = self._new_repo(spec.model)

            if spec.many:
                tasks.append(
                    self._attach_has_many_relation(
                        results,
                        objects=objects,
                        relation_name=relation_name,
                        spec=spec,
                        related_repo=related_repo,
                    )
                )
                continue

            tasks.append(
                self._attach_belongs_to_relation(
                    results,
                    objects=objects,
                    relation_name=relation_name,
                    spec=spec,
                    related_repo=related_repo,
                )
            )

        if tasks:
            await asyncio.gather(*tasks)

        return results

    async def _attach_has_many_relation(
        self,
        results: list[dict[str, Any]],
        *,
        objects: Sequence[T],
        relation_name: str,
        spec: RelationSpec,
        related_repo: Any,
    ) -> None:
        owner_keys = self._dedupe_non_null([getattr(obj, spec.local_key) for obj in objects])
        grouped: dict[Any, list[DataclassModel]] = {}
        if owner_keys:
            related_rows = await related_repo.list(
                where=C.in_(spec.remote_key, owner_keys),
                order_by=[OrderBy(spec.remote_key), OrderBy(related_repo.meta.pk)],
            )
            for row in related_rows:
                grouped.setdefault(getattr(row, spec.remote_key), []).append(row)

        for index, obj in enumerate(objects):
            key = getattr(obj, spec.local_key)
            results[index][relation_name] = grouped.get(key, [])

    async def _attach_belongs_to_relation(
        self,
        results: list[dict[str, Any]],
        *,
        objects: Sequence[T],
        relation_name: str,
        spec: RelationSpec,
        related_repo: Any,
    ) -> None:
        fk_values = self._dedupe_non_null([getattr(obj, spec.local_key) for obj in objects])
        mapped: dict[Any, DataclassModel] = {}
        if fk_values:
            related_rows = await related_repo.list(where=C.in_(spec.remote_key, fk_values))
            for row in related_rows:
                mapped[getattr(row, spec.remote_key)] = row

        for index, obj in enumerate(objects):
            key = getattr(obj, spec.local_key)
            results[index][relation_name] = mapped.get(key)

    def _new_repo(self, model: type[DataclassModel]) -> Any:
        repo_type = type(self.repo)
        return repo_type(self.repo.db, model)

    def _relation_spec(self, relation_name: str) -> RelationSpec:
        spec = self.repo.meta.relations.get(relation_name)
        if spec is None:
            available = ", ".join(sorted(self.repo.meta.relations)) or "<none>"
            raise ValueError(
                f"Unknown relation {relation_name!r} on {self.repo.model.__name__}. "
                f"Available: {available}"
            )
        return spec

    def _normalize_include(self, include: Sequence[str]) -> list[str]:
        normalized: list[str] = []
        for name in include:
            if not isinstance(name, str) or not name:
                raise TypeError("include relation names must be non-empty strings.")
            if name in normalized:
                continue
            normalized.append(name)
        return normalized

    def _dedupe_non_null(self, values: Sequence[Any]) -> list[Any]:
        deduped: list[Any] = []
        seen: set[Any] = set()
        for value in values:
            if value is None:
                continue
            if value in seen:
                continue
            seen.add(value)
            deduped.append(value)
        return deduped
