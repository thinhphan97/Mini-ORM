from __future__ import annotations

import inspect
import sqlite3
import unittest
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from mini_orm import (
    C,
    AsyncDatabase,
    AsyncRepository,
    OrderBy,
    SQLiteDialect,
    apply_schema_async,
)
from mini_orm.ports.db_api.dialects import Dialect


@dataclass
class UserRow:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = field(default="", metadata={"unique_index": True})
    age: Optional[int] = None


@dataclass
class AuthorRow:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    name: str = ""


@dataclass
class PostRow:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    author_id: Optional[int] = field(
        default=None,
        metadata={
            "fk": (AuthorRow, "id"),
            "relation": "author",
            "related_name": "posts",
        },
    )
    title: str = ""


@dataclass
class AutoAuthor:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    name: str = ""


@dataclass
class AutoPost:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    author_id: Optional[int] = field(
        default=None,
        metadata={
            "fk": (AutoAuthor, "id"),
            "relation": "author",
            "related_name": "posts",
        },
    )
    title: str = ""


@dataclass
class OnlyPkRow:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})


@dataclass
class MultiPkRow:
    id1: int = field(default=0, metadata={"pk": True})
    id2: int = field(default=0, metadata={"pk": True})


@dataclass
class NonUniqueLookupRow:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = ""


class TicketStatus(str, Enum):
    OPEN = "open"
    CLOSED = "closed"


@dataclass
class TicketRow:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    status: TicketStatus = TicketStatus.OPEN
    payload: dict[str, Any] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)


class PlainModel:
    pass


class _NoReturningDialect(Dialect):
    paramstyle = "named"
    supports_returning = False
    quote_char = '"'


class _NoReturningAsyncDb:
    def __init__(self) -> None:
        self.dialect = _NoReturningDialect()
        self.executed: list[tuple[str, object]] = []

    class _Cursor:
        rowcount = 1
        lastrowid = 77

    async def execute(self, _sql, _params=None):  # noqa: ANN001,ANN201
        self.executed.append((_sql, _params))
        return self._Cursor()

    async def fetchone(self, _sql, _params=None):  # noqa: ANN001,ANN201
        return None

    async def fetchall(self, _sql, _params=None):  # noqa: ANN001,ANN201
        return []

    def transaction(self):  # pragma: no cover
        raise NotImplementedError


class AsyncDatabaseAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.db = AsyncDatabase(self.conn, SQLiteDialect())

    async def asyncTearDown(self) -> None:
        self.conn.close()

    async def test_execute_fetchone_fetchall(self) -> None:
        await self.db.execute('CREATE TABLE "t" ("id" INTEGER, "name" TEXT);')
        await self.db.execute(
            'INSERT INTO "t" ("id", "name") VALUES (:id, :name);',
            {"id": 1, "name": "a"},
        )
        await self.db.execute(
            'INSERT INTO "t" ("id", "name") VALUES (:id, :name);',
            {"id": 2, "name": "b"},
        )

        row = await self.db.fetchone('SELECT * FROM "t" WHERE "id" = :id;', {"id": 1})
        rows = await self.db.fetchall('SELECT * FROM "t" ORDER BY "id" ASC;')

        self.assertEqual(row["name"], "a")
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[1]["name"], "b")

    async def test_transaction_rolls_back_on_error(self) -> None:
        await self.db.execute('CREATE TABLE "t" ("id" INTEGER);')

        with self.assertRaises(RuntimeError):
            async with self.db.transaction():
                await self.db.execute('INSERT INTO "t" ("id") VALUES (1);')
                raise RuntimeError("boom")

        count = await self.db.fetchone('SELECT COUNT(*) AS "count" FROM "t";')
        self.assertEqual(count["count"], 0)


class AsyncRepositorySQLiteTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.db = AsyncDatabase(self.conn, SQLiteDialect())
        await apply_schema_async(self.db, UserRow)
        self.repo = AsyncRepository[UserRow](self.db, UserRow)

    async def asyncTearDown(self) -> None:
        self.conn.close()

    async def _seed_users(self) -> list[UserRow]:
        users = [
            UserRow(email="alice@example.com", age=25),
            UserRow(email="bob@example.com", age=30),
            UserRow(email="charlie@sample.com", age=35),
            UserRow(email="david@example.com", age=None),
        ]
        async with self.db.transaction():
            for user in users:
                await self.repo.insert(user)
        return users

    async def _relation_repositories(
        self,
    ) -> tuple[sqlite3.Connection, AsyncRepository[AuthorRow], AsyncRepository[PostRow]]:
        conn = sqlite3.connect(":memory:")
        self.addCleanup(conn.close)
        db = AsyncDatabase(conn, SQLiteDialect())
        await apply_schema_async(db, AuthorRow)
        await apply_schema_async(db, PostRow)
        return conn, AsyncRepository[AuthorRow](db, AuthorRow), AsyncRepository[PostRow](db, PostRow)

    async def test_async_repository_surface_matches_sync_names(self) -> None:
        method_names = [
            "insert",
            "update",
            "delete",
            "get",
            "list",
            "count",
            "exists",
            "insert_many",
            "create",
            "get_related",
            "list_related",
            "update_where",
            "delete_where",
            "get_or_create",
        ]
        for name in method_names:
            self.assertTrue(hasattr(AsyncRepository, name))
            self.assertTrue(inspect.iscoroutinefunction(getattr(AsyncRepository, name)))

    async def test_crud_and_query_utilities(self) -> None:
        inserted = await self.repo.insert(UserRow(email="new@example.com", age=20))
        self.assertIsNotNone(inserted.id)

        found = await self.repo.get(inserted.id)
        self.assertEqual(found.email, "new@example.com")

        found.age = 21
        self.assertEqual(await self.repo.update(found), 1)
        self.assertEqual((await self.repo.get(found.id)).age, 21)

        self.assertEqual(await self.repo.count(), 1)
        self.assertTrue(await self.repo.exists(where=C.eq("email", "new@example.com")))

        self.assertEqual(await self.repo.delete(found), 1)
        self.assertIsNone(await self.repo.get(found.id))

    async def test_insert_assigns_auto_pk(self) -> None:
        user = UserRow(email="new@example.com", age=20)
        inserted = await self.repo.insert(user)
        self.assertIsNotNone(inserted.id)
        self.assertEqual(inserted.email, "new@example.com")

    async def test_update_and_delete(self) -> None:
        inserted = await self.repo.insert(UserRow(email="mutate@example.com", age=10))
        inserted.age = 11
        update_count = await self.repo.update(inserted)
        updated = await self.repo.get(inserted.id)

        delete_count = await self.repo.delete(inserted)
        missing = await self.repo.get(inserted.id)

        self.assertEqual(update_count, 1)
        self.assertEqual(updated.age, 11)
        self.assertEqual(delete_count, 1)
        self.assertIsNone(missing)

    async def test_insert_many_update_where_delete_where_and_get_or_create(self) -> None:
        await self.repo.insert_many(
            [
                UserRow(email="m1@example.com", age=10),
                UserRow(email="m2@example.com", age=20),
                UserRow(email="m3@sample.com", age=30),
            ]
        )

        updated = await self.repo.update_where(
            {"age": 99},
            where=C.in_("email", ["m1@example.com", "m2@example.com"]),
        )
        self.assertEqual(updated, 2)

        rows = await self.repo.list(
            where=C.like("email", "%@example.com"),
            order_by=[OrderBy("email")],
        )
        self.assertEqual([r.age for r in rows], [99, 99])

        deleted = await self.repo.delete_where(where=C.like("email", "%@sample.com"))
        self.assertEqual(deleted, 1)
        self.assertEqual(await self.repo.count(), 2)

        first, created_first = await self.repo.get_or_create(
            lookup={"email": "gc@example.com"},
            defaults={"age": 33},
        )
        second, created_second = await self.repo.get_or_create(
            lookup={"email": "gc@example.com"},
            defaults={"age": 77},
        )
        self.assertTrue(created_first)
        self.assertFalse(created_second)
        self.assertEqual(first.id, second.id)
        self.assertEqual(second.age, 33)

    async def test_get_or_create(self) -> None:
        first, created_first = await self.repo.get_or_create(
            lookup={"email": "new@example.com"},
            defaults={"age": 33},
        )
        second, created_second = await self.repo.get_or_create(
            lookup={"email": "new@example.com"},
            defaults={"age": 99},
        )
        self.assertTrue(created_first)
        self.assertFalse(created_second)
        self.assertEqual(first.email, "new@example.com")
        self.assertEqual(first.age, 33)
        self.assertEqual(second.id, first.id)

    async def test_async_relations_create_and_query(self) -> None:
        await apply_schema_async(self.db, AuthorRow)
        await apply_schema_async(self.db, PostRow)
        author_repo = AsyncRepository[AuthorRow](self.db, AuthorRow)
        post_repo = AsyncRepository[PostRow](self.db, PostRow)

        author = await author_repo.create(
            AuthorRow(name="Reader"),
            relations={"posts": [PostRow(title="T1"), PostRow(title="T2")]},
        )
        self.assertIsNotNone(author.id)

        author_with_posts = await author_repo.get_related(author.id, include=["posts"])
        self.assertIsNotNone(author_with_posts)
        self.assertEqual(len(author_with_posts.relations["posts"]), 2)

        posts_with_author = await post_repo.list_related(
            include=["author"],
            order_by=[OrderBy("id")],
        )
        self.assertEqual(len(posts_with_author), 2)
        self.assertTrue(all(item.relations["author"] is not None for item in posts_with_author))

    async def test_create_with_has_many_relations(self) -> None:
        await apply_schema_async(self.db, AuthorRow)
        await apply_schema_async(self.db, PostRow)
        author_repo = AsyncRepository[AuthorRow](self.db, AuthorRow)
        post_repo = AsyncRepository[PostRow](self.db, PostRow)

        author = AuthorRow(name="Alice")
        posts = [PostRow(title="Post A"), PostRow(title="Post B")]
        await author_repo.create(author, relations={"posts": posts})

        self.assertIsNotNone(author.id)
        self.assertEqual(await post_repo.count(), 2)
        loaded_posts = await post_repo.list(order_by=[OrderBy("id")])
        self.assertTrue(all(post.author_id == author.id for post in loaded_posts))

    async def test_get_related_and_list_related(self) -> None:
        await apply_schema_async(self.db, AuthorRow)
        await apply_schema_async(self.db, PostRow)
        author_repo = AsyncRepository[AuthorRow](self.db, AuthorRow)
        post_repo = AsyncRepository[PostRow](self.db, PostRow)

        author = await author_repo.create(
            AuthorRow(name="Reader"),
            relations={"posts": [PostRow(title="T1"), PostRow(title="T2")]},
        )

        author_with_posts = await author_repo.get_related(author.id, include=["posts"])
        self.assertIsNotNone(author_with_posts)
        self.assertEqual(len(author_with_posts.relations["posts"]), 2)

        posts_with_author = await post_repo.list_related(include=["author"], order_by=[OrderBy("id")])
        self.assertEqual(len(posts_with_author), 2)
        self.assertTrue(all(item.relations["author"] is not None for item in posts_with_author))
        self.assertTrue(all(item.relations["author"].name == "Reader" for item in posts_with_author))

    async def test_list_with_condition_order_limit_offset(self) -> None:
        await self._seed_users()
        rows = await self.repo.list(
            where=[C.like("email", "%@example.com"), C.is_not_null("age")],
            order_by=[OrderBy("age", desc=True)],
            limit=1,
            offset=1,
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].email, "alice@example.com")

    async def test_insert_uses_lastrowid_when_returning_is_not_supported(self) -> None:
        fake_db = _NoReturningAsyncDb()
        repo = AsyncRepository[UserRow](fake_db, UserRow)
        user = UserRow(email="fallback@example.com", age=10)
        await repo.insert(user)
        self.assertEqual(user.id, 77)
        self.assertTrue(fake_db.executed)

    async def test_get_and_get_missing(self) -> None:
        inserted = await self.repo.insert(UserRow(email="lookup@example.com", age=50))
        found = await self.repo.get(inserted.id)
        missing = await self.repo.get(9999)

        self.assertEqual(found.email, "lookup@example.com")
        self.assertIsNone(missing)

    async def test_update_and_delete_require_pk(self) -> None:
        missing_pk = UserRow(email="x", age=1)
        with self.assertRaises(ValueError):
            await self.repo.update(missing_pk)
        with self.assertRaises(ValueError):
            await self.repo.delete(missing_pk)

    async def test_list_with_in_and_is_null(self) -> None:
        users = await self._seed_users()
        selected = await self.repo.list(
            where=C.in_("id", [users[0].id, users[1].id]),
            order_by=[OrderBy("id")],
        )
        null_age = await self.repo.list(where=C.is_null("age"))

        self.assertEqual([row.email for row in selected], ["alice@example.com", "bob@example.com"])
        self.assertEqual(len(null_age), 1)
        self.assertEqual(null_age[0].email, "david@example.com")

    async def test_list_with_grouped_conditions(self) -> None:
        await self._seed_users()
        rows = await self.repo.list(
            where=C.or_(
                C.eq("email", "alice@example.com"),
                C.eq("email", "bob@example.com"),
            ),
            order_by=[OrderBy("id")],
        )
        self.assertEqual([row.email for row in rows], ["alice@example.com", "bob@example.com"])

    async def test_list_rejects_invalid_limit_offset(self) -> None:
        with self.assertRaises(ValueError):
            await self.repo.list(limit=0)
        with self.assertRaises(ValueError):
            await self.repo.list(offset=-1)

    async def test_count_and_exists(self) -> None:
        await self._seed_users()
        self.assertEqual(await self.repo.count(), 4)
        self.assertEqual(await self.repo.count(where=C.like("email", "%@example.com")), 3)
        self.assertTrue(await self.repo.exists(where=C.eq("email", "alice@example.com")))
        self.assertFalse(await self.repo.exists(where=C.eq("email", "nobody@example.com")))

    async def test_enum_and_json_codec_serialize_and_deserialize(self) -> None:
        conn = sqlite3.connect(":memory:")
        self.addCleanup(conn.close)
        db = AsyncDatabase(conn, SQLiteDialect())
        await apply_schema_async(db, TicketRow)
        repo = AsyncRepository[TicketRow](db, TicketRow)

        inserted = await repo.insert(
            TicketRow(
                status=TicketStatus.CLOSED,
                payload={"priority": 2, "tags": ["bug"]},
                tags=["bug", "urgent"],
            )
        )
        self.assertIsNotNone(inserted.id)

        loaded = await repo.get(inserted.id)
        self.assertEqual(loaded.status, TicketStatus.CLOSED)
        self.assertEqual(loaded.payload, {"priority": 2, "tags": ["bug"]})
        self.assertEqual(loaded.tags, ["bug", "urgent"])

        filtered = await repo.list(where=C.eq("status", TicketStatus.CLOSED))
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0].id, inserted.id)

        updated = await repo.update_where(
            {"payload": {"priority": 1}},
            where=C.eq("status", TicketStatus.CLOSED),
        )
        self.assertEqual(updated, 1)

        refreshed = await repo.get(inserted.id)
        self.assertEqual(refreshed.payload, {"priority": 1})
        self.assertEqual(refreshed.tags, ["bug", "urgent"])

    async def test_insert_many(self) -> None:
        inserted = await self.repo.insert_many(
            [
                UserRow(email="m1@example.com", age=10),
                UserRow(email="m2@example.com", age=20),
            ]
        )
        self.assertEqual(len(inserted), 2)
        self.assertEqual(await self.repo.count(), 2)
        self.assertTrue(all(item.id is not None for item in inserted))

    async def test_update_where(self) -> None:
        await self._seed_users()
        updated = await self.repo.update_where(
            {"age": 40},
            where=C.or_(
                C.eq("email", "alice@example.com"),
                C.eq("email", "bob@example.com"),
            ),
        )
        rows = await self.repo.list(
            where=C.in_("email", ["alice@example.com", "bob@example.com"]),
            order_by=[OrderBy("id")],
        )
        self.assertEqual(updated, 2)
        self.assertEqual([row.age for row in rows], [40, 40])

    async def test_update_where_validations(self) -> None:
        await self._seed_users()
        with self.assertRaises(ValueError):
            await self.repo.update_where({}, where=C.eq("id", 1))
        with self.assertRaises(ValueError):
            await self.repo.update_where({"age": 20}, where=None)
        with self.assertRaises(ValueError):
            await self.repo.update_where({"unknown": 1}, where=C.eq("id", 1))
        with self.assertRaises(ValueError):
            await self.repo.update_where({"id": 123}, where=C.eq("id", 1))

    async def test_delete_where(self) -> None:
        await self._seed_users()
        deleted = await self.repo.delete_where(
            where=C.like("email", "%@example.com"),
        )
        self.assertEqual(deleted, 3)
        self.assertEqual(await self.repo.count(), 1)

    async def test_delete_where_requires_where(self) -> None:
        with self.assertRaises(ValueError):
            await self.repo.delete_where(where=None)

    async def test_get_or_create_insert_first_conflict_path(self) -> None:
        existing = await self.repo.insert(UserRow(email="conflict@example.com", age=20))

        insert_call_count = 0
        original_insert = self.repo.insert

        async def tracked_insert(obj: UserRow) -> UserRow:
            nonlocal insert_call_count
            insert_call_count += 1
            return await original_insert(obj)

        self.repo.insert = tracked_insert  # type: ignore[method-assign]
        row, created = await self.repo.get_or_create(
            lookup={"email": "conflict@example.com"},
            defaults={"age": 99},
        )

        self.assertFalse(created)
        self.assertEqual(insert_call_count, 1)
        self.assertEqual(row.id, existing.id)

    async def test_get_or_create_requires_lookup(self) -> None:
        with self.assertRaises(ValueError):
            await self.repo.get_or_create(lookup={})

    async def test_get_or_create_requires_unique_lookup_constraint(self) -> None:
        conn = sqlite3.connect(":memory:")
        db = AsyncDatabase(conn, SQLiteDialect())
        await apply_schema_async(db, NonUniqueLookupRow)
        repo = AsyncRepository[NonUniqueLookupRow](db, NonUniqueLookupRow)

        with self.assertRaises(ValueError):
            await repo.get_or_create(lookup={"email": "x@example.com"})
        conn.close()

    async def test_get_or_create_reraises_non_integrity_errors(self) -> None:
        async def failing_insert(obj: UserRow) -> UserRow:  # noqa: ARG001
            raise RuntimeError("boom")

        self.repo.insert = failing_insert  # type: ignore[method-assign]
        with self.assertRaises(RuntimeError):
            await self.repo.get_or_create(lookup={"email": "boom@example.com"})

    async def test_get_or_create_reraises_integrity_when_row_still_missing(self) -> None:
        async def failing_insert(obj: UserRow) -> UserRow:  # noqa: ARG001
            raise sqlite3.IntegrityError()

        async def empty_list(*_args, **_kwargs) -> list[UserRow]:  # noqa: ANN002, ANN003
            return []

        self.repo.insert = failing_insert  # type: ignore[method-assign]
        self.repo.list = empty_list  # type: ignore[method-assign]
        with self.assertRaises(sqlite3.IntegrityError):
            await self.repo.get_or_create(lookup={"email": "missing@example.com"})

    async def test_repository_requires_dataclass_and_single_pk(self) -> None:
        with self.assertRaises(TypeError):
            AsyncRepository(self.db, PlainModel)
        with self.assertRaises(ValueError):
            AsyncRepository(self.db, MultiPkRow)

    async def test_insert_with_only_auto_pk_model_uses_default_values(self) -> None:
        conn = sqlite3.connect(":memory:")
        self.addCleanup(conn.close)
        db = AsyncDatabase(conn, SQLiteDialect())
        await apply_schema_async(db, OnlyPkRow)
        repo = AsyncRepository[OnlyPkRow](db, OnlyPkRow)

        obj = await repo.insert(OnlyPkRow())
        self.assertIsNotNone(obj.id)

    async def test_update_with_only_auto_pk_model_raises_clear_error(self) -> None:
        conn = sqlite3.connect(":memory:")
        self.addCleanup(conn.close)
        db = AsyncDatabase(conn, SQLiteDialect())
        await apply_schema_async(db, OnlyPkRow)
        repo = AsyncRepository[OnlyPkRow](db, OnlyPkRow)

        await db.execute('INSERT INTO "onlypkrow" DEFAULT VALUES;')
        obj = await repo.get(1)
        self.assertIsNotNone(obj)
        with self.assertRaises(ValueError):
            await repo.update(obj)

    async def test_create_with_belongs_to_relation(self) -> None:
        _conn = sqlite3.connect(":memory:")
        self.addCleanup(_conn.close)
        db = AsyncDatabase(_conn, SQLiteDialect())
        await apply_schema_async(db, AuthorRow)
        await apply_schema_async(db, PostRow)
        author_repo = AsyncRepository[AuthorRow](db, AuthorRow)
        post_repo = AsyncRepository[PostRow](db, PostRow)

        post = PostRow(title="Nested")
        await post_repo.create(post, relations={"author": AuthorRow(name="Nested Author")})

        self.assertIsNotNone(post.id)
        self.assertIsNotNone(post.author_id)
        self.assertEqual(await author_repo.count(), 1)

    async def test_get_related_returns_none_for_missing_row(self) -> None:
        _conn, author_repo, _ = await self._relation_repositories()
        self.assertIsNone(await author_repo.get_related(9999, include=["posts"]))

    async def test_create_with_unknown_relation_raises(self) -> None:
        _conn, author_repo, post_repo = await self._relation_repositories()
        with self.assertRaises(ValueError):
            await author_repo.create(AuthorRow(name="A"), relations={"missing_relation": []})

        self.assertEqual(await author_repo.count(), 0)
        self.assertEqual(await post_repo.count(), 0)

    async def test_create_has_many_requires_sequence_and_rolls_back(self) -> None:
        _conn, author_repo, post_repo = await self._relation_repositories()
        with self.assertRaises(TypeError):
            await author_repo.create(
                AuthorRow(name="A"),
                relations={"posts": PostRow(title="must-be-sequence")},  # type: ignore[arg-type]
            )

        self.assertEqual(await author_repo.count(), 0)
        self.assertEqual(await post_repo.count(), 0)

    async def test_create_has_many_rejects_invalid_child_type_and_rolls_back(self) -> None:
        _conn, author_repo, post_repo = await self._relation_repositories()
        with self.assertRaises(TypeError):
            await author_repo.create(
                AuthorRow(name="A"),
                relations={
                    "posts": [
                        PostRow(title="valid"),
                        AuthorRow(name="invalid-child"),  # type: ignore[list-item]
                    ]
                },
            )

        self.assertEqual(await author_repo.count(), 0)
        self.assertEqual(await post_repo.count(), 0)

    async def test_create_belongs_to_requires_model_type(self) -> None:
        _conn, author_repo, post_repo = await self._relation_repositories()
        with self.assertRaises(TypeError):
            await post_repo.create(PostRow(title="invalid"), relations={"author": "not-a-model"})  # type: ignore[arg-type]

        self.assertEqual(await author_repo.count(), 0)
        self.assertEqual(await post_repo.count(), 0)

    async def test_get_related_for_belongs_to_returns_none_when_fk_is_null(self) -> None:
        _conn, _, post_repo = await self._relation_repositories()
        post = await post_repo.insert(PostRow(title="Orphan", author_id=None))
        result = await post_repo.get_related(post.id, include=["author"])

        self.assertIsNotNone(result)
        self.assertIsNone(result.relations["author"])

    async def test_list_related_validates_include_items(self) -> None:
        _conn, _, post_repo = await self._relation_repositories()
        with self.assertRaises(TypeError):
            await post_repo.list_related(include=["author", ""])
        with self.assertRaises(TypeError):
            await post_repo.list_related(include=[123])  # type: ignore[list-item]

    async def test_list_related_validates_unknown_relation_name(self) -> None:
        _conn, _, post_repo = await self._relation_repositories()
        with self.assertRaises(ValueError):
            await post_repo.list_related(include=["missing"])

    async def test_list_related_deduplicates_include_names(self) -> None:
        _conn, _, post_repo = await self._relation_repositories()
        await post_repo.create(PostRow(title="One"), relations={"author": AuthorRow(name="Single")})

        rows = await post_repo.list_related(include=["author", "author"])
        self.assertEqual(len(rows), 1)
        self.assertEqual(list(rows[0].relations.keys()), ["author"])

    async def test_relations_can_be_inferred_from_fk_metadata(self) -> None:
        conn = sqlite3.connect(":memory:")
        db = AsyncDatabase(conn, SQLiteDialect())
        await apply_schema_async(db, AutoAuthor)
        await apply_schema_async(db, AutoPost)
        author_repo = AsyncRepository[AutoAuthor](db, AutoAuthor)
        post_repo = AsyncRepository[AutoPost](db, AutoPost)

        author = await author_repo.create(
            AutoAuthor(name="Inferred"),
            relations={"posts": [AutoPost(title="T1"), AutoPost(title="T2")]},
        )
        post_with_author = await post_repo.create(
            AutoPost(title="Nested"),
            relations={"author": AutoAuthor(name="Nested Author")},
        )

        self.assertIsNotNone(author.id)
        self.assertIsNotNone(post_with_author.author_id)
        self.assertEqual(await post_repo.count(), 3)
        self.assertEqual(await author_repo.count(), 2)

        author_with_posts = await author_repo.get_related(author.id, include=["posts"])
        self.assertIsNotNone(author_with_posts)
        self.assertEqual(len(author_with_posts.relations["posts"]), 2)

        rows = await post_repo.list_related(include=["author"], order_by=[OrderBy("id")])
        self.assertEqual(len(rows), 3)
        self.assertTrue(all(item.relations["author"] is not None for item in rows))
        conn.close()


if __name__ == "__main__":
    unittest.main()
