from __future__ import annotations

import importlib
import os
import unittest
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from mini_orm import (
    AsyncUnifiedRepository,
    C,
    AsyncDatabase,
    AsyncRepository,
    OrderBy,
    PostgresDialect,
    apply_schema_async,
)


def _load_connect() -> Any:
    for module_name in ("psycopg", "psycopg2"):
        try:
            module = importlib.import_module(module_name)
        except (ModuleNotFoundError, ImportError):
            continue
        connect = getattr(module, "connect", None)
        if connect is not None:
            return connect
    return None


POSTGRES_CONNECT = _load_connect()
HAS_POSTGRES_DRIVER = POSTGRES_CONNECT is not None


@dataclass
class AsyncPgDialectUser:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = field(default="", metadata={"unique_index": True})
    age: Optional[int] = None


@dataclass
class AsyncPgDialectAuthor:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    name: str = ""


@dataclass
class AsyncPgDialectPost:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    author_id: Optional[int] = field(default=None, metadata={"fk": (AsyncPgDialectAuthor, "id")})
    title: str = ""


class AsyncPgCodecStatus(str, Enum):
    OPEN = "open"
    CLOSED = "closed"


@dataclass
class AsyncPgCodecTicket:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    status: AsyncPgCodecStatus = AsyncPgCodecStatus.OPEN
    payload: dict[str, Any] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)


@dataclass
class AsyncPgAutoSchemaUserV1:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = ""


AsyncPgAutoSchemaUserV1.__table__ = "asyncpgautoschemauser"


@dataclass
class AsyncPgAutoSchemaUserV2:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = ""
    age: Optional[int] = None


AsyncPgAutoSchemaUserV2.__table__ = "asyncpgautoschemauser"


AsyncPgDialectAuthor.__relations__ = {
    "posts": {
        "model": AsyncPgDialectPost,
        "local_key": "id",
        "remote_key": "author_id",
        "type": "has_many",
    }
}

AsyncPgDialectPost.__relations__ = {
    "author": {
        "model": AsyncPgDialectAuthor,
        "local_key": "author_id",
        "remote_key": "id",
        "type": "belongs_to",
    }
}


@unittest.skipUnless(HAS_POSTGRES_DRIVER, "psycopg/psycopg2 is not installed")
class AsyncRepositoryPostgresDialectTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        password = os.getenv(
            "MINI_ORM_PG_PASSWORD",
            os.getenv("PGPASSWORD", os.getenv("POSTGRES_PASSWORD", "password")),
        )
        params = {
            "host": os.getenv("MINI_ORM_PG_HOST", os.getenv("PGHOST", "localhost")),
            "port": int(os.getenv("MINI_ORM_PG_PORT", os.getenv("PGPORT", "5432"))),
            "user": os.getenv("MINI_ORM_PG_USER", os.getenv("PGUSER", "postgres")),
            "password": password,
            "dbname": os.getenv("MINI_ORM_PG_DATABASE", os.getenv("PGDATABASE", "postgres")),
        }

        try:
            cls.conn = POSTGRES_CONNECT(**params)
        except Exception as exc:
            raise unittest.SkipTest(
                f"PostgreSQL is not reachable at {params['host']}:{params['port']} "
                f"with configured credentials: {exc}"
            ) from exc

        cls.db = AsyncDatabase(cls.conn, PostgresDialect())
        cls.repo = AsyncRepository[AsyncPgDialectUser](cls.db, AsyncPgDialectUser)
        cls.author_repo = AsyncRepository[AsyncPgDialectAuthor](cls.db, AsyncPgDialectAuthor)
        cls.post_repo = AsyncRepository[AsyncPgDialectPost](cls.db, AsyncPgDialectPost)
        cls.codec_repo = AsyncRepository[AsyncPgCodecTicket](cls.db, AsyncPgCodecTicket)

    @classmethod
    def tearDownClass(cls) -> None:
        conn = getattr(cls, "conn", None)
        if conn is not None:
            conn.close()

    async def asyncSetUp(self) -> None:
        async with self.db.transaction():
            await self.db.execute('DROP TABLE IF EXISTS "asyncpgcodecticket";')
            await self.db.execute('DROP TABLE IF EXISTS "asyncpgdialectpost";')
            await self.db.execute('DROP TABLE IF EXISTS "asyncpgdialectauthor";')
            await self.db.execute('DROP TABLE IF EXISTS "asyncpgdialectuser";')
            await self.db.execute('DROP TABLE IF EXISTS "asyncpgautoschemauser";')
        await apply_schema_async(self.db, AsyncPgDialectUser)
        await apply_schema_async(self.db, AsyncPgDialectAuthor)
        await apply_schema_async(self.db, AsyncPgDialectPost)
        await apply_schema_async(self.db, AsyncPgCodecTicket)

    async def test_insert_update_delete_roundtrip(self) -> None:
        async with self.db.transaction():
            user = await self.repo.insert(AsyncPgDialectUser(email="alice@example.com", age=30))

        self.assertIsNotNone(user.id)

        fetched = await self.repo.get(user.id)
        self.assertIsNotNone(fetched)
        self.assertEqual(fetched.email, "alice@example.com")

        user.age = 31
        async with self.db.transaction():
            updated = await self.repo.update(user)
        self.assertEqual(updated, 1)
        self.assertEqual((await self.repo.get(user.id)).age, 31)

        async with self.db.transaction():
            deleted = await self.repo.delete(user)
        self.assertEqual(deleted, 1)
        self.assertIsNone(await self.repo.get(user.id))

    async def test_filters_order_and_pagination_with_format_params(self) -> None:
        async with self.db.transaction():
            await self.repo.insert_many(
                [
                    AsyncPgDialectUser(email="alice@example.com", age=25),
                    AsyncPgDialectUser(email="bob@example.com", age=30),
                    AsyncPgDialectUser(email="charlie@sample.com", age=35),
                    AsyncPgDialectUser(email="david@example.com", age=None),
                ]
            )

        rows = await self.repo.list(
            where=[C.like("email", "%@example.com"), C.is_not_null("age")],
            order_by=[OrderBy("age", desc=True)],
            limit=1,
            offset=1,
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].email, "alice@example.com")
        self.assertEqual(await self.repo.count(), 4)
        self.assertTrue(await self.repo.exists(where=C.eq("email", "bob@example.com")))

    async def test_create_with_has_many_relation(self) -> None:
        author = AsyncPgDialectAuthor(name="Alice")
        posts = [AsyncPgDialectPost(title="P1"), AsyncPgDialectPost(title="P2")]

        await self.author_repo.create(author, relations={"posts": posts})

        self.assertIsNotNone(author.id)
        self.assertEqual(await self.post_repo.count(), 2)
        loaded_posts = await self.post_repo.list(order_by=[OrderBy("id")])
        self.assertTrue(all(post.author_id == author.id for post in loaded_posts))

    async def test_async_unified_repository_reuses_cached_repositories(self) -> None:
        unified = AsyncUnifiedRepository(self.db)

        user_repo_1 = unified.repo(AsyncPgDialectUser)
        user_repo_2 = unified.repo(AsyncPgDialectUser)
        author_repo = unified.repo(AsyncPgDialectAuthor)

        self.assertIs(user_repo_1, user_repo_2)
        self.assertIsNot(user_repo_1, author_repo)

    async def test_async_unified_repository_crud_and_relations(self) -> None:
        unified = AsyncUnifiedRepository(self.db)

        async with self.db.transaction():
            user = await unified.insert(AsyncPgDialectUser, AsyncPgDialectUser(email="hub@example.com", age=20))
        self.assertIsNotNone(user.id)
        self.assertEqual(await unified.count(AsyncPgDialectUser), 1)

        loaded = await unified.get(AsyncPgDialectUser, user.id)
        self.assertIsNotNone(loaded)
        if loaded is None:
            self.fail("Expected inserted row to exist.")

        loaded.age = 21
        async with self.db.transaction():
            self.assertEqual(await unified.update(AsyncPgDialectUser, loaded), 1)
        async with self.db.transaction():
            self.assertEqual(await unified.delete(AsyncPgDialectUser, loaded), 1)
        self.assertEqual(await unified.count(AsyncPgDialectUser), 0)

        author = await unified.create(
            AsyncPgDialectAuthor,
            AsyncPgDialectAuthor(name="Unified Reader"),
            relations={"posts": [AsyncPgDialectPost(title="T1"), AsyncPgDialectPost(title="T2")]},
        )
        self.assertIsNotNone(author.id)
        if author.id is None:
            self.fail("Expected author auto PK.")

        author_with_posts = await unified.get_related(
            AsyncPgDialectAuthor,
            author.id,
            include=["posts"],
        )
        self.assertIsNotNone(author_with_posts)
        if author_with_posts is None:
            self.fail("Expected related result for existing author.")
        self.assertEqual(len(author_with_posts.relations["posts"]), 2)

    async def test_async_repository_auto_schema_additive_for_postgres(self) -> None:
        repo_v1 = AsyncRepository(self.db, AsyncPgAutoSchemaUserV1, auto_schema=True)
        async with self.db.transaction():
            await repo_v1.insert(AsyncPgAutoSchemaUserV1(email="a@example.com"))

        repo_v2 = AsyncRepository(self.db, AsyncPgAutoSchemaUserV2, auto_schema=True)
        async with self.db.transaction():
            await repo_v2.insert(AsyncPgAutoSchemaUserV2(email="b@example.com", age=20))
        self.assertEqual(await repo_v2.count(), 2)

    async def test_async_unified_repository_auto_schema_additive_for_postgres(self) -> None:
        unified = AsyncUnifiedRepository(self.db, auto_schema=True)
        async with self.db.transaction():
            await unified.insert(
                AsyncPgAutoSchemaUserV1,
                AsyncPgAutoSchemaUserV1(email="a@example.com"),
            )
            await unified.insert(
                AsyncPgAutoSchemaUserV2,
                AsyncPgAutoSchemaUserV2(email="b@example.com", age=20),
            )
        self.assertEqual(await unified.count(AsyncPgAutoSchemaUserV2), 2)

    async def test_get_related_and_list_related(self) -> None:
        author = await self.author_repo.create(
            AsyncPgDialectAuthor(name="Reader"),
            relations={
                "posts": [
                    AsyncPgDialectPost(title="T1"),
                    AsyncPgDialectPost(title="T2"),
                ]
            },
        )

        author_with_posts = await self.author_repo.get_related(author.id, include=["posts"])
        self.assertIsNotNone(author_with_posts)
        self.assertEqual(len(author_with_posts.relations["posts"]), 2)

        posts_with_author = await self.post_repo.list_related(
            include=["author"],
            order_by=[OrderBy("id")],
        )
        self.assertEqual(len(posts_with_author), 2)
        self.assertTrue(all(item.relations["author"] is not None for item in posts_with_author))
        self.assertTrue(all(item.relations["author"].name == "Reader" for item in posts_with_author))

    async def test_enum_and_json_codec_roundtrip(self) -> None:
        async with self.db.transaction():
            ticket = await self.codec_repo.insert(
                AsyncPgCodecTicket(
                    status=AsyncPgCodecStatus.CLOSED,
                    payload={"priority": 2, "tags": ["bug"]},
                    tags=["bug", "urgent"],
                )
            )
        self.assertIsNotNone(ticket.id)

        loaded = await self.codec_repo.get(ticket.id)
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.status, AsyncPgCodecStatus.CLOSED)
        self.assertEqual(loaded.payload, {"priority": 2, "tags": ["bug"]})
        self.assertEqual(loaded.tags, ["bug", "urgent"])

        rows = await self.codec_repo.list(where=C.eq("status", AsyncPgCodecStatus.CLOSED))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].id, ticket.id)

        async with self.db.transaction():
            updated = await self.codec_repo.update_where(
                {"payload": {"priority": 1}},
                where=C.eq("status", AsyncPgCodecStatus.CLOSED),
            )
        self.assertEqual(updated, 1)
        refreshed = await self.codec_repo.get(ticket.id)
        self.assertEqual(refreshed.payload, {"priority": 1})
        self.assertEqual(refreshed.tags, ["bug", "urgent"])


if __name__ == "__main__":
    unittest.main()
