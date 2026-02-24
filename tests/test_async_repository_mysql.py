from __future__ import annotations

import importlib
import os
import unittest
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from mini_orm import (
    C,
    AsyncDatabase,
    AsyncRepository,
    MySQLDialect,
    OrderBy,
    apply_schema_async,
)


def _load_mysql_driver() -> tuple[str, Any] | tuple[None, None]:
    for module_name in ("MySQLdb", "pymysql", "mysql.connector"):
        try:
            module = importlib.import_module(module_name)
        except ImportError:
            continue
        connect = getattr(module, "connect", None)
        if connect is not None:
            return module_name, connect
    return None, None


MYSQL_DRIVER, MYSQL_CONNECT = _load_mysql_driver()
HAS_MYSQL_DRIVER = MYSQL_CONNECT is not None


@dataclass
class AsyncMySQLDialectUser:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = ""
    age: Optional[int] = None


@dataclass
class AsyncMySQLDialectAuthor:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    name: str = ""


@dataclass
class AsyncMySQLDialectPost:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    author_id: Optional[int] = field(default=None, metadata={"fk": (AsyncMySQLDialectAuthor, "id")})
    title: str = ""


class AsyncMySQLCodecStatus(str, Enum):
    OPEN = "open"
    CLOSED = "closed"


@dataclass
class AsyncMySQLCodecTicket:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    status: AsyncMySQLCodecStatus = AsyncMySQLCodecStatus.OPEN
    payload: dict[str, Any] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)


AsyncMySQLDialectAuthor.__relations__ = {
    "posts": {
        "model": AsyncMySQLDialectPost,
        "local_key": "id",
        "remote_key": "author_id",
        "type": "has_many",
    }
}

AsyncMySQLDialectPost.__relations__ = {
    "author": {
        "model": AsyncMySQLDialectAuthor,
        "local_key": "author_id",
        "remote_key": "id",
        "type": "belongs_to",
    }
}


def _mysql_connect(
    *,
    driver_name: str,
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
) -> Any:
    if driver_name == "MySQLdb":
        return MYSQL_CONNECT(  # type: ignore[misc]
            host=host,
            port=port,
            user=user,
            passwd=password,
            db=database,
            charset="utf8mb4",
        )
    if driver_name == "pymysql":
        return MYSQL_CONNECT(  # type: ignore[misc]
            host=host,
            port=port,
            user=user,
            password=password,
            db=database,
            charset="utf8mb4",
        )
    return MYSQL_CONNECT(  # mysql.connector # type: ignore[misc]
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
    )


@unittest.skipUnless(HAS_MYSQL_DRIVER, "mysql driver is not installed")
class AsyncRepositoryMySQLDialectTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.host = os.getenv("MINI_ORM_MYSQL_HOST", os.getenv("MYSQL_HOST", "localhost"))
        cls.port = int(os.getenv("MINI_ORM_MYSQL_PORT", os.getenv("MYSQL_PORT", "3306")))
        cls.user = os.getenv("MINI_ORM_MYSQL_USER", os.getenv("MYSQL_USER", "root"))
        cls.password = os.getenv(
            "MINI_ORM_MYSQL_PASSWORD",
            os.getenv("MYSQL_ROOT_PASSWORD", os.getenv("MYSQL_PASSWORD", "password")),
        )
        cls.database = os.getenv(
            "MINI_ORM_MYSQL_DATABASE",
            os.getenv("MYSQL_DATABASE", "mini_orm_test"),
        )
        bootstrap_db = os.getenv("MINI_ORM_MYSQL_BOOTSTRAP_DB", "mysql")

        try:
            bootstrap_conn = _mysql_connect(
                driver_name=MYSQL_DRIVER,  # type: ignore[arg-type]
                host=cls.host,
                port=cls.port,
                user=cls.user,
                password=cls.password,
                database=bootstrap_db,
            )
            cur = bootstrap_conn.cursor()
            cur.execute(f"CREATE DATABASE IF NOT EXISTS `{cls.database}`;")
            bootstrap_conn.commit()
            cur.close()
            bootstrap_conn.close()

            cls.conn = _mysql_connect(
                driver_name=MYSQL_DRIVER,  # type: ignore[arg-type]
                host=cls.host,
                port=cls.port,
                user=cls.user,
                password=cls.password,
                database=cls.database,
            )
        except Exception as exc:
            raise unittest.SkipTest(
                f"MySQL is not reachable at {cls.host}:{cls.port} "
                f"with configured credentials: {exc}"
            ) from exc

        cls.db = AsyncDatabase(cls.conn, MySQLDialect())
        cls.repo = AsyncRepository[AsyncMySQLDialectUser](cls.db, AsyncMySQLDialectUser)
        cls.author_repo = AsyncRepository[AsyncMySQLDialectAuthor](cls.db, AsyncMySQLDialectAuthor)
        cls.post_repo = AsyncRepository[AsyncMySQLDialectPost](cls.db, AsyncMySQLDialectPost)
        cls.codec_repo = AsyncRepository[AsyncMySQLCodecTicket](cls.db, AsyncMySQLCodecTicket)

    @classmethod
    def tearDownClass(cls) -> None:
        conn = getattr(cls, "conn", None)
        if conn is not None:
            conn.close()

    async def asyncSetUp(self) -> None:
        async with self.db.transaction():
            await self.db.execute("DROP TABLE IF EXISTS `asyncmysqlcodecticket`;")
            await self.db.execute("DROP TABLE IF EXISTS `asyncmysqldialectpost`;")
            await self.db.execute("DROP TABLE IF EXISTS `asyncmysqldialectauthor`;")
            await self.db.execute("DROP TABLE IF EXISTS `asyncmysqldialectuser`;")
        await apply_schema_async(self.db, AsyncMySQLDialectUser)
        await apply_schema_async(self.db, AsyncMySQLDialectAuthor)
        await apply_schema_async(self.db, AsyncMySQLDialectPost)
        await apply_schema_async(self.db, AsyncMySQLCodecTicket)

    async def test_insert_update_delete_roundtrip(self) -> None:
        async with self.db.transaction():
            user = await self.repo.insert(AsyncMySQLDialectUser(email="alice@example.com", age=30))

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
                    AsyncMySQLDialectUser(email="alice@example.com", age=25),
                    AsyncMySQLDialectUser(email="bob@example.com", age=30),
                    AsyncMySQLDialectUser(email="charlie@sample.com", age=35),
                    AsyncMySQLDialectUser(email="david@example.com", age=None),
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
        author = AsyncMySQLDialectAuthor(name="Alice")
        posts = [AsyncMySQLDialectPost(title="P1"), AsyncMySQLDialectPost(title="P2")]

        await self.author_repo.create(author, relations={"posts": posts})

        self.assertIsNotNone(author.id)
        self.assertEqual(await self.post_repo.count(), 2)
        loaded_posts = await self.post_repo.list(order_by=[OrderBy("id")])
        self.assertTrue(all(post.author_id == author.id for post in loaded_posts))

    async def test_get_related_and_list_related(self) -> None:
        author = await self.author_repo.create(
            AsyncMySQLDialectAuthor(name="Reader"),
            relations={
                "posts": [
                    AsyncMySQLDialectPost(title="T1"),
                    AsyncMySQLDialectPost(title="T2"),
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
                AsyncMySQLCodecTicket(
                    status=AsyncMySQLCodecStatus.CLOSED,
                    payload={"priority": 2, "tags": ["bug"]},
                    tags=["bug", "urgent"],
                )
            )
        self.assertIsNotNone(ticket.id)

        loaded = await self.codec_repo.get(ticket.id)
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.status, AsyncMySQLCodecStatus.CLOSED)
        self.assertEqual(loaded.payload, {"priority": 2, "tags": ["bug"]})
        self.assertEqual(loaded.tags, ["bug", "urgent"])

        rows = await self.codec_repo.list(where=C.eq("status", AsyncMySQLCodecStatus.CLOSED))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].id, ticket.id)

        async with self.db.transaction():
            updated = await self.codec_repo.update_where(
                {"payload": {"priority": 1}},
                where=C.eq("status", AsyncMySQLCodecStatus.CLOSED),
            )
        self.assertEqual(updated, 1)
        refreshed = await self.codec_repo.get(ticket.id)
        self.assertEqual(refreshed.payload, {"priority": 1})
        self.assertEqual(refreshed.tags, ["bug", "urgent"])


if __name__ == "__main__":
    unittest.main()
