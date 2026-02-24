from __future__ import annotations

import importlib
import os
import unittest
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from mini_orm import C, Database, OrderBy, PostgresDialect, Repository, apply_schema


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
class PgDialectUser:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = field(default="", metadata={"unique_index": True})
    age: Optional[int] = None


@dataclass
class PgDialectAuthor:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    name: str = ""


@dataclass
class PgDialectPost:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    author_id: Optional[int] = field(default=None, metadata={"fk": (PgDialectAuthor, "id")})
    title: str = ""


class PgCodecStatus(str, Enum):
    OPEN = "open"
    CLOSED = "closed"


@dataclass
class PgCodecTicket:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    status: PgCodecStatus = PgCodecStatus.OPEN
    payload: dict[str, Any] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)


PgDialectAuthor.__relations__ = {
    "posts": {
        "model": PgDialectPost,
        "local_key": "id",
        "remote_key": "author_id",
        "type": "has_many",
    }
}

PgDialectPost.__relations__ = {
    "author": {
        "model": PgDialectAuthor,
        "local_key": "author_id",
        "remote_key": "id",
        "type": "belongs_to",
    }
}


@unittest.skipUnless(HAS_POSTGRES_DRIVER, "psycopg/psycopg2 is not installed")
class RepositoryPostgresDialectTests(unittest.TestCase):
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
                "PostgreSQL is not reachable at localhost:5432 "
                f"with configured credentials: {exc}"
            ) from exc

        cls.db = Database(cls.conn, PostgresDialect())
        cls.repo = Repository[PgDialectUser](cls.db, PgDialectUser)
        cls.author_repo = Repository[PgDialectAuthor](cls.db, PgDialectAuthor)
        cls.post_repo = Repository[PgDialectPost](cls.db, PgDialectPost)
        cls.codec_repo = Repository[PgCodecTicket](cls.db, PgCodecTicket)

    @classmethod
    def tearDownClass(cls) -> None:
        conn = getattr(cls, "conn", None)
        if conn is not None:
            conn.close()

    def setUp(self) -> None:
        with self.db.transaction():
            self.db.execute('DROP TABLE IF EXISTS "pgcodecticket";')
            self.db.execute('DROP TABLE IF EXISTS "pgdialectpost";')
            self.db.execute('DROP TABLE IF EXISTS "pgdialectauthor";')
            self.db.execute('DROP TABLE IF EXISTS "pgdialectuser";')
        apply_schema(self.db, PgDialectUser)
        apply_schema(self.db, PgDialectAuthor)
        apply_schema(self.db, PgDialectPost)
        apply_schema(self.db, PgCodecTicket)

    def test_insert_update_delete_roundtrip(self) -> None:
        with self.db.transaction():
            user = self.repo.insert(PgDialectUser(email="alice@example.com", age=30))

        self.assertIsNotNone(user.id)

        fetched = self.repo.get(user.id)
        self.assertIsNotNone(fetched)
        self.assertEqual(fetched.email, "alice@example.com")

        user.age = 31
        with self.db.transaction():
            updated = self.repo.update(user)
        self.assertEqual(updated, 1)
        self.assertEqual(self.repo.get(user.id).age, 31)

        with self.db.transaction():
            deleted = self.repo.delete(user)
        self.assertEqual(deleted, 1)
        self.assertIsNone(self.repo.get(user.id))

    def test_filters_order_and_pagination_with_format_params(self) -> None:
        with self.db.transaction():
            self.repo.insert_many(
                [
                    PgDialectUser(email="alice@example.com", age=25),
                    PgDialectUser(email="bob@example.com", age=30),
                    PgDialectUser(email="charlie@sample.com", age=35),
                    PgDialectUser(email="david@example.com", age=None),
                ]
            )

        rows = self.repo.list(
            where=[C.like("email", "%@example.com"), C.is_not_null("age")],
            order_by=[OrderBy("age", desc=True)],
            limit=1,
            offset=1,
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].email, "alice@example.com")
        self.assertEqual(self.repo.count(), 4)
        self.assertTrue(self.repo.exists(where=C.eq("email", "bob@example.com")))

    def test_create_with_has_many_relation(self) -> None:
        author = PgDialectAuthor(name="Alice")
        posts = [PgDialectPost(title="P1"), PgDialectPost(title="P2")]

        self.author_repo.create(author, relations={"posts": posts})

        self.assertIsNotNone(author.id)
        self.assertEqual(self.post_repo.count(), 2)
        loaded_posts = self.post_repo.list(order_by=[OrderBy("id")])
        self.assertTrue(all(post.author_id == author.id for post in loaded_posts))

    def test_get_related_and_list_related(self) -> None:
        author = self.author_repo.create(
            PgDialectAuthor(name="Reader"),
            relations={"posts": [PgDialectPost(title="T1"), PgDialectPost(title="T2")]},
        )

        author_with_posts = self.author_repo.get_related(author.id, include=["posts"])
        self.assertIsNotNone(author_with_posts)
        self.assertEqual(len(author_with_posts.relations["posts"]), 2)

        posts_with_author = self.post_repo.list_related(include=["author"], order_by=[OrderBy("id")])
        self.assertEqual(len(posts_with_author), 2)
        self.assertTrue(all(item.relations["author"] is not None for item in posts_with_author))
        self.assertTrue(all(item.relations["author"].name == "Reader" for item in posts_with_author))

    def test_enum_and_json_codec_roundtrip(self) -> None:
        with self.db.transaction():
            ticket = self.codec_repo.insert(
                PgCodecTicket(
                    status=PgCodecStatus.CLOSED,
                    payload={"priority": 2, "tags": ["bug"]},
                    tags=["bug", "urgent"],
                )
            )

        self.assertIsNotNone(ticket.id)

        loaded = self.codec_repo.get(ticket.id)
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.status, PgCodecStatus.CLOSED)
        self.assertEqual(loaded.payload, {"priority": 2, "tags": ["bug"]})
        self.assertEqual(loaded.tags, ["bug", "urgent"])

        rows = self.codec_repo.list(where=C.eq("status", PgCodecStatus.CLOSED))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].id, ticket.id)

        with self.db.transaction():
            updated = self.codec_repo.update_where(
                {"payload": {"priority": 1}},
                where=C.eq("status", PgCodecStatus.CLOSED),
            )
        self.assertEqual(updated, 1)
        refreshed = self.codec_repo.get(ticket.id)
        self.assertEqual(refreshed.payload, {"priority": 1})
        self.assertEqual(refreshed.tags, ["bug", "urgent"])


if __name__ == "__main__":
    unittest.main()
