from __future__ import annotations

import importlib
import os
import unittest
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from mini_orm import C, Database, MySQLDialect, OrderBy, Repository, apply_schema
from tests._codec_roundtrip_mixin import CodecRoundtripMixin


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
class MySQLDialectUser:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = ""
    age: Optional[int] = None


@dataclass
class MySQLDialectAuthor:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    name: str = ""


@dataclass
class MySQLDialectPost:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    author_id: Optional[int] = field(default=None, metadata={"fk": (MySQLDialectAuthor, "id")})
    title: str = ""


class MySQLCodecStatus(str, Enum):
    OPEN = "open"
    CLOSED = "closed"


@dataclass
class MySQLCodecTicket:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    status: MySQLCodecStatus = MySQLCodecStatus.OPEN
    payload: dict[str, Any] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)


MySQLDialectAuthor.__relations__ = {
    "posts": {
        "model": MySQLDialectPost,
        "local_key": "id",
        "remote_key": "author_id",
        "type": "has_many",
    }
}

MySQLDialectPost.__relations__ = {
    "author": {
        "model": MySQLDialectAuthor,
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
class RepositoryMySQLDialectTests(CodecRoundtripMixin, unittest.TestCase):
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
                "MySQL is not reachable at localhost:3306 "
                f"with configured credentials: {exc}"
            ) from exc

        cls.db = Database(cls.conn, MySQLDialect())
        cls.repo = Repository[MySQLDialectUser](cls.db, MySQLDialectUser)
        cls.author_repo = Repository[MySQLDialectAuthor](cls.db, MySQLDialectAuthor)
        cls.post_repo = Repository[MySQLDialectPost](cls.db, MySQLDialectPost)
        cls.codec_repo = Repository[MySQLCodecTicket](cls.db, MySQLCodecTicket)
        cls.codec_ticket_cls = MySQLCodecTicket
        cls.codec_closed_status = MySQLCodecStatus.CLOSED

    @classmethod
    def tearDownClass(cls) -> None:
        conn = getattr(cls, "conn", None)
        if conn is not None:
            conn.close()

    def setUp(self) -> None:
        with self.db.transaction():
            self.db.execute("DROP TABLE IF EXISTS `mysqlcodecticket`;")
            self.db.execute("DROP TABLE IF EXISTS `mysqldialectpost`;")
            self.db.execute("DROP TABLE IF EXISTS `mysqldialectauthor`;")
            self.db.execute("DROP TABLE IF EXISTS `mysqldialectuser`;")
        apply_schema(self.db, MySQLDialectUser)
        apply_schema(self.db, MySQLDialectAuthor)
        apply_schema(self.db, MySQLDialectPost)
        apply_schema(self.db, MySQLCodecTicket)

    def test_insert_update_delete_roundtrip(self) -> None:
        with self.db.transaction():
            user = self.repo.insert(MySQLDialectUser(email="alice@example.com", age=30))

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
                    MySQLDialectUser(email="alice@example.com", age=25),
                    MySQLDialectUser(email="bob@example.com", age=30),
                    MySQLDialectUser(email="charlie@sample.com", age=35),
                    MySQLDialectUser(email="david@example.com", age=None),
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
        author = MySQLDialectAuthor(name="Alice")
        posts = [MySQLDialectPost(title="P1"), MySQLDialectPost(title="P2")]

        self.author_repo.create(author, relations={"posts": posts})

        self.assertIsNotNone(author.id)
        self.assertEqual(self.post_repo.count(), 2)
        loaded_posts = self.post_repo.list(order_by=[OrderBy("id")])
        self.assertTrue(all(post.author_id == author.id for post in loaded_posts))

    def test_get_related_and_list_related(self) -> None:
        author = self.author_repo.create(
            MySQLDialectAuthor(name="Reader"),
            relations={"posts": [MySQLDialectPost(title="T1"), MySQLDialectPost(title="T2")]},
        )

        author_with_posts = self.author_repo.get_related(author.id, include=["posts"])
        self.assertIsNotNone(author_with_posts)
        self.assertEqual(len(author_with_posts.relations["posts"]), 2)

        posts_with_author = self.post_repo.list_related(include=["author"], order_by=[OrderBy("id")])
        self.assertEqual(len(posts_with_author), 2)
        self.assertTrue(all(item.relations["author"] is not None for item in posts_with_author))
        self.assertTrue(all(item.relations["author"].name == "Reader" for item in posts_with_author))

if __name__ == "__main__":
    unittest.main()
