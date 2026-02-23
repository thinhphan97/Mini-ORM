from __future__ import annotations

import sqlite3
import unittest
from dataclasses import dataclass, field
from typing import Optional

from mini_orm import C, Database, OrderBy, Repository, SQLiteDialect, apply_schema
from mini_orm.ports.db_api.dialects import Dialect


@dataclass
class UserRow:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = ""
    age: Optional[int] = None


@dataclass
class MultiPkRow:
    id1: int = field(default=0, metadata={"pk": True})
    id2: int = field(default=0, metadata={"pk": True})


class PlainModel:
    pass


class _NoReturningDialect(Dialect):
    paramstyle = "named"
    supports_returning = False
    quote_char = '"'


class _NoReturningDb:
    def __init__(self) -> None:
        self.dialect = _NoReturningDialect()
        self.executed: list[tuple[str, object]] = []

    class _Cursor:
        rowcount = 1
        lastrowid = 77

    def execute(self, sql, params=None):  # noqa: ANN001,ANN201
        self.executed.append((sql, params))
        return self._Cursor()

    def fetchone(self, sql, params=None):  # noqa: ANN001,ANN201
        return None

    def fetchall(self, sql, params=None):  # noqa: ANN001,ANN201
        return []

    def transaction(self):  # pragma: no cover
        raise NotImplementedError


class RepositorySQLiteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.db = Database(self.conn, SQLiteDialect())
        apply_schema(self.db, UserRow)
        self.repo = Repository[UserRow](self.db, UserRow)

    def tearDown(self) -> None:
        self.conn.close()

    def _seed_users(self) -> list[UserRow]:
        users = [
            UserRow(email="alice@example.com", age=25),
            UserRow(email="bob@example.com", age=30),
            UserRow(email="charlie@sample.com", age=35),
            UserRow(email="david@example.com", age=None),
        ]
        with self.db.transaction():
            for user in users:
                self.repo.insert(user)
        return users

    def test_insert_assigns_auto_pk(self) -> None:
        user = UserRow(email="new@example.com", age=20)
        inserted = self.repo.insert(user)
        self.assertIsNotNone(inserted.id)
        self.assertEqual(inserted.email, "new@example.com")

    def test_insert_uses_lastrowid_when_returning_is_not_supported(self) -> None:
        fake_db = _NoReturningDb()
        repo = Repository[UserRow](fake_db, UserRow)
        user = UserRow(email="fallback@example.com", age=10)
        repo.insert(user)
        self.assertEqual(user.id, 77)
        self.assertTrue(fake_db.executed)

    def test_get_and_get_missing(self) -> None:
        inserted = self.repo.insert(UserRow(email="lookup@example.com", age=50))
        found = self.repo.get(inserted.id)
        missing = self.repo.get(9999)

        self.assertEqual(found.email, "lookup@example.com")
        self.assertIsNone(missing)

    def test_update_and_delete(self) -> None:
        inserted = self.repo.insert(UserRow(email="mutate@example.com", age=10))
        inserted.age = 11
        update_count = self.repo.update(inserted)
        updated = self.repo.get(inserted.id)

        delete_count = self.repo.delete(inserted)
        missing = self.repo.get(inserted.id)

        self.assertEqual(update_count, 1)
        self.assertEqual(updated.age, 11)
        self.assertEqual(delete_count, 1)
        self.assertIsNone(missing)

    def test_update_and_delete_require_pk(self) -> None:
        missing_pk = UserRow(email="x", age=1)
        with self.assertRaises(ValueError):
            self.repo.update(missing_pk)
        with self.assertRaises(ValueError):
            self.repo.delete(missing_pk)

    def test_list_with_condition_order_limit_offset(self) -> None:
        self._seed_users()
        rows = self.repo.list(
            where=[C.like("email", "%@example.com"), C.is_not_null("age")],
            order_by=[OrderBy("age", desc=True)],
            limit=1,
            offset=1,
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].email, "alice@example.com")

    def test_list_with_in_and_is_null(self) -> None:
        users = self._seed_users()
        selected = self.repo.list(
            where=C.in_("id", [users[0].id, users[1].id]),
            order_by=[OrderBy("id")],
        )
        null_age = self.repo.list(where=C.is_null("age"))

        self.assertEqual([row.email for row in selected], ["alice@example.com", "bob@example.com"])
        self.assertEqual(len(null_age), 1)
        self.assertEqual(null_age[0].email, "david@example.com")

    def test_repository_requires_dataclass_and_single_pk(self) -> None:
        with self.assertRaises(TypeError):
            Repository(self.db, PlainModel)
        with self.assertRaises(ValueError):
            Repository(self.db, MultiPkRow)


if __name__ == "__main__":
    unittest.main()
