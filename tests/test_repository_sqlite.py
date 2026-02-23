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
class OnlyPkRow:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})


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

    def test_list_with_grouped_conditions(self) -> None:
        self._seed_users()
        rows = self.repo.list(
            where=C.or_(
                C.eq("email", "alice@example.com"),
                C.eq("email", "bob@example.com"),
            ),
            order_by=[OrderBy("id")],
        )
        self.assertEqual([row.email for row in rows], ["alice@example.com", "bob@example.com"])

    def test_list_rejects_invalid_limit_offset(self) -> None:
        with self.assertRaises(ValueError):
            self.repo.list(limit=0)
        with self.assertRaises(ValueError):
            self.repo.list(offset=-1)

    def test_count_and_exists(self) -> None:
        self._seed_users()
        self.assertEqual(self.repo.count(), 4)
        self.assertEqual(self.repo.count(where=C.like("email", "%@example.com")), 3)
        self.assertTrue(self.repo.exists(where=C.eq("email", "alice@example.com")))
        self.assertFalse(self.repo.exists(where=C.eq("email", "nobody@example.com")))

    def test_insert_many(self) -> None:
        inserted = self.repo.insert_many(
            [
                UserRow(email="m1@example.com", age=10),
                UserRow(email="m2@example.com", age=20),
            ]
        )
        self.assertEqual(len(inserted), 2)
        self.assertEqual(self.repo.count(), 2)
        self.assertTrue(all(item.id is not None for item in inserted))

    def test_update_where(self) -> None:
        self._seed_users()
        updated = self.repo.update_where(
            {"age": 40},
            where=C.or_(
                C.eq("email", "alice@example.com"),
                C.eq("email", "bob@example.com"),
            ),
        )
        rows = self.repo.list(
            where=C.in_("email", ["alice@example.com", "bob@example.com"]),
            order_by=[OrderBy("id")],
        )
        self.assertEqual(updated, 2)
        self.assertEqual([row.age for row in rows], [40, 40])

    def test_update_where_validations(self) -> None:
        self._seed_users()
        with self.assertRaises(ValueError):
            self.repo.update_where({}, where=C.eq("id", 1))
        with self.assertRaises(ValueError):
            self.repo.update_where({"age": 20}, where=None)
        with self.assertRaises(ValueError):
            self.repo.update_where({"unknown": 1}, where=C.eq("id", 1))
        with self.assertRaises(ValueError):
            self.repo.update_where({"id": 123}, where=C.eq("id", 1))

    def test_delete_where(self) -> None:
        self._seed_users()
        deleted = self.repo.delete_where(
            where=C.like("email", "%@example.com"),
        )
        self.assertEqual(deleted, 3)
        self.assertEqual(self.repo.count(), 1)

    def test_delete_where_requires_where(self) -> None:
        with self.assertRaises(ValueError):
            self.repo.delete_where(where=None)

    def test_get_or_create(self) -> None:
        first, created_first = self.repo.get_or_create(
            lookup={"email": "new@example.com"},
            defaults={"age": 33},
        )
        second, created_second = self.repo.get_or_create(
            lookup={"email": "new@example.com"},
            defaults={"age": 99},
        )
        self.assertTrue(created_first)
        self.assertFalse(created_second)
        self.assertEqual(first.email, "new@example.com")
        self.assertEqual(first.age, 33)
        self.assertEqual(second.id, first.id)

    def test_get_or_create_requires_lookup(self) -> None:
        with self.assertRaises(ValueError):
            self.repo.get_or_create(lookup={})

    def test_repository_requires_dataclass_and_single_pk(self) -> None:
        with self.assertRaises(TypeError):
            Repository(self.db, PlainModel)
        with self.assertRaises(ValueError):
            Repository(self.db, MultiPkRow)

    def test_insert_with_only_auto_pk_model_uses_default_values(self) -> None:
        conn = sqlite3.connect(":memory:")
        db = Database(conn, SQLiteDialect())
        apply_schema(db, OnlyPkRow)
        repo = Repository[OnlyPkRow](db, OnlyPkRow)

        obj = repo.insert(OnlyPkRow())
        self.assertIsNotNone(obj.id)
        conn.close()

    def test_update_with_only_auto_pk_model_raises_clear_error(self) -> None:
        conn = sqlite3.connect(":memory:")
        db = Database(conn, SQLiteDialect())
        apply_schema(db, OnlyPkRow)
        repo = Repository[OnlyPkRow](db, OnlyPkRow)

        db.execute('INSERT INTO "onlypkrow" DEFAULT VALUES;')
        obj = repo.get(1)
        self.assertIsNotNone(obj)
        with self.assertRaises(ValueError):
            repo.update(obj)
        conn.close()


if __name__ == "__main__":
    unittest.main()
