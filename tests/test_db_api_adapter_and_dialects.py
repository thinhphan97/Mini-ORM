from __future__ import annotations

import sqlite3
import unittest

from mini_orm.ports.db_api.database import Database
from mini_orm.ports.db_api.dialects import Dialect, MySQLDialect, PostgresDialect, SQLiteDialect


class _DummyCursor:
    def __init__(self, description=None, lastrowid=None):
        self.description = description
        self.lastrowid = lastrowid


class _QmarkDialect(Dialect):
    paramstyle = "qmark"


class _InvalidDialect(Dialect):
    paramstyle = "invalid"


class DialectTests(unittest.TestCase):
    def test_builtin_dialect_properties(self) -> None:
        self.assertEqual(SQLiteDialect().placeholder("x"), ":x")
        self.assertEqual(PostgresDialect().placeholder("x"), "%s")
        self.assertEqual(MySQLDialect().placeholder("x"), "%s")
        self.assertEqual(_QmarkDialect().placeholder("x"), "?")

    def test_invalid_paramstyle_raises(self) -> None:
        with self.assertRaises(ValueError):
            _InvalidDialect().placeholder("x")

    def test_returning_clause_and_lastrowid(self) -> None:
        sqlite_dialect = SQLiteDialect()
        mysql_dialect = MySQLDialect()

        self.assertEqual(sqlite_dialect.returning_clause("id"), ' RETURNING "id"')
        self.assertEqual(mysql_dialect.returning_clause("id"), "")

        cursor = _DummyCursor(lastrowid=99)
        self.assertEqual(sqlite_dialect.get_lastrowid(cursor), 99)


class DatabaseAdapterTests(unittest.TestCase):
    def test_execute_fetchone_fetchall(self) -> None:
        conn = sqlite3.connect(":memory:")
        db = Database(conn, SQLiteDialect())
        db.execute('CREATE TABLE "t" ("id" INTEGER, "name" TEXT);')
        db.execute('INSERT INTO "t" ("id", "name") VALUES (:id, :name);', {"id": 1, "name": "a"})
        db.execute('INSERT INTO "t" ("id", "name") VALUES (:id, :name);', {"id": 2, "name": "b"})

        row = db.fetchone('SELECT * FROM "t" WHERE "id" = :id;', {"id": 1})
        rows = db.fetchall('SELECT * FROM "t" ORDER BY "id" ASC;')

        self.assertEqual(row["name"], "a")
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[1]["name"], "b")
        conn.close()

    def test_row_factory_mapping_is_supported(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        db = Database(conn, SQLiteDialect())
        db.execute('CREATE TABLE "t" ("id" INTEGER);')
        db.execute('INSERT INTO "t" ("id") VALUES (1);')
        row = db.fetchone('SELECT * FROM "t";')
        self.assertEqual(row["id"], 1)
        conn.close()

    def test_transaction_rolls_back_on_error(self) -> None:
        conn = sqlite3.connect(":memory:")
        db = Database(conn, SQLiteDialect())
        db.execute('CREATE TABLE "t" ("id" INTEGER);')

        with self.assertRaises(RuntimeError):
            with db.transaction():
                db.execute('INSERT INTO "t" ("id") VALUES (1);')
                raise RuntimeError("boom")

        count = db.fetchone('SELECT COUNT(*) AS "count" FROM "t";')
        self.assertEqual(count["count"], 0)
        conn.close()

    def test_row_to_mapping_tuple_without_description_raises(self) -> None:
        conn = sqlite3.connect(":memory:")
        db = Database(conn, SQLiteDialect())
        with self.assertRaises(TypeError):
            db._row_to_mapping(_DummyCursor(description=None), (1,))  # noqa: SLF001
        conn.close()

    def test_row_to_mapping_fallback_dict_and_unsupported_type(self) -> None:
        conn = sqlite3.connect(":memory:")
        db = Database(conn, SQLiteDialect())

        mapped = db._row_to_mapping(_DummyCursor(), {("id", 1)})  # noqa: SLF001
        self.assertEqual(mapped["id"], 1)

        with self.assertRaises(TypeError):
            db._row_to_mapping(_DummyCursor(), 12345)  # noqa: SLF001
        conn.close()


if __name__ == "__main__":
    unittest.main()
