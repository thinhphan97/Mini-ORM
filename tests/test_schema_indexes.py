from __future__ import annotations

import sqlite3
import unittest
from datetime import date, datetime, time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

from mini_orm import (
    Database,
    SQLiteDialect,
    apply_schema,
    create_index_sql,
    create_indexes_sql,
    create_schema_sql,
    create_table_sql,
)


@dataclass
class IndexedUser:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = field(default="", metadata={"index": True, "index_name": "idx_email"})
    age: Optional[int] = field(default=None, metadata={"index": True})
    score: Optional[float] = None

    __indexes__ = [
        {"columns": ("email", "age"), "name": "idx_email_age"},
        {"columns": ("score",), "unique": True},
    ]


@dataclass
class DuplicateIndexModel:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = field(default="", metadata={"index": True, "index_name": "idx_email"})
    __indexes__ = [
        {"columns": ("email",), "name": "idx_email"},
        "email",
    ]


@dataclass
class InvalidIndexColumnModel:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = ""
    __indexes__ = [{"columns": ("missing_col",)}]


@dataclass
class InvalidIndexTypeModel:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = ""
    __indexes__ = [123]


@dataclass
class InvalidIndexColumnsModel:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = ""
    __indexes__ = [{"columns": []}]


@dataclass
class TypedColumnsModel:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    age: Optional[int] = None
    score: Optional[float] = None
    name: str = ""


@dataclass
class RichTypedColumnsModel:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    enabled: bool = False
    created_at: Optional[datetime] = None
    birthday: Optional[date] = None
    wakeup_at: Optional[time] = None
    amount: Optional[Decimal] = None
    raw: Optional[bytes] = None


class SchemaIndexTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dialect = SQLiteDialect()

    def test_create_index_sql_allows_any_existing_column(self) -> None:
        sql = create_index_sql(IndexedUser, self.dialect, "score", unique=True)
        self.assertIn('CREATE UNIQUE INDEX "uidx_indexeduser_score"', sql)
        self.assertIn('ON "indexeduser" ("score")', sql)

    def test_create_index_sql_rejects_unknown_column(self) -> None:
        with self.assertRaises(ValueError):
            create_index_sql(IndexedUser, self.dialect, "unknown_col")

    def test_create_indexes_sql_from_metadata_and_model_indexes(self) -> None:
        sql_list = create_indexes_sql(IndexedUser, self.dialect)

        self.assertEqual(len(sql_list), 4)
        self.assertTrue(
            any('CREATE INDEX "idx_email" ON "indexeduser" ("email")' in sql for sql in sql_list)
        )
        self.assertTrue(
            any('CREATE INDEX "idx_indexeduser_age" ON "indexeduser" ("age")' in sql for sql in sql_list)
        )
        self.assertTrue(
            any('CREATE INDEX "idx_email_age" ON "indexeduser" ("email", "age")' in sql for sql in sql_list)
        )
        self.assertTrue(
            any(
                'CREATE UNIQUE INDEX "uidx_indexeduser_score" ON "indexeduser" ("score")'
                in sql
                for sql in sql_list
            )
        )

    def test_create_schema_sql_includes_table_first_then_indexes(self) -> None:
        sql_list = create_schema_sql(IndexedUser, self.dialect)
        self.assertEqual(len(sql_list), 5)
        self.assertTrue(sql_list[0].startswith('CREATE TABLE "indexeduser"'))
        for sql in sql_list[1:]:
            self.assertTrue(sql.startswith("CREATE "))
            self.assertIn(" INDEX ", sql)

    def test_create_indexes_sql_deduplicates_equal_specs(self) -> None:
        sql_list = create_indexes_sql(DuplicateIndexModel, self.dialect)
        self.assertEqual(
            len([sql for sql in sql_list if '"idx_email"' in sql]),
            1,
        )

    def test_create_indexes_sql_rejects_invalid_indexes(self) -> None:
        with self.assertRaises(ValueError):
            create_indexes_sql(InvalidIndexColumnModel, self.dialect)
        with self.assertRaises(TypeError):
            create_indexes_sql(InvalidIndexTypeModel, self.dialect)
        with self.assertRaises(ValueError):
            create_indexes_sql(InvalidIndexColumnsModel, self.dialect)

    def test_sqlite_creates_expected_indexes(self) -> None:
        conn = sqlite3.connect(":memory:")
        cursor = conn.cursor()

        cursor.execute(create_table_sql(IndexedUser, self.dialect))
        for sql in create_indexes_sql(IndexedUser, self.dialect):
            cursor.execute(sql)

        rows = cursor.execute("PRAGMA index_list('indexeduser');").fetchall()
        index_names = {row[1] for row in rows}

        self.assertIn("idx_email", index_names)
        self.assertIn("idx_indexeduser_age", index_names)
        self.assertIn("idx_email_age", index_names)
        self.assertIn("uidx_indexeduser_score", index_names)

        conn.close()

    def test_create_table_sql_maps_types(self) -> None:
        sql = create_table_sql(TypedColumnsModel, self.dialect)
        self.assertIn('"age" INTEGER', sql)
        self.assertIn('"score" REAL', sql)
        self.assertIn('"name" TEXT', sql)

    def test_create_table_sql_maps_rich_types(self) -> None:
        sql = create_table_sql(RichTypedColumnsModel, self.dialect)
        self.assertIn('"enabled" BOOLEAN', sql)
        self.assertIn('"created_at" TIMESTAMP', sql)
        self.assertIn('"birthday" DATE', sql)
        self.assertIn('"wakeup_at" TIME', sql)
        self.assertIn('"amount" NUMERIC', sql)
        self.assertIn('"raw" BLOB', sql)

    def test_create_schema_sql_if_not_exists(self) -> None:
        sql_list = create_schema_sql(IndexedUser, self.dialect, if_not_exists=True)
        self.assertTrue(sql_list[0].startswith('CREATE TABLE IF NOT EXISTS "indexeduser"'))
        self.assertTrue(any("CREATE INDEX IF NOT EXISTS" in sql for sql in sql_list[1:]))
        self.assertTrue(any("CREATE UNIQUE INDEX IF NOT EXISTS" in sql for sql in sql_list[1:]))

    def test_apply_schema_creates_table_and_indexes(self) -> None:
        conn = sqlite3.connect(":memory:")
        db = Database(conn, self.dialect)

        statements = apply_schema(db, IndexedUser)
        self.assertEqual(len(statements), 5)

        cursor = conn.cursor()
        table_rows = cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='indexeduser';"
        ).fetchall()
        index_rows = cursor.execute("PRAGMA index_list('indexeduser');").fetchall()
        index_names = {row[1] for row in index_rows}

        self.assertEqual(table_rows[0][0], "indexeduser")
        self.assertIn("idx_email", index_names)
        self.assertIn("idx_indexeduser_age", index_names)
        self.assertIn("idx_email_age", index_names)
        self.assertIn("uidx_indexeduser_score", index_names)

        conn.close()

    def test_apply_schema_if_not_exists_is_idempotent(self) -> None:
        conn = sqlite3.connect(":memory:")
        db = Database(conn, self.dialect)

        apply_schema(db, IndexedUser, if_not_exists=True)
        apply_schema(db, IndexedUser, if_not_exists=True)

        index_rows = conn.cursor().execute("PRAGMA index_list('indexeduser');").fetchall()
        index_names = {row[1] for row in index_rows}
        self.assertIn("idx_email", index_names)
        self.assertIn("idx_indexeduser_age", index_names)
        self.assertIn("idx_email_age", index_names)
        self.assertIn("uidx_indexeduser_score", index_names)

        conn.close()


if __name__ == "__main__":
    unittest.main()
