from __future__ import annotations

import sqlite3
import unittest
from datetime import date, datetime, time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

from mini_orm import (
    Database,
    MySQLDialect,
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


@dataclass
class ParentModel:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    title: str = ""


@dataclass
class ChildModel:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    parent_id: Optional[int] = field(default=None, metadata={"fk": (ParentModel, "id")})
    name: str = ""


@dataclass
class ChildModelFkString:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    parent_id: Optional[int] = field(default=None, metadata={"fk": "parentmodel.id"})


@dataclass
class ChildModelFkMappingWithModel:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    parent_id: Optional[int] = field(
        default=None, metadata={"fk": {"model": ParentModel, "column": "id"}}
    )


@dataclass
class ChildModelFkMappingWithTable:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    parent_id: Optional[int] = field(
        default=None, metadata={"fk": {"table": "parentmodel", "column": "id"}}
    )


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

    def test_create_schema_sql_if_not_exists_omits_clause_for_mysql_indexes(self) -> None:
        mysql_sql = create_schema_sql(IndexedUser, MySQLDialect(), if_not_exists=True)
        self.assertTrue(mysql_sql[0].startswith("CREATE TABLE IF NOT EXISTS"))
        for sql in mysql_sql[1:]:
            self.assertNotIn("IF NOT EXISTS", sql)

    def test_create_table_sql_supports_foreign_key_metadata(self) -> None:
        sql = create_table_sql(ChildModel, self.dialect)
        self.assertIn('"parent_id" INTEGER NULL REFERENCES "parentmodel" ("id")', sql)

    def test_create_table_sql_supports_foreign_key_string_and_mapping_formats(self) -> None:
        sql_string = create_table_sql(ChildModelFkString, self.dialect)
        sql_mapping_model = create_table_sql(ChildModelFkMappingWithModel, self.dialect)
        sql_mapping_table = create_table_sql(ChildModelFkMappingWithTable, self.dialect)

        self.assertIn('REFERENCES "parentmodel" ("id")', sql_string)
        self.assertIn('REFERENCES "parentmodel" ("id")', sql_mapping_model)
        self.assertIn('REFERENCES "parentmodel" ("id")', sql_mapping_table)

    def test_create_table_sql_rejects_invalid_fk_string(self) -> None:
        @dataclass
        class InvalidFkStringModel:
            id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
            parent_id: Optional[int] = field(default=None, metadata={"fk": "invalid_fk"})

        with self.assertRaises(ValueError):
            create_table_sql(InvalidFkStringModel, self.dialect)

    def test_create_table_sql_rejects_invalid_fk_sequence(self) -> None:
        @dataclass
        class InvalidFkSequenceModel:
            id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
            parent_id: Optional[int] = field(default=None, metadata={"fk": (ParentModel,)})

        with self.assertRaises(ValueError):
            create_table_sql(InvalidFkSequenceModel, self.dialect)

    def test_create_table_sql_rejects_invalid_fk_mapping_column(self) -> None:
        @dataclass
        class InvalidFkColumnModel:
            id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
            parent_id: Optional[int] = field(
                default=None,
                metadata={"fk": {"table": "parentmodel", "column": 123}},
            )

        with self.assertRaises(TypeError):
            create_table_sql(InvalidFkColumnModel, self.dialect)

    def test_create_table_sql_rejects_fk_model_that_is_not_dataclass(self) -> None:
        class Plain:
            pass

        @dataclass
        class InvalidFkModelRef:
            id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
            parent_id: Optional[int] = field(default=None, metadata={"fk": (Plain, "id")})

        with self.assertRaises(TypeError):
            create_table_sql(InvalidFkModelRef, self.dialect)

    def test_sqlite_enforces_foreign_key_when_clause_exists(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.execute("PRAGMA foreign_keys = ON;")
        cursor = conn.cursor()
        cursor.execute(create_table_sql(ParentModel, self.dialect))
        cursor.execute(create_table_sql(ChildModel, self.dialect))

        with self.assertRaises(sqlite3.IntegrityError):
            cursor.execute('INSERT INTO "childmodel" ("parent_id", "name") VALUES (999, "x");')

        cursor.execute('INSERT INTO "parentmodel" ("title") VALUES ("p1");')
        cursor.execute('INSERT INTO "childmodel" ("parent_id", "name") VALUES (1, "ok");')
        conn.close()

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
