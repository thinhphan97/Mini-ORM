from __future__ import annotations

import importlib
import json
import math
import os
import unittest
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Optional
from uuid import uuid4

from mini_orm import (
    Database,
    PgVectorStore,
    PostgresDialect,
    Repository,
    VectorMetric,
    VectorRecord,
    VectorRepository,
)


def _load_pg_connect() -> Any | None:
    for module_name in ("psycopg", "psycopg2"):
        try:
            module = importlib.import_module(module_name)
        except (ModuleNotFoundError, ImportError):
            continue
        connect = getattr(module, "connect", None)
        if connect is not None:
            return connect
    return None


RUN_HOST_VECTOR_TESTS = os.getenv("MINI_ORM_VECTOR_HOST_TESTS", "").lower() in {
    "1",
    "true",
    "yes",
}


class _FakePostgresDialect:
    name = "postgres"
    paramstyle = "format"
    supports_returning = True

    @staticmethod
    def placeholder(key: str) -> str:  # noqa: ARG004 - key ignored for format style
        return "%s"


class _FakePostgresDB:
    def __init__(self) -> None:
        self.dialect = _FakePostgresDialect()
        self.extension_created = False
        self._tables: dict[tuple[str, str], dict[str, object]] = {}

    @contextmanager
    def transaction(self):
        yield

    def execute(self, sql, params=None):
        lowered = " ".join(sql.strip().split()).lower()
        if lowered.startswith("create extension if not exists vector"):
            self.extension_created = True
            return None
        if lowered.startswith("drop table"):
            schema, table = self._parse_table_after(sql, "DROP TABLE")
            self._tables.pop((schema, table), None)
            return None
        if lowered.startswith("create table"):
            schema, table = self._parse_table_after(sql, "CREATE TABLE")
            dim_start = lowered.find("vector(")
            dim_end = lowered.find(")", dim_start)
            dimension = int(lowered[dim_start + 7 : dim_end])
            self._tables[(schema, table)] = {
                "dimension": dimension,
                "rows": {},
            }
            return None
        if lowered.startswith("insert into"):
            schema, table = self._parse_table_after(sql, "INSERT INTO")
            id_value, vector_literal, payload_json = tuple(params or ())
            table_state = self._table_state(schema, table)
            payload = json.loads(payload_json) if payload_json is not None else None
            table_state["rows"][str(id_value)] = {
                "id": str(id_value),
                "vector": self._parse_vector(vector_literal),
                "payload": payload,
            }
            return None
        raise AssertionError(f"Unexpected execute SQL in fake DB: {sql}")

    def fetchone(self, sql, params=None):
        lowered = " ".join(sql.strip().split()).lower()
        if "from information_schema.tables" in lowered:
            schema, table = self._table_lookup_from_params(params or ())
            if (schema, table) in self._tables:
                return {"exists_flag": 1}
            return None
        if "from pg_attribute a" in lowered and "format_type" in lowered:
            schema, table = self._table_lookup_from_params(params or ())
            table_state = self._tables.get((schema, table))
            if table_state is None:
                return None
            return {"data_type": f"vector({table_state['dimension']})"}
        raise AssertionError(f"Unexpected fetchone SQL in fake DB: {sql}")

    def fetchall(self, sql, params=None):
        lowered = " ".join(sql.strip().split()).lower()
        params_tuple = tuple(params or ())
        if lowered.startswith('select "id", "payload",') and "order by __distance asc" in lowered:
            schema, table = self._parse_table_after(sql, "FROM")
            table_state = self._table_state(schema, table)
            query_vector = self._parse_vector(params_tuple[0])
            limit = int(params_tuple[-1])
            filter_objects = [json.loads(item) for item in params_tuple[1:-1]]
            metric = "<=>" if "<=>" in sql else "<#>" if "<#>" in sql else "<->"
            rows = []
            for record in table_state["rows"].values():
                payload = record["payload"]
                if not self._payload_matches(payload, filter_objects):
                    continue
                distance = self._distance(metric, query_vector, record["vector"])
                rows.append(
                    {
                        "id": record["id"],
                        "payload": payload,
                        "__distance": distance,
                    }
                )
            rows.sort(key=lambda item: item["__distance"])
            return rows[:limit]

        if lowered.startswith('select "id", "embedding"::text as __vector_text, "payload"'):
            schema, table = self._parse_table_after(sql, "FROM")
            table_state = self._table_state(schema, table)
            rows_by_id = table_state["rows"]
            if 'where "id" in' in lowered:
                return [
                    self._record_to_row(rows_by_id[str(item_id)])
                    for item_id in params_tuple
                    if str(item_id) in rows_by_id
                ]
            return [
                self._record_to_row(rows_by_id[item_id])
                for item_id in sorted(rows_by_id.keys())
            ]

        if lowered.startswith("delete from") and 'returning "id"' in lowered:
            schema, table = self._parse_table_after(sql, "DELETE FROM")
            table_state = self._table_state(schema, table)
            deleted = []
            for item_id in params_tuple:
                sid = str(item_id)
                if sid in table_state["rows"]:
                    del table_state["rows"][sid]
                    deleted.append({"id": sid})
            return deleted

        raise AssertionError(f"Unexpected fetchall SQL in fake DB: {sql}")

    def _table_lookup_from_params(self, params: tuple[object, ...]) -> tuple[str, str]:
        if len(params) == 1:
            return "public", str(params[0])
        if len(params) == 2:
            return str(params[0]), str(params[1])
        raise AssertionError(f"Unexpected table lookup params: {params}")

    @staticmethod
    def _unquote_identifier(identifier: str) -> str:
        cleaned = identifier.strip()
        if not (cleaned.startswith('"') and cleaned.endswith('"')):
            raise AssertionError(f"Expected quoted identifier, got: {identifier}")
        return cleaned[1:-1].replace('""', '"')

    def _parse_table_after(self, sql: str, keyword: str) -> tuple[str, str]:
        after = sql.split(keyword, 1)[1].strip()
        qualified = after.split()[0].strip().rstrip(";")
        parts = [part.strip() for part in qualified.split(".")]
        if len(parts) == 1:
            return "public", self._unquote_identifier(parts[0])
        if len(parts) == 2:
            return self._unquote_identifier(parts[0]), self._unquote_identifier(parts[1])
        raise AssertionError(f"Unexpected qualified table expression: {qualified}")

    def _table_state(self, schema: str, table: str) -> dict[str, object]:
        state = self._tables.get((schema, table))
        if state is None:
            raise KeyError(f"Collection does not exist: {schema}.{table}")
        return state

    @staticmethod
    def _parse_vector(vector_literal: str) -> list[float]:
        body = vector_literal.strip()[1:-1].strip()
        if not body:
            return []
        return [float(item.strip()) for item in body.split(",")]

    @staticmethod
    def _record_to_row(record: dict[str, object]) -> dict[str, object]:
        vector = record["vector"]
        vector_text = "[" + ",".join(str(item) for item in vector) + "]"
        return {
            "id": record["id"],
            "__vector_text": vector_text,
            "payload": record["payload"],
        }

    @staticmethod
    def _payload_matches(payload: object, filter_objects: list[dict[str, object]]) -> bool:
        if not filter_objects:
            return True
        if not isinstance(payload, dict):
            return False
        return all(
            payload.get(next(iter(item.keys()))) == next(iter(item.values()))
            for item in filter_objects
        )

    @staticmethod
    def _distance(metric: str, left, right) -> float:
        if metric == "<#>":
            return -sum(a * b for a, b in zip(left, right))
        if metric == "<->":
            return math.sqrt(sum((a - b) ** 2 for a, b in zip(left, right)))

        dot = sum(a * b for a, b in zip(left, right))
        norm_left = math.sqrt(sum(a * a for a in left))
        norm_right = math.sqrt(sum(b * b for b in right))
        if norm_left == 0.0 or norm_right == 0.0:
            return 1.0
        return 1.0 - (dot / (norm_left * norm_right))


class PgVectorStoreTests(unittest.TestCase):
    def test_pgvector_end_to_end_with_filters(self) -> None:
        db = _FakePostgresDB()
        store = PgVectorStore(db)
        store.create_collection("items", dimension=3, metric=VectorMetric.COSINE)

        store.upsert(
            "items",
            [
                VectorRecord("u1", [1, 0, 0], {"group": "a"}),
                VectorRecord("u2", [0, 1, 0], {"group": "b"}),
                VectorRecord("u3", [0.9, 0.1, 0], {"group": "a"}),
            ],
        )

        fetched = store.fetch("items", ids=["u2", "u1"])
        hits = store.query("items", [1, 0, 0], top_k=2)
        filtered = store.query("items", [1, 0, 0], top_k=5, filters={"group": "a"})
        deleted = store.delete("items", ["u2", "missing"])
        remaining = store.fetch("items")

        self.assertTrue(db.extension_created)
        self.assertEqual([item.id for item in fetched], ["u2", "u1"])
        self.assertEqual([hit.id for hit in hits], ["u1", "u3"])
        self.assertEqual([hit.id for hit in filtered], ["u1", "u3"])
        self.assertEqual(deleted, 1)
        self.assertEqual([item.id for item in remaining], ["u1", "u3"])

    def test_pgvector_metrics_dot_and_l2(self) -> None:
        metric_cases = [
            (
                VectorMetric.DOT,
                [
                    VectorRecord("a", [1, 0]),
                    VectorRecord("b", [2, 0]),
                    VectorRecord("c", [0, 1]),
                ],
                [1, 0],
                ["b", "a", "c"],
            ),
            (
                VectorMetric.L2,
                [
                    VectorRecord("x", [1, 0]),
                    VectorRecord("y", [2, 0]),
                    VectorRecord("z", [0, 1]),
                ],
                [1, 0],
                ["x", "y", "z"],
            ),
        ]

        for metric, records, query_vector, expected_order in metric_cases:
            db = _FakePostgresDB()
            store = PgVectorStore(db)
            store.create_collection("metric_items", dimension=2, metric=metric)
            store.upsert("metric_items", records)

            hits = store.query("metric_items", query_vector, top_k=3)
            self.assertEqual([hit.id for hit in hits], expected_order)

    def test_pgvector_dimension_validation_and_discovery(self) -> None:
        db = _FakePostgresDB()
        store = PgVectorStore(db)
        store.create_collection(
            "discovery_items", dimension=2, metric=VectorMetric.COSINE
        )
        store.upsert("discovery_items", [VectorRecord("a", [1, 0])])

        fresh_store = PgVectorStore(db, ensure_extension=False)
        fetched = fresh_store.fetch("discovery_items")
        self.assertEqual([item.id for item in fetched], ["a"])

        with self.assertRaises(ValueError):
            store.upsert("discovery_items", [VectorRecord("bad", [1, 0, 0])])

    def test_pgvector_requires_postgres_dialect(self) -> None:
        db = _FakePostgresDB()
        db.dialect.name = "sqlite"
        with self.assertRaises(ValueError):
            PgVectorStore(db)

    def test_pgvector_payload_must_be_json_serializable(self) -> None:
        class Unsupported:
            pass

        db = _FakePostgresDB()
        store = PgVectorStore(db)
        store.create_collection("payload_items", dimension=2)

        with self.assertRaisesRegex(TypeError, "JSON-serializable"):
            store.upsert(
                "payload_items", [VectorRecord("bad", [1, 0], {"obj": Unsupported()})]
            )


@unittest.skipUnless(
    RUN_HOST_VECTOR_TESTS,
    "Set MINI_ORM_VECTOR_HOST_TESTS=1 to run PostgreSQL host integration tests.",
)
class PgVectorSqlIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        connect = _load_pg_connect()
        if connect is None:
            raise unittest.SkipTest("psycopg/psycopg2 is not installed.")

        password = os.getenv(
            "MINI_ORM_PG_PASSWORD",
            os.getenv("PGPASSWORD", os.getenv("POSTGRES_PASSWORD", "password")),
        )
        params = {
            "host": os.getenv("MINI_ORM_PG_HOST", os.getenv("PGHOST", "localhost")),
            "port": int(os.getenv("MINI_ORM_PG_PORT", os.getenv("PGPORT", "5432"))),
            "user": os.getenv("MINI_ORM_PG_USER", os.getenv("PGUSER", "postgres")),
            "password": password,
            "dbname": os.getenv(
                "MINI_ORM_PG_DATABASE",
                os.getenv("PGDATABASE", "postgres"),
            ),
        }

        try:
            conn = connect(**params)
        except Exception as exc:
            raise unittest.SkipTest(
                f"PostgreSQL host is not reachable at {params['host']}:{params['port']}: {exc}"
            ) from exc

        cls.db = Database(conn, PostgresDialect())
        cls._suffix = uuid4().hex[:8]
        cls.vector_table = f"pgvector_docs_embedding_{cls._suffix}"
        sql_table = f"pgvector_docs_sql_{cls._suffix}"

        @dataclass
        class Document:
            __table__ = sql_table

            id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
            title: str = field(default="")
            category: str = field(default="")

        cls.Document = Document

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            cls._cleanup_tables()
        finally:
            cls.db.close()

    @classmethod
    def _cleanup_tables(cls) -> None:
        with cls.db.transaction():
            cls.db.execute(f'DROP TABLE IF EXISTS "{cls.vector_table}";')
            cls.db.execute(f'DROP TABLE IF EXISTS "{cls.Document.__table__}";')

    def setUp(self) -> None:
        self._cleanup_tables()
        self.sql_repo = Repository[self.Document](self.db, self.Document, auto_schema=True)
        self.vector_repo = VectorRepository(
            PgVectorStore(self.db),
            self.vector_table,
            dimension=3,
            metric=VectorMetric.COSINE,
            auto_create=True,
            overwrite=True,
        )

    def test_sql_repository_and_pgvector_share_same_database(self) -> None:
        doc1 = self.sql_repo.insert(self.Document(title="ORM intro", category="guide"))
        doc2 = self.sql_repo.insert(self.Document(title="API ref", category="docs"))
        doc3 = self.sql_repo.insert(self.Document(title="ORM advanced", category="guide"))

        self.vector_repo.upsert(
            [
                VectorRecord(str(doc1.id), [1.0, 0.0, 0.0], {"category": doc1.category}),
                VectorRecord(str(doc2.id), [0.0, 1.0, 0.0], {"category": doc2.category}),
                VectorRecord(str(doc3.id), [0.9, 0.1, 0.0], {"category": doc3.category}),
            ]
        )

        hits = self.vector_repo.query(
            [1.0, 0.0, 0.0],
            top_k=3,
            filters={"category": "guide"},
        )
        docs_by_id = {str(item.id): item for item in self.sql_repo.list()}

        self.assertEqual([item.id for item in hits], [str(doc1.id), str(doc3.id)])
        self.assertEqual(docs_by_id[hits[0].id].title, "ORM intro")
        self.assertEqual(docs_by_id[hits[1].id].title, "ORM advanced")

    def test_sql_rows_and_vector_rows_can_be_managed_independently(self) -> None:
        doc1 = self.sql_repo.insert(self.Document(title="alpha", category="guide"))
        doc2 = self.sql_repo.insert(self.Document(title="beta", category="docs"))

        self.vector_repo.upsert(
            [
                VectorRecord(str(doc1.id), [1.0, 0.0, 0.0], {"category": doc1.category}),
                VectorRecord(str(doc2.id), [0.0, 1.0, 0.0], {"category": doc2.category}),
            ]
        )

        self.assertEqual(self.sql_repo.count(), 2)
        self.assertEqual(len(self.vector_repo.fetch()), 2)

        deleted_vector_count = self.vector_repo.delete([str(doc2.id)])
        self.assertEqual(deleted_vector_count, 1)
        self.assertEqual(len(self.vector_repo.fetch()), 1)
        self.assertEqual(self.sql_repo.count(), 2)

        self.sql_repo.delete(doc1)
        self.assertEqual(self.sql_repo.count(), 1)
        self.assertEqual(len(self.vector_repo.fetch()), 1)


if __name__ == "__main__":
    unittest.main()
