"""PostgreSQL pgvector adapter implementing vector store operations.

This adapter uses a regular Mini ORM `Database` configured with `PostgresDialect`,
so SQL CRUD and vector search can share the same PostgreSQL connection/pool.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Mapping, Optional, Sequence

from ...core.contracts import DatabasePort
from ...core.vectors.vector_metrics import (
    VectorMetric,
    VectorMetricInput,
    normalize_vector_metric,
)
from ...core.vectors.vector_policies import VectorIdPolicy
from ...core.vectors.vector_types import VectorRecord, VectorSearchResult

_SUPPORTED_METRICS = {
    VectorMetric.COSINE,
    VectorMetric.DOT,
    VectorMetric.L2,
}

_METRIC_OPERATOR = {
    VectorMetric.COSINE: "<=>",  # cosine distance
    VectorMetric.DOT: "<#>",     # negative inner product
    VectorMetric.L2: "<->",      # Euclidean distance
}

_VECTOR_TYPE_RE = re.compile(r"^vector\((\d+)\)$")
_COLLECTIONS_META_TABLE = "_miniorm_pgvector_collections"


@dataclass
class _CollectionState:
    schema: str | None
    table: str
    dimension: int
    metric: VectorMetric | None


class PgVectorStore:
    """Vector store adapter backed by PostgreSQL + pgvector extension."""

    supports_filters = True
    id_policy = VectorIdPolicy.ANY

    def __init__(self, db: DatabasePort, *, ensure_extension: bool = True) -> None:
        self._db = db
        self._ensure_extension = ensure_extension
        self._collections: dict[str, _CollectionState] = {}

        dialect_name = str(getattr(db.dialect, "name", "")).lower()
        if dialect_name != "postgres":
            raise ValueError(
                "PgVectorStore requires a PostgreSQL database adapter "
                "configured with PostgresDialect."
            )

    def create_collection(
        self,
        name: str,
        dimension: int,
        metric: VectorMetricInput = VectorMetric.COSINE,
        *,
        overwrite: bool = False,
    ) -> None:
        if dimension <= 0:
            raise ValueError("dimension must be > 0")

        normalized_metric = normalize_vector_metric(
            metric,
            supported=_SUPPORTED_METRICS,
            aliases={"euclid": VectorMetric.L2},
        )
        schema, table = self._split_collection_name(name)
        qualified_table = self._quote_table(schema, table)
        exists = self._table_exists(schema, table)

        if exists and not overwrite:
            raise ValueError(f"Collection already exists: {name}")

        with self._db.transaction():
            if self._ensure_extension:
                self._db.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            self._ensure_collections_metadata_table()
            if exists:
                self._db.execute(f"DROP TABLE {qualified_table};")
            self._db.execute(
                f"""CREATE TABLE {qualified_table} (
                "id" TEXT PRIMARY KEY,
                "embedding" vector({dimension}) NOT NULL,
                "payload" JSONB
            );"""
            )
            self._upsert_collection_metadata(
                schema=schema,
                table=table,
                metric=normalized_metric,
                dimension=dimension,
            )

        self._collections[name] = _CollectionState(
            schema=schema,
            table=table,
            dimension=dimension,
            metric=normalized_metric,
        )

    def upsert(self, collection: str, records: Sequence[VectorRecord]) -> None:
        if not records:
            return

        state = self._get_collection_state(collection)
        table_sql = self._quote_table(state.schema, state.table)
        sql = (
            f"INSERT INTO {table_sql} "
            '("id", "embedding", "payload") '
            f"VALUES ({self._placeholder('id')}, {self._placeholder('vector')}::vector, "
            f"{self._placeholder('payload')}::jsonb) "
            'ON CONFLICT ("id") DO UPDATE SET '
            '"embedding" = EXCLUDED."embedding", '
            '"payload" = EXCLUDED."payload";'
        )

        with self._db.transaction():
            for record in records:
                vector = [float(value) for value in record.vector]
                if len(vector) != state.dimension:
                    raise ValueError(
                        f"Vector dimension mismatch: expected {state.dimension}, "
                        f"got {len(vector)}"
                    )

                payload_json = self._serialize_json(record.payload)
                params = self._build_params(
                    ("id", str(record.id)),
                    ("vector", self._vector_literal(vector)),
                    ("payload", payload_json),
                )
                self._db.execute(sql, params)

    def query(
        self,
        collection: str,
        vector: Sequence[float],
        *,
        top_k: int = 10,
        filters: Optional[Mapping[str, Any]] = None,
    ) -> list[VectorSearchResult]:
        if top_k <= 0:
            return []

        state = self._get_collection_state(collection)
        query_vector = [float(value) for value in vector]
        if len(query_vector) != state.dimension:
            raise ValueError(
                f"Vector dimension mismatch: expected {state.dimension}, "
                f"got {len(query_vector)}"
            )
        if state.metric is None:
            raise ValueError(
                f"Collection {collection!r} metric metadata is missing. "
                "Recreate collection via create_collection(..., overwrite=True) "
                "to store metric metadata."
            )

        operator = _METRIC_OPERATOR[state.metric]
        table_sql = self._quote_table(state.schema, state.table)
        distance_expr = (
            f'"embedding" {operator} {self._placeholder("query_vector")}::vector'
        )
        where_clauses: list[str] = []
        params: list[tuple[str, Any]] = [
            ("query_vector", self._vector_literal(query_vector))
        ]

        if filters:
            for idx, (key, value) in enumerate(filters.items(), start=1):
                param_name = f"filter_{idx}"
                where_clauses.append(
                    f'"payload" @> {self._placeholder(param_name)}::jsonb'
                )
                params.append((param_name, self._serialize_json({str(key): value})))

        where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        params.append(("limit", int(top_k)))

        sql = (
            'SELECT "id", "payload", '
            f"{distance_expr} AS __distance "
            f"FROM {table_sql}"
            f"{where_sql} "
            "ORDER BY __distance ASC "
            f"LIMIT {self._placeholder('limit')};"
        )

        rows = self._db.fetchall(sql, self._build_params(*params))
        hits: list[VectorSearchResult] = []
        for row in rows:
            distance = float(self._row_get(row, "__distance", 0.0))
            hits.append(
                VectorSearchResult(
                    id=str(self._row_get(row, "id")),
                    score=self._distance_to_score(state.metric, distance),
                    payload=self._deserialize_payload(self._row_get(row, "payload")),
                )
            )
        return hits

    def fetch(
        self, collection: str, ids: Optional[Sequence[str]] = None
    ) -> list[VectorRecord]:
        state = self._get_collection_state(collection)
        table_sql = self._quote_table(state.schema, state.table)
        base_sql = (
            'SELECT "id", "embedding"::text AS __vector_text, "payload" '
            f"FROM {table_sql}"
        )

        if ids is None:
            rows = self._db.fetchall(base_sql + ' ORDER BY "id" ASC;')
            return [self._row_to_record(row) for row in rows]

        if not ids:
            return []

        params: list[tuple[str, Any]] = []
        placeholders: list[str] = []
        for idx, item_id in enumerate(ids):
            name = f"id_{idx}"
            placeholders.append(self._placeholder(name))
            params.append((name, str(item_id)))

        sql = f'{base_sql} WHERE "id" IN ({", ".join(placeholders)});'
        rows = self._db.fetchall(sql, self._build_params(*params))
        rows_by_id = {str(self._row_get(row, "id")): row for row in rows}
        return [self._row_to_record(rows_by_id[str(item_id)]) for item_id in ids if str(item_id) in rows_by_id]

    def delete(self, collection: str, ids: Sequence[str]) -> int:
        if not ids:
            return 0

        state = self._get_collection_state(collection)
        table_sql = self._quote_table(state.schema, state.table)
        unique_ids = list(dict.fromkeys(str(item_id) for item_id in ids))
        params: list[tuple[str, Any]] = []
        placeholders: list[str] = []
        for idx, item_id in enumerate(unique_ids):
            name = f"id_{idx}"
            placeholders.append(self._placeholder(name))
            params.append((name, item_id))

        sql = (
            f'DELETE FROM {table_sql} WHERE "id" IN ({", ".join(placeholders)}) '
            'RETURNING "id";'
        )
        rows = self._db.fetchall(sql, self._build_params(*params))
        return len(rows)

    def _get_collection_state(self, collection: str) -> _CollectionState:
        state = self._collections.get(collection)
        if state is not None:
            return state

        schema, table = self._split_collection_name(collection)
        if not self._table_exists(schema, table):
            raise KeyError(f"Collection does not exist: {collection}")

        dimension = self._resolve_dimension(schema, table)
        if dimension is None:
            raise ValueError(
                f"Collection {collection!r} must include an embedding column with "
                "type vector(<dimension>)."
            )

        state = _CollectionState(
            schema=schema,
            table=table,
            dimension=dimension,
            metric=self._resolve_metric(schema, table),
        )
        self._collections[collection] = state
        return state

    def _ensure_collections_metadata_table(self) -> None:
        metadata_table = self._quote_identifier(_COLLECTIONS_META_TABLE)
        self._db.execute(
            f"""CREATE TABLE IF NOT EXISTS {metadata_table} (
            "collection_schema" TEXT NOT NULL,
            "collection_table" TEXT NOT NULL,
            "metric" TEXT NOT NULL,
            "dimension" INTEGER NOT NULL,
            PRIMARY KEY ("collection_schema", "collection_table")
        );"""
        )

    def _upsert_collection_metadata(
        self,
        *,
        schema: str | None,
        table: str,
        metric: VectorMetric,
        dimension: int,
    ) -> None:
        effective_schema = self._effective_schema(schema)
        metadata_table = self._quote_identifier(_COLLECTIONS_META_TABLE)
        sql = (
            f"INSERT INTO {metadata_table} "
            '("collection_schema", "collection_table", "metric", "dimension") '
            f"VALUES ({self._placeholder('collection_schema')}, "
            f"{self._placeholder('collection_table')}, "
            f"{self._placeholder('metric')}, {self._placeholder('dimension')}) "
            'ON CONFLICT ("collection_schema", "collection_table") DO UPDATE SET '
            '"metric" = EXCLUDED."metric", '
            '"dimension" = EXCLUDED."dimension";'
        )
        self._db.execute(
            sql,
            self._build_params(
                ("collection_schema", effective_schema),
                ("collection_table", table),
                ("metric", metric.value),
                ("dimension", int(dimension)),
            ),
        )

    def _table_exists(self, schema: str | None, table: str) -> bool:
        if schema is None:
            sql = (
                "SELECT 1 AS exists_flag "
                "FROM information_schema.tables "
                f"WHERE table_schema = current_schema() AND table_name = {self._placeholder('table')} "
                "LIMIT 1;"
            )
            params = self._build_params(("table", table))
        else:
            sql = (
                "SELECT 1 AS exists_flag "
                "FROM information_schema.tables "
                f"WHERE table_schema = {self._placeholder('schema')} "
                f"AND table_name = {self._placeholder('table')} "
                "LIMIT 1;"
            )
            params = self._build_params(("schema", schema), ("table", table))
        row = self._db.fetchone(sql, params)
        return row is not None

    def _resolve_dimension(self, schema: str | None, table: str) -> int | None:
        base_sql = (
            "SELECT format_type(a.atttypid, a.atttypmod) AS data_type "
            "FROM pg_attribute a "
            "JOIN pg_class c ON c.oid = a.attrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            f"WHERE c.relname = {self._placeholder('table')} "
            "AND a.attname = 'embedding' "
            "AND a.attnum > 0 "
            "AND NOT a.attisdropped "
        )
        if schema is None:
            sql = base_sql + "AND n.nspname = current_schema() LIMIT 1;"
            params = self._build_params(("table", table))
        else:
            sql = (
                base_sql
                + f"AND n.nspname = {self._placeholder('schema')} LIMIT 1;"
            )
            params = self._build_params(("table", table), ("schema", schema))

        row = self._db.fetchone(sql, params)
        if row is None:
            return None

        data_type = str(self._row_get(row, "data_type", "")).strip().lower()
        match = _VECTOR_TYPE_RE.fullmatch(data_type)
        if match is None:
            return None
        return int(match.group(1))

    def _resolve_metric(self, schema: str | None, table: str) -> VectorMetric | None:
        effective_schema = self._effective_schema(schema)
        metadata_table = self._quote_identifier(_COLLECTIONS_META_TABLE)
        sql = (
            'SELECT "metric" '
            f"FROM {metadata_table} "
            f'WHERE "collection_schema" = {self._placeholder("collection_schema")} '
            f'AND "collection_table" = {self._placeholder("collection_table")} '
            "LIMIT 1;"
        )
        try:
            row = self._db.fetchone(
                sql,
                self._build_params(
                    ("collection_schema", effective_schema),
                    ("collection_table", table),
                ),
            )
        except Exception:
            return None

        if row is None:
            return None

        raw_metric = self._row_get(row, "metric")
        if raw_metric is None:
            return None

        try:
            return normalize_vector_metric(
                str(raw_metric),
                supported=_SUPPORTED_METRICS,
                aliases={"euclid": VectorMetric.L2},
            )
        except ValueError as exc:
            raise ValueError(
                f"Collection metric metadata for {effective_schema}.{table} "
                f"is invalid: {raw_metric!r}"
            ) from exc

    def _current_schema(self) -> str:
        row = self._db.fetchone("SELECT current_schema() AS schema_name;")
        if row is None:
            raise RuntimeError("Unable to resolve current PostgreSQL schema.")
        schema_name = self._row_get(
            row,
            "schema_name",
            self._row_get(row, "current_schema"),
        )
        if not schema_name:
            raise RuntimeError("Unable to resolve current PostgreSQL schema.")
        return str(schema_name)

    def _effective_schema(self, schema: str | None) -> str:
        if schema is not None:
            return schema
        return self._current_schema()

    def _placeholder(self, key: str) -> str:
        return self._db.dialect.placeholder(key)

    def _build_params(self, *items: tuple[str, Any]) -> Any:
        if getattr(self._db.dialect, "paramstyle", "named") == "named":
            return {key: value for key, value in items}
        return tuple(value for _, value in items)

    @staticmethod
    def _distance_to_score(metric: VectorMetric, distance: float) -> float:
        if metric == VectorMetric.COSINE:
            return 1.0 - distance
        return -distance

    @staticmethod
    def _vector_literal(vector: Sequence[float]) -> str:
        return "[" + ",".join(f"{float(value):.17g}" for value in vector) + "]"

    @staticmethod
    def _serialize_json(payload: Mapping[str, Any] | None) -> str | None:
        if payload is None:
            return None
        try:
            return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
        except TypeError as exc:
            raise TypeError(
                "PgVectorStore payload must be JSON-serializable. "
                "Use JsonVectorPayloadCodec() for complex Python values."
            ) from exc

    @staticmethod
    def _deserialize_payload(value: Any) -> Mapping[str, Any] | None:
        if value is None:
            return None
        if isinstance(value, Mapping):
            return dict(value)
        if isinstance(value, (bytes, bytearray)):
            value = value.decode("utf-8")
        if isinstance(value, str):
            loaded = json.loads(value)
            if loaded is None:
                return None
            if not isinstance(loaded, Mapping):
                raise TypeError("PgVectorStore payload JSON must decode to an object.")
            return dict(loaded)
        raise TypeError(f"Unsupported payload type from PostgreSQL: {type(value).__name__}")

    @staticmethod
    def _parse_vector_text(value: Any) -> list[float]:
        if isinstance(value, (list, tuple)):
            return [float(item) for item in value]
        if isinstance(value, (bytes, bytearray)):
            value = value.decode("utf-8")
        if not isinstance(value, str):
            raise TypeError(f"Unsupported vector type from PostgreSQL: {type(value).__name__}")
        stripped = value.strip()
        if not (stripped.startswith("[") and stripped.endswith("]")):
            raise ValueError(f"Invalid pgvector text format: {value!r}")
        body = stripped[1:-1].strip()
        if not body:
            return []
        return [float(item.strip()) for item in body.split(",")]

    def _row_to_record(self, row: Mapping[str, Any]) -> VectorRecord:
        return VectorRecord(
            id=str(self._row_get(row, "id")),
            vector=self._parse_vector_text(self._row_get(row, "__vector_text")),
            payload=self._deserialize_payload(self._row_get(row, "payload")),
        )

    @staticmethod
    def _row_get(row: Mapping[str, Any], key: str, default: Any = None) -> Any:
        if key in row:
            return row[key]
        lowered = {str(item_key).lower(): value for item_key, value in row.items()}
        return lowered.get(key.lower(), default)

    @staticmethod
    def _split_collection_name(name: str) -> tuple[str | None, str]:
        cleaned = str(name).strip()
        if not cleaned:
            raise ValueError("collection name must be a non-empty string")
        if "\x00" in cleaned:
            raise ValueError("collection name contains invalid null character")

        parts = cleaned.split(".")
        if len(parts) == 1:
            table = parts[0].strip()
            if not table:
                raise ValueError("collection table name must be non-empty")
            return None, table
        if len(parts) == 2:
            schema = parts[0].strip()
            table = parts[1].strip()
            if not schema or not table:
                raise ValueError(
                    "collection name with schema must have format 'schema.table'"
                )
            return schema, table
        raise ValueError(
            "collection name must be 'table' or 'schema.table' for PgVectorStore"
        )

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        return '"' + str(identifier).replace('"', '""') + '"'

    def _quote_table(self, schema: str | None, table: str) -> str:
        table_quoted = self._quote_identifier(table)
        if schema is None:
            return table_quoted
        return f"{self._quote_identifier(schema)}.{table_quoted}"
