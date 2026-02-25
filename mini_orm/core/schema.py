"""Schema helpers for deriving and applying table/index SQL from models."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import MISSING, Field
from typing import Any, Type

from .contracts import AsyncDatabasePort, DatabasePort, DialectPort
from .models import DataclassModel, model_fields, require_dataclass_model, table_name
from .schema_columns import column_sql, is_nullable, resolve_sql_type
from .schema_indexes import (
    IndexSpec,
    build_index_sql,
    collect_index_specs,
    default_index_name,
    model_column_names,
)

_ALLOWED_SCHEMA_CONFLICTS = frozenset({"raise", "recreate"})


def validate_schema_conflict(schema_conflict: str) -> str:
    """Validate schema conflict mode and return normalized value."""

    if schema_conflict not in _ALLOWED_SCHEMA_CONFLICTS:
        allowed = ", ".join(sorted(_ALLOWED_SCHEMA_CONFLICTS))
        raise ValueError(f"schema_conflict must be one of: {allowed}.")
    return schema_conflict


def create_table_sql(
    cls: Type[DataclassModel],
    dialect: DialectPort,
    *,
    if_not_exists: bool = False,
) -> str:
    """Build `CREATE TABLE` statement for a dataclass model."""

    require_dataclass_model(cls)

    table_sql = dialect.q(table_name(cls))
    column_definitions = [column_sql(field, dialect) for field in model_fields(cls)]
    prefix = "CREATE TABLE IF NOT EXISTS" if if_not_exists else "CREATE TABLE"
    return f"{prefix} {table_sql} (\n  " + ",\n  ".join(column_definitions) + "\n);"


def create_index_sql(
    cls: Type[DataclassModel],
    dialect: DialectPort,
    column: str,
    *,
    unique: bool = False,
    name: str | None = None,
    if_not_exists: bool = False,
) -> str:
    """Build one single-column index SQL statement."""

    require_dataclass_model(cls)
    spec = IndexSpec(columns=(column,), unique=unique, name=name)
    return build_index_sql(
        table_name(cls),
        spec,
        dialect,
        model_column_names(cls),
        if_not_exists=if_not_exists,
    )


def create_indexes_sql(
    cls: Type[DataclassModel],
    dialect: DialectPort,
    *,
    if_not_exists: bool = False,
) -> list[str]:
    """Build index SQL statements from model field metadata and `__indexes__`."""

    require_dataclass_model(cls)
    table = table_name(cls)
    columns = model_column_names(cls)
    specs = collect_index_specs(cls)
    return [
        build_index_sql(
            table,
            spec,
            dialect,
            columns,
            if_not_exists=if_not_exists,
        )
        for spec in specs
    ]


def create_schema_sql(
    cls: Type[DataclassModel],
    dialect: DialectPort,
    *,
    if_not_exists: bool = False,
) -> list[str]:
    """Build full schema SQL list (table first, then indexes)."""

    return [
        create_table_sql(cls, dialect, if_not_exists=if_not_exists),
        *create_indexes_sql(cls, dialect, if_not_exists=if_not_exists),
    ]


def apply_schema(
    db: DatabasePort,
    cls: Type[DataclassModel],
    *,
    if_not_exists: bool = False,
) -> list[str]:
    """Create table and all configured indexes for a model on a database."""

    statements = create_schema_sql(cls, db.dialect, if_not_exists=if_not_exists)
    with db.transaction():
        for sql in statements:
            db.execute(sql)
    return statements


async def apply_schema_async(
    db: AsyncDatabasePort,
    cls: Type[DataclassModel],
    *,
    if_not_exists: bool = False,
) -> list[str]:
    """Async variant of `apply_schema` with identical SQL generation."""

    statements = create_schema_sql(cls, db.dialect, if_not_exists=if_not_exists)
    async with db.transaction():
        for sql in statements:
            await db.execute(sql)
    return statements


def ensure_schema(
    db: DatabasePort,
    cls: Type[DataclassModel],
    *,
    schema_conflict: str = "raise",
) -> list[str]:
    """Ensure schema exists and sync additive changes for a model.

    Behavior:
    - Create table/indexes if table does not exist.
    - If table exists:
      - Keep unchanged schema as-is.
      - Add missing columns when possible.
      - Create missing indexes and recreate changed index definitions.
    - For incompatible column changes (type/nullability/PK changes), behavior
      is controlled by `schema_conflict`:
      - `"raise"`: raise `ValueError`.
      - `"recreate"`: drop and recreate table/indexes.
    """

    require_dataclass_model(cls)
    schema_conflict = validate_schema_conflict(schema_conflict)
    table = table_name(cls)

    if not _table_exists(db, table):
        return apply_schema(db, cls)

    existing_columns = _existing_columns(db, table)
    missing_fields, conflicts = _column_diff(cls, existing_columns)

    if conflicts:
        if schema_conflict == "recreate":
            return _recreate_schema(db, cls)
        raise ValueError(
            f"Incompatible schema for table {table!r}: " + "; ".join(conflicts)
        )

    statements: list[str] = []
    with db.transaction():
        for field in missing_fields:
            sql = _add_column_sql(cls, db.dialect, field)
            db.execute(sql)
            statements.append(sql)

        statements.extend(_sync_indexes(db, cls))

    return statements


async def ensure_schema_async(
    db: AsyncDatabasePort,
    cls: Type[DataclassModel],
    *,
    schema_conflict: str = "raise",
) -> list[str]:
    """Async variant of `ensure_schema` with the same behavior."""

    require_dataclass_model(cls)
    schema_conflict = validate_schema_conflict(schema_conflict)
    table = table_name(cls)

    if not await _table_exists_async(db, table):
        return await apply_schema_async(db, cls)

    existing_columns = await _existing_columns_async(db, table)
    missing_fields, conflicts = _column_diff(cls, existing_columns)

    if conflicts:
        if schema_conflict == "recreate":
            return await _recreate_schema_async(db, cls)
        raise ValueError(
            f"Incompatible schema for table {table!r}: " + "; ".join(conflicts)
        )

    statements: list[str] = []
    async with db.transaction():
        for field in missing_fields:
            sql = _add_column_sql(cls, db.dialect, field)
            await db.execute(sql)
            statements.append(sql)

        index_statements = await _sync_indexes_async(db, cls)
        statements.extend(index_statements)

    return statements


def _recreate_schema(db: DatabasePort, cls: Type[DataclassModel]) -> list[str]:
    table = table_name(cls)
    drop_sql = _drop_table_sql(db.dialect, table)
    statements = [drop_sql, *create_schema_sql(cls, db.dialect, if_not_exists=False)]
    with db.transaction():
        for sql in statements:
            db.execute(sql)
    return statements


async def _recreate_schema_async(
    db: AsyncDatabasePort, cls: Type[DataclassModel]
) -> list[str]:
    table = table_name(cls)
    drop_sql = _drop_table_sql(db.dialect, table)
    statements = [drop_sql, *create_schema_sql(cls, db.dialect, if_not_exists=False)]
    async with db.transaction():
        for sql in statements:
            await db.execute(sql)
    return statements


def _table_exists(db: DatabasePort, table: str) -> bool:
    dialect_name = getattr(db.dialect, "name", "").lower()
    ph = db.dialect.placeholder("table")
    if dialect_name == "sqlite":
        sql = f"SELECT 1 AS exists_flag FROM sqlite_master WHERE type = 'table' AND name = {ph} LIMIT 1;"
    elif dialect_name == "postgres":
        sql = (
            "SELECT 1 AS exists_flag "
            f"FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = {ph} LIMIT 1;"
        )
    else:
        sql = (
            "SELECT 1 AS exists_flag "
            f"FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = {ph} LIMIT 1;"
        )

    row = db.fetchone(sql, _one_param(db.dialect, "table", table))
    return row is not None


async def _table_exists_async(db: AsyncDatabasePort, table: str) -> bool:
    dialect_name = getattr(db.dialect, "name", "").lower()
    ph = db.dialect.placeholder("table")
    if dialect_name == "sqlite":
        sql = f"SELECT 1 AS exists_flag FROM sqlite_master WHERE type = 'table' AND name = {ph} LIMIT 1;"
    elif dialect_name == "postgres":
        sql = (
            "SELECT 1 AS exists_flag "
            f"FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = {ph} LIMIT 1;"
        )
    else:
        sql = (
            "SELECT 1 AS exists_flag "
            f"FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = {ph} LIMIT 1;"
        )

    row = await db.fetchone(sql, _one_param(db.dialect, "table", table))
    return row is not None


def _existing_columns(
    db: DatabasePort,
    table: str,
) -> dict[str, tuple[str, bool]]:
    dialect_name = getattr(db.dialect, "name", "").lower()
    if dialect_name == "sqlite":
        rows = db.fetchall(f"PRAGMA table_info({db.dialect.q(table)});")
        result: dict[str, tuple[str, bool]] = {}
        for row in rows:
            name = str(_row_get(row, "name"))
            col_type = _normalize_type(_row_get(row, "type"))
            nullable = not bool(_row_get(row, "notnull"))
            result[name] = (col_type, nullable)
        return result

    ph = db.dialect.placeholder("table")
    if dialect_name == "postgres":
        sql = (
            "SELECT column_name, data_type, is_nullable "
            "FROM information_schema.columns "
            f"WHERE table_schema = current_schema() AND table_name = {ph} "
            "ORDER BY ordinal_position;"
        )
    else:
        sql = (
            "SELECT column_name, data_type, is_nullable "
            "FROM information_schema.columns "
            f"WHERE table_schema = DATABASE() AND table_name = {ph} "
            "ORDER BY ordinal_position;"
        )
    rows = db.fetchall(sql, _one_param(db.dialect, "table", table))
    result = {}
    for row in rows:
        name = str(_row_get(row, "column_name"))
        col_type = _normalize_type(_row_get(row, "data_type"))
        nullable = str(_row_get(row, "is_nullable")).upper() == "YES"
        result[name] = (col_type, nullable)
    return result


async def _existing_columns_async(
    db: AsyncDatabasePort,
    table: str,
) -> dict[str, tuple[str, bool]]:
    dialect_name = getattr(db.dialect, "name", "").lower()
    if dialect_name == "sqlite":
        rows = await db.fetchall(f"PRAGMA table_info({db.dialect.q(table)});")
        result: dict[str, tuple[str, bool]] = {}
        for row in rows:
            name = str(_row_get(row, "name"))
            col_type = _normalize_type(_row_get(row, "type"))
            nullable = not bool(_row_get(row, "notnull"))
            result[name] = (col_type, nullable)
        return result

    ph = db.dialect.placeholder("table")
    if dialect_name == "postgres":
        sql = (
            "SELECT column_name, data_type, is_nullable "
            "FROM information_schema.columns "
            f"WHERE table_schema = current_schema() AND table_name = {ph} "
            "ORDER BY ordinal_position;"
        )
    else:
        sql = (
            "SELECT column_name, data_type, is_nullable "
            "FROM information_schema.columns "
            f"WHERE table_schema = DATABASE() AND table_name = {ph} "
            "ORDER BY ordinal_position;"
        )
    rows = await db.fetchall(sql, _one_param(db.dialect, "table", table))
    result = {}
    for row in rows:
        name = str(_row_get(row, "column_name"))
        col_type = _normalize_type(_row_get(row, "data_type"))
        nullable = str(_row_get(row, "is_nullable")).upper() == "YES"
        result[name] = (col_type, nullable)
    return result


def _column_diff(
    cls: Type[DataclassModel],
    existing: dict[str, tuple[str, bool]],
) -> tuple[list[Field[Any]], list[str]]:
    missing_fields: list[Field[Any]] = []
    conflicts: list[str] = []

    for field in model_fields(cls):
        current = existing.get(field.name)
        if current is None:
            if field.metadata.get("pk") or field.metadata.get("auto"):
                conflicts.append(
                    f"missing auto/pk column {field.name!r} cannot be added automatically"
                )
                continue
            if not _can_add_missing_column_safely(field):
                conflicts.append(
                    f"missing NOT NULL column {field.name!r} cannot be added automatically; "
                    "add it as nullable first or run a backfill/default migration explicitly"
                )
                continue
            missing_fields.append(field)
            continue

        current_type, current_nullable = current
        expected_type = _normalize_type(resolve_sql_type(field.type))
        expected_nullable = is_nullable(field)

        if not _types_compatible(expected_type, current_type):
            conflicts.append(
                f"type mismatch for column {field.name!r}: expected {expected_type}, got {current_type}"
            )
        if expected_nullable != current_nullable and not _nullable_compat_for_pk_auto(
            field,
            current_nullable=current_nullable,
        ):
            conflicts.append(
                f"nullability mismatch for column {field.name!r}: expected "
                f"{'NULL' if expected_nullable else 'NOT NULL'}, got "
                f"{'NULL' if current_nullable else 'NOT NULL'}"
            )

    return missing_fields, conflicts


def _nullable_compat_for_pk_auto(
    field: Field[Any],
    *,
    current_nullable: bool,
) -> bool:
    """Allow DB-enforced NOT NULL for PK/auto columns declared Optional in Python."""

    if current_nullable:
        return False
    return bool(field.metadata.get("pk") or field.metadata.get("auto"))


def _can_add_missing_column_safely(field: Field[Any]) -> bool:
    if field.metadata.get("pk") or field.metadata.get("auto"):
        return False

    if field.metadata.get("nullable") is True:
        return True

    if is_nullable(field):
        return True

    # Field-level Python defaults/default_factory are not SQL defaults.
    # Adding NOT NULL columns without an SQL backfill/default may fail on existing rows.
    if field.default is not MISSING or field.default_factory is not MISSING:
        return False

    if "default" in field.metadata:
        return False

    return False


def _add_column_sql(
    cls: Type[DataclassModel],
    dialect: DialectPort,
    field: Field[Any],
) -> str:
    table = dialect.q(table_name(cls))
    return f"ALTER TABLE {table} ADD COLUMN {column_sql(field, dialect)};"


def _sync_indexes(db: DatabasePort, cls: Type[DataclassModel]) -> list[str]:
    table = table_name(cls)
    existing = _existing_indexes(db, table)
    desired = _desired_indexes(cls, db.dialect)
    statements: list[str] = []

    for index_name, (columns, unique, create_sql) in desired.items():
        current = existing.get(index_name)
        if current is None:
            db.execute(create_sql)
            statements.append(create_sql)
            continue
        if current != (columns, unique):
            drop_sql = _drop_index_sql(db.dialect, table, index_name)
            db.execute(drop_sql)
            db.execute(create_sql)
            statements.extend([drop_sql, create_sql])
    return statements


async def _sync_indexes_async(
    db: AsyncDatabasePort, cls: Type[DataclassModel]
) -> list[str]:
    table = table_name(cls)
    existing = await _existing_indexes_async(db, table)
    desired = _desired_indexes(cls, db.dialect)
    statements: list[str] = []

    for index_name, (columns, unique, create_sql) in desired.items():
        current = existing.get(index_name)
        if current is None:
            await db.execute(create_sql)
            statements.append(create_sql)
            continue
        if current != (columns, unique):
            drop_sql = _drop_index_sql(db.dialect, table, index_name)
            await db.execute(drop_sql)
            await db.execute(create_sql)
            statements.extend([drop_sql, create_sql])
    return statements


def _desired_indexes(
    cls: Type[DataclassModel],
    dialect: DialectPort,
) -> dict[str, tuple[tuple[str, ...], bool, str]]:
    table = table_name(cls)
    available_columns = model_column_names(cls)
    desired: dict[str, tuple[tuple[str, ...], bool, str]] = {}
    for spec in collect_index_specs(cls):
        name = spec.name or default_index_name(table, spec.columns, spec.unique)
        create_sql = build_index_sql(
            table,
            spec,
            dialect,
            available_columns,
            if_not_exists=False,
        )
        desired[name] = (spec.columns, spec.unique, create_sql)
    return desired


def _existing_indexes(
    db: DatabasePort,
    table: str,
) -> dict[str, tuple[tuple[str, ...], bool]]:
    dialect_name = getattr(db.dialect, "name", "").lower()
    if dialect_name == "sqlite":
        return _existing_indexes_sqlite(db, table)
    if dialect_name == "postgres":
        return _existing_indexes_postgres(db, table)
    return _existing_indexes_mysql(db, table)


async def _existing_indexes_async(
    db: AsyncDatabasePort,
    table: str,
) -> dict[str, tuple[tuple[str, ...], bool]]:
    dialect_name = getattr(db.dialect, "name", "").lower()
    if dialect_name == "sqlite":
        return await _existing_indexes_sqlite_async(db, table)
    if dialect_name == "postgres":
        return await _existing_indexes_postgres_async(db, table)
    return await _existing_indexes_mysql_async(db, table)


def _existing_indexes_sqlite(
    db: DatabasePort,
    table: str,
) -> dict[str, tuple[tuple[str, ...], bool]]:
    rows = db.fetchall(f"PRAGMA index_list({db.dialect.q(table)});")
    existing: dict[str, tuple[tuple[str, ...], bool]] = {}
    for row in rows:
        name = str(_row_get(row, "name"))
        origin = str(_row_get(row, "origin", default="")).lower()
        if origin == "pk":
            continue
        unique = bool(_row_get(row, "unique"))
        info_rows = db.fetchall(f"PRAGMA index_info({db.dialect.q(name)});")
        ordered = sorted(info_rows, key=lambda item: int(_row_get(item, "seqno", default=0)))
        columns = tuple(str(_row_get(item, "name")) for item in ordered)
        existing[name] = (columns, unique)
    return existing


async def _existing_indexes_sqlite_async(
    db: AsyncDatabasePort,
    table: str,
) -> dict[str, tuple[tuple[str, ...], bool]]:
    rows = await db.fetchall(f"PRAGMA index_list({db.dialect.q(table)});")
    existing: dict[str, tuple[tuple[str, ...], bool]] = {}
    for row in rows:
        name = str(_row_get(row, "name"))
        origin = str(_row_get(row, "origin", default="")).lower()
        if origin == "pk":
            continue
        unique = bool(_row_get(row, "unique"))
        info_rows = await db.fetchall(f"PRAGMA index_info({db.dialect.q(name)});")
        ordered = sorted(info_rows, key=lambda item: int(_row_get(item, "seqno", default=0)))
        columns = tuple(str(_row_get(item, "name")) for item in ordered)
        existing[name] = (columns, unique)
    return existing


def _existing_indexes_postgres(
    db: DatabasePort,
    table: str,
) -> dict[str, tuple[tuple[str, ...], bool]]:
    ph = db.dialect.placeholder("table")
    sql = (
        "SELECT i.relname AS index_name, "
        "ix.indisunique AS is_unique, "
        "a.attname AS column_name, "
        "ord.ordinality AS ordinal_position "
        "FROM pg_class t "
        "JOIN pg_namespace ns ON ns.oid = t.relnamespace "
        "JOIN pg_index ix ON t.oid = ix.indrelid "
        "JOIN pg_class i ON i.oid = ix.indexrelid "
        "JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS ord(attnum, ordinality) ON true "
        "JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ord.attnum "
        f"WHERE ns.nspname = current_schema() AND t.relname = {ph} AND ix.indisprimary = false "
        "ORDER BY i.relname, ord.ordinality;"
    )
    rows = db.fetchall(sql, _one_param(db.dialect, "table", table))
    return _aggregate_index_rows(rows)


async def _existing_indexes_postgres_async(
    db: AsyncDatabasePort,
    table: str,
) -> dict[str, tuple[tuple[str, ...], bool]]:
    ph = db.dialect.placeholder("table")
    sql = (
        "SELECT i.relname AS index_name, "
        "ix.indisunique AS is_unique, "
        "a.attname AS column_name, "
        "ord.ordinality AS ordinal_position "
        "FROM pg_class t "
        "JOIN pg_namespace ns ON ns.oid = t.relnamespace "
        "JOIN pg_index ix ON t.oid = ix.indrelid "
        "JOIN pg_class i ON i.oid = ix.indexrelid "
        "JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS ord(attnum, ordinality) ON true "
        "JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ord.attnum "
        f"WHERE ns.nspname = current_schema() AND t.relname = {ph} AND ix.indisprimary = false "
        "ORDER BY i.relname, ord.ordinality;"
    )
    rows = await db.fetchall(sql, _one_param(db.dialect, "table", table))
    return _aggregate_index_rows(rows)


def _existing_indexes_mysql(
    db: DatabasePort,
    table: str,
) -> dict[str, tuple[tuple[str, ...], bool]]:
    rows = db.fetchall(f"SHOW INDEX FROM {db.dialect.q(table)};")
    return _aggregate_mysql_indexes(rows)


async def _existing_indexes_mysql_async(
    db: AsyncDatabasePort,
    table: str,
) -> dict[str, tuple[tuple[str, ...], bool]]:
    rows = await db.fetchall(f"SHOW INDEX FROM {db.dialect.q(table)};")
    return _aggregate_mysql_indexes(rows)


def _aggregate_index_rows(
    rows: list[dict[str, Any]],
) -> dict[str, tuple[tuple[str, ...], bool]]:
    grouped: dict[str, list[tuple[int, str]]] = defaultdict(list)
    unique_map: dict[str, bool] = {}
    for row in rows:
        name = str(_row_get(row, "index_name"))
        column_name = str(_row_get(row, "column_name"))
        position = int(_row_get(row, "ordinal_position", default=1))
        unique = bool(_row_get(row, "is_unique"))
        grouped[name].append((position, column_name))
        unique_map[name] = unique

    parsed: dict[str, tuple[tuple[str, ...], bool]] = {}
    for name, columns in grouped.items():
        ordered = tuple(column for _, column in sorted(columns, key=lambda item: item[0]))
        parsed[name] = (ordered, unique_map.get(name, False))
    return parsed


def _aggregate_mysql_indexes(
    rows: list[dict[str, Any]],
) -> dict[str, tuple[tuple[str, ...], bool]]:
    grouped: dict[str, list[tuple[int, str]]] = defaultdict(list)
    unique_map: dict[str, bool] = {}
    for row in rows:
        name = str(_row_get(row, "Key_name", "key_name"))
        if name.upper() == "PRIMARY":
            continue
        column_name = str(_row_get(row, "Column_name", "column_name"))
        position = int(_row_get(row, "Seq_in_index", "seq_in_index", default=1))
        non_unique = _row_get(row, "Non_unique", "non_unique", default=1)
        unique = str(non_unique) in {"0", "False", "false"}
        grouped[name].append((position, column_name))
        unique_map[name] = unique

    parsed: dict[str, tuple[tuple[str, ...], bool]] = {}
    for name, columns in grouped.items():
        ordered = tuple(column for _, column in sorted(columns, key=lambda item: item[0]))
        parsed[name] = (ordered, unique_map.get(name, False))
    return parsed


def _drop_index_sql(dialect: DialectPort, table: str, index_name: str) -> str:
    index_sql = dialect.q(index_name)
    if getattr(dialect, "name", "").lower() == "mysql":
        return f"DROP INDEX {index_sql} ON {dialect.q(table)};"
    return f"DROP INDEX {index_sql};"


def _drop_table_sql(dialect: DialectPort, table: str) -> str:
    table_sql = dialect.q(table)
    if getattr(dialect, "name", "").lower() == "postgres":
        return f"DROP TABLE {table_sql} CASCADE;"
    return f"DROP TABLE {table_sql};"


def _type_tokens(raw: Any) -> tuple[str, ...]:
    text = (
        str(raw or "")
        .upper()
        .replace("(", " ")
        .replace(")", " ")
        .replace(",", " ")
    )
    return tuple(token for token in text.split() if token)


def _normalize_type(raw: Any) -> str:
    tokens = _type_tokens(raw)
    if not tokens:
        return ""

    joined = " ".join(tokens)
    aliases = {
        "BOOL": "BOOLEAN",
        "DATETIME": "TIMESTAMP",
        "INT": "INTEGER",
        "SERIAL": "INTEGER",
        "SMALLSERIAL": "SMALLINT",
        "BIGSERIAL": "BIGINT",
        "CHARACTER": "TEXT",
        "CHAR": "TEXT",
        "VARCHAR": "TEXT",
        "NVARCHAR": "TEXT",
        "VARCHAR2": "TEXT",
        "NVARCHAR2": "TEXT",
        "CHARACTER VARYING": "TEXT",
        "DOUBLE PRECISION": "DOUBLE",
        "JSONB": "JSON",
        "DEC": "DECIMAL",
    }
    exact = aliases.get(joined)
    if exact is not None:
        return exact

    first = tokens[0]
    if first in aliases:
        return aliases[first]
    if first in {"CHARACTER", "VARCHAR", "NVARCHAR"}:
        return "TEXT"
    if "PRECISION" in tokens and "DOUBLE" in tokens:
        return "DOUBLE"
    if first in {"VARBINARY"}:
        return "BLOB"
    return first


def _types_compatible(expected: str, current: str) -> bool:
    expected_normalized = _normalize_type(expected)
    current_normalized = _normalize_type(current)
    if expected_normalized == current_normalized:
        return True

    groups = {
        "INTEGER": {"INTEGER", "BIGINT", "SMALLINT", "TINYINT", "MEDIUMINT"},
        "REAL": {"REAL", "FLOAT", "DOUBLE"},
        "TEXT": {"TEXT", "CHAR", "STRING", "CLOB"},
        "BOOLEAN": {"BOOLEAN", "TINYINT"},
        "TIMESTAMP": {"TIMESTAMP"},
        "DATE": {"DATE"},
        "TIME": {"TIME"},
        "NUMERIC": {"NUMERIC", "DECIMAL", "NUMBER"},
        "BLOB": {"BLOB", "BYTEA", "BINARY"},
        "JSON": {"JSON"},
    }
    for compatible in groups.values():
        if expected_normalized in compatible and current_normalized in compatible:
            return True

    expected_tokens = list(_type_tokens(expected))
    current_tokens = list(_type_tokens(current))
    if expected_tokens and current_tokens:
        if expected_tokens[0] == current_tokens[0]:
            expected_suffix = set(expected_tokens[1:])
            current_suffix = set(current_tokens[1:])
            if expected_suffix <= current_suffix or current_suffix <= expected_suffix:
                return True

        expected_token_set = set(expected_tokens)
        current_token_set = set(current_tokens)
        if (
            expected_tokens[0] == current_tokens[0]
            and (
                expected_token_set <= current_token_set
                or current_token_set <= expected_token_set
            )
        ):
            return True

        known_markers = {
            "INTEGER",
            "BIGINT",
            "SMALLINT",
            "TINYINT",
            "REAL",
            "FLOAT",
            "DOUBLE",
            "NUMERIC",
            "DECIMAL",
            "TEXT",
            "CHARACTER",
            "VARCHAR",
            "BOOLEAN",
            "TIMESTAMP",
            "DATE",
            "TIME",
            "BLOB",
            "BYTEA",
            "JSON",
        }
        expected_markers = set(expected_tokens) & known_markers
        current_markers = set(current_tokens) & known_markers
        if expected_markers and expected_markers == current_markers:
            return True

    return False


def _row_get(row: dict[str, Any], *keys: str, default: Any = None) -> Any:
    lowered = {str(key).lower(): value for key, value in row.items()}
    sentinel = object()
    for key in keys:
        value = lowered.get(key.lower(), sentinel)
        if value is not sentinel:
            return value
    return default


def _one_param(dialect: DialectPort, key: str, value: Any) -> Any:
    if getattr(dialect, "paramstyle", "named") == "named":
        return {key: value}
    return (value,)
