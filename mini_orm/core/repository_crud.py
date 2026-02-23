"""Low-level CRUD/query implementations used by `Repository`."""

from __future__ import annotations

from collections.abc import Sequence as SequenceABC
from typing import Any, Mapping, Sequence

from .conditions import C
from .models import model_fields, row_to_model, to_dict
from .query_builder import append_limit_offset, compile_order_by, compile_where


def insert(repo: Any, obj: Any) -> Any:
    """Insert an object and populate auto primary key when available."""

    data = to_dict(obj)
    columns = repo.meta.columns

    if repo.meta.auto_pk and data.get(repo.meta.auto_pk) is None:
        columns = [name for name in columns if name != repo.meta.auto_pk]

    table_sql = repo.d.q(repo.meta.table)

    if not columns:
        sql = f"INSERT INTO {table_sql} DEFAULT VALUES"
        if repo.meta.auto_pk and repo.d.supports_returning:
            sql += repo.d.returning_clause(repo.meta.auto_pk) + ";"
            row = repo.db.fetchone(sql)
            if row and repo.meta.auto_pk in row:
                setattr(obj, repo.meta.auto_pk, row[repo.meta.auto_pk])
            return obj

        sql += ";"
        cursor = repo.db.execute(sql)
        if repo.meta.auto_pk and getattr(obj, repo.meta.auto_pk) is None:
            new_id = repo.d.get_lastrowid(cursor)
            if new_id is not None:
                setattr(obj, repo.meta.auto_pk, new_id)
        return obj

    column_sql = ", ".join(repo.d.q(name) for name in columns)

    if repo.d.paramstyle == "named":
        placeholders = ", ".join(f":{name}" for name in columns)
        params = {name: data[name] for name in columns}
        sql = f"INSERT INTO {table_sql} ({column_sql}) VALUES ({placeholders})"

        if repo.meta.auto_pk and repo.d.supports_returning:
            sql += repo.d.returning_clause(repo.meta.auto_pk) + ";"
            row = repo.db.fetchone(sql, params)
            if row and repo.meta.auto_pk in row:
                setattr(obj, repo.meta.auto_pk, row[repo.meta.auto_pk])
            return obj

        sql += ";"
        cursor = repo.db.execute(sql, params)
    else:
        placeholders = ", ".join(repo.d.placeholder(name) for name in columns)
        sql = f"INSERT INTO {table_sql} ({column_sql}) VALUES ({placeholders})"
        values = [data[name] for name in columns]

        if repo.meta.auto_pk and repo.d.supports_returning:
            sql += repo.d.returning_clause(repo.meta.auto_pk) + ";"
            row = repo.db.fetchone(sql, values)
            if row and repo.meta.auto_pk in row:
                setattr(obj, repo.meta.auto_pk, row[repo.meta.auto_pk])
            return obj

        sql += ";"
        cursor = repo.db.execute(sql, values)

    if repo.meta.auto_pk and getattr(obj, repo.meta.auto_pk) is None:
        new_id = repo.d.get_lastrowid(cursor)
        if new_id is not None:
            setattr(obj, repo.meta.auto_pk, new_id)

    return obj


def update(repo: Any, obj: Any) -> int:
    """Update one row identified by model primary key."""

    data = to_dict(obj)
    pk_value = data.get(repo.meta.pk)
    if pk_value is None:
        raise ValueError("Cannot UPDATE without PK set on object.")
    if not repo.meta.writable_columns:
        raise ValueError(
            "Cannot UPDATE model with no writable columns besides primary key."
        )

    table_sql = repo.d.q(repo.meta.table)

    if repo.d.paramstyle == "named":
        set_clause = ", ".join(
            f"{repo.d.q(name)} = :{name}" for name in repo.meta.writable_columns
        )
        sql = (
            f"UPDATE {table_sql} SET {set_clause} "
            f"WHERE {repo.d.q(repo.meta.pk)} = :{repo.meta.pk};"
        )
        params = {name: data[name] for name in repo.meta.writable_columns}
        params[repo.meta.pk] = pk_value
        cursor = repo.db.execute(sql, params)
        return cursor.rowcount

    set_clause = ", ".join(
        f"{repo.d.q(name)} = {repo.d.placeholder(name)}"
        for name in repo.meta.writable_columns
    )
    sql = (
        f"UPDATE {table_sql} SET {set_clause} "
        f"WHERE {repo.d.q(repo.meta.pk)} = {repo.d.placeholder(repo.meta.pk)};"
    )
    values = [data[name] for name in repo.meta.writable_columns] + [pk_value]
    cursor = repo.db.execute(sql, values)
    return cursor.rowcount


def delete(repo: Any, obj: Any) -> int:
    """Delete one row identified by model primary key."""

    data = to_dict(obj)
    pk_value = data.get(repo.meta.pk)
    if pk_value is None:
        raise ValueError("Cannot DELETE without PK set on object.")

    table_sql = repo.d.q(repo.meta.table)

    if repo.d.paramstyle == "named":
        sql = f"DELETE FROM {table_sql} WHERE {repo.d.q(repo.meta.pk)} = :pk;"
        cursor = repo.db.execute(sql, {"pk": pk_value})
        return cursor.rowcount

    sql = (
        f"DELETE FROM {table_sql} WHERE {repo.d.q(repo.meta.pk)} "
        f"= {repo.d.placeholder('pk')};"
    )
    cursor = repo.db.execute(sql, [pk_value])
    return cursor.rowcount


def get(repo: Any, pk_value: Any) -> Any:
    """Fetch one row by primary key and map it to the model type."""

    table_sql = repo.d.q(repo.meta.table)

    if repo.d.paramstyle == "named":
        sql = f"SELECT * FROM {table_sql} WHERE {repo.d.q(repo.meta.pk)} = :pk LIMIT 1;"
        row = repo.db.fetchone(sql, {"pk": pk_value})
    else:
        sql = (
            f"SELECT * FROM {table_sql} WHERE {repo.d.q(repo.meta.pk)} = "
            f"{repo.d.placeholder('pk')} LIMIT 1;"
        )
        row = repo.db.fetchone(sql, [pk_value])

    return row_to_model(repo.model, row) if row else None


def list_rows(
    repo: Any,
    *,
    where: Any = None,
    order_by: Sequence[Any] | None = None,
    limit: int | None = None,
    offset: int | None = None,
) -> list[Any]:
    """List rows with optional filtering, sorting, and pagination."""

    sql = f"SELECT * FROM {repo.d.q(repo.meta.table)}"

    where_fragment = compile_where(where, repo.d)
    sql += where_fragment.sql
    sql += compile_order_by(order_by, repo.d)

    sql, params = append_limit_offset(
        sql,
        where_fragment.params,
        limit=limit,
        offset=offset,
        dialect=repo.d,
    )

    rows = repo.db.fetchall(sql + ";", params)
    return [row_to_model(repo.model, row) for row in rows]


def count_rows(repo: Any, *, where: Any = None) -> int:
    """Count rows matching optional conditions."""

    sql = f'SELECT COUNT(*) AS "__count" FROM {repo.d.q(repo.meta.table)}'
    where_fragment = compile_where(where, repo.d)
    sql += where_fragment.sql

    row = repo.db.fetchone(sql + ";", where_fragment.params)
    if not row:
        return 0
    return int(row["__count"])


def exists_rows(repo: Any, *, where: Any = None) -> bool:
    """Return whether at least one row matches optional conditions."""

    sql = f"SELECT 1 FROM {repo.d.q(repo.meta.table)}"
    where_fragment = compile_where(where, repo.d)
    sql += where_fragment.sql
    sql += " LIMIT 1"

    row = repo.db.fetchone(sql + ";", where_fragment.params)
    return row is not None


def insert_many(repo: Any, objects: Sequence[Any]) -> list[Any]:
    """Insert many objects and return inserted objects."""

    inserted: list[Any] = []
    for obj in objects:
        inserted.append(repo.insert(obj))
    return inserted


def update_where(repo: Any, values: Mapping[str, Any], *, where: Any) -> int:
    """Update rows by conditions and return affected row count."""

    if not values:
        raise ValueError("update values must not be empty.")
    if where is None:
        raise ValueError("where is required for update_where().")

    invalid = [key for key in values if key not in repo.meta.writable_columns]
    if invalid:
        raise ValueError(
            "update_where() only supports writable model columns. "
            f"Invalid: {invalid}"
        )

    table_sql = repo.d.q(repo.meta.table)
    where_fragment = compile_where(where, repo.d)
    if not where_fragment.sql:
        raise ValueError("where is required for update_where().")

    if repo.d.paramstyle == "named":
        set_clause = ", ".join(f"{repo.d.q(key)} = :set_{key}" for key in values)
        sql = f"UPDATE {table_sql} SET {set_clause}{where_fragment.sql};"
        params = {f"set_{key}": value for key, value in values.items()}
        if isinstance(where_fragment.params, dict):
            params.update(where_fragment.params)
        cursor = repo.db.execute(sql, params)
        return cursor.rowcount

    set_clause = ", ".join(
        f"{repo.d.q(key)} = {repo.d.placeholder(f'set_{key}')}" for key in values
    )
    sql = f"UPDATE {table_sql} SET {set_clause}{where_fragment.sql};"

    params: list[Any] = list(values.values())
    if isinstance(where_fragment.params, list):
        params.extend(where_fragment.params)

    cursor = repo.db.execute(sql, params)
    return cursor.rowcount


def delete_where(repo: Any, *, where: Any) -> int:
    """Delete rows by conditions and return affected row count."""

    if where is None:
        raise ValueError("where is required for delete_where().")

    where_fragment = compile_where(where, repo.d)
    if not where_fragment.sql:
        raise ValueError("where is required for delete_where().")

    sql = f"DELETE FROM {repo.d.q(repo.meta.table)}{where_fragment.sql};"
    cursor = repo.db.execute(sql, where_fragment.params)
    return cursor.rowcount


def get_or_create(
    repo: Any,
    *,
    lookup: Mapping[str, Any],
    defaults: Mapping[str, Any] | None = None,
) -> tuple[Any, bool]:
    """Get first row by lookup fields or atomically create a new object.

    This flow is intentionally insert-first to avoid TOCTOU races:
    - build `obj = repo.model(**payload)`
    - try `repo.insert(obj)` first
    - on integrity conflict, query existing row and return `(row, False)`
    """

    if not lookup:
        raise ValueError("lookup must not be empty.")
    if not _has_unique_lookup_constraint(repo, tuple(lookup.keys())):
        raise ValueError(
            "get_or_create() requires lookup fields backed by a UNIQUE or "
            "PRIMARY KEY constraint."
        )

    payload: dict[str, Any] = dict(lookup)
    if defaults:
        payload.update(defaults)

    obj = repo.model(**payload)
    integrity_error: Exception | None = None
    try:
        repo.insert(obj)
        return obj, True
    except Exception as exc:
        if not _is_integrity_error(repo, exc):
            raise
        integrity_error = exc

    conditions = [C.eq(key, value) for key, value in lookup.items()]
    found = repo.list(where=conditions, limit=1)
    if found:
        return found[0], False

    assert integrity_error is not None
    raise integrity_error


def _has_unique_lookup_constraint(repo: Any, lookup_keys: tuple[str, ...]) -> bool:
    key_set = set(lookup_keys)
    if not key_set:
        return False

    if key_set == {repo.meta.pk}:
        return True

    fields_by_name = {f.name: f for f in model_fields(repo.model)}
    if len(lookup_keys) == 1:
        field = fields_by_name.get(lookup_keys[0])
        if field and bool(field.metadata.get("unique_index")):
            return True

    raw_indexes = getattr(repo.model, "__indexes__", ())
    for raw in raw_indexes:
        if not isinstance(raw, Mapping):
            continue
        if not bool(raw.get("unique", False)):
            continue
        raw_columns = raw.get("columns")
        if isinstance(raw_columns, str):
            columns = (raw_columns,)
        elif isinstance(raw_columns, SequenceABC):
            columns = tuple(raw_columns)
        else:
            continue

        if len(columns) == len(lookup_keys) and set(columns) == key_set:
            return True

    return False


def _is_integrity_error(repo: Any, exc: Exception) -> bool:
    """Detect integrity exceptions from DB driver with safe fallbacks."""

    conn = getattr(repo.db, "conn", None)
    driver_integrity = None
    if conn is not None:
        module_obj = __import__(conn.__class__.__module__)
        driver_integrity = getattr(module_obj, "IntegrityError", None)

    if driver_integrity and isinstance(exc, driver_integrity):
        return True

    return any(cls.__name__ == "IntegrityError" for cls in type(exc).mro())
