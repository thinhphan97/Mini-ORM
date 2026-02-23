"""Repository implementation for dataclass-based models."""

from __future__ import annotations

from typing import Any, Generic, List, Mapping, Optional, Sequence, Type, TypeVar

from .conditions import C, OrderBy
from .contracts import DatabasePort
from .metadata import build_model_metadata
from .models import DataclassModel, require_dataclass_model, row_to_model, to_dict
from .query_builder import (
    WhereInput,
    append_limit_offset,
    compile_order_by,
    compile_where,
)

T = TypeVar("T", bound=DataclassModel)


class Repository(Generic[T]):
    """CRUD repository backed by a `DatabasePort` implementation.

    The repository is intentionally single-table and single-primary-key to keep
    the core simple while still being extensible.
    """

    def __init__(self, db: DatabasePort, model: Type[T]):
        """Create repository for a model type.

        Args:
            db: Database adapter implementing `DatabasePort`.
            model: Dataclass model type.
        """

        require_dataclass_model(model)
        self.db = db
        self.model = model
        self.d = db.dialect
        self.meta = build_model_metadata(model)

    def insert(self, obj: T) -> T:
        """Insert an object and populate auto primary key when available."""

        data = to_dict(obj)
        columns = self.meta.columns

        if self.meta.auto_pk and data.get(self.meta.auto_pk) is None:
            columns = [name for name in columns if name != self.meta.auto_pk]

        table_sql = self.d.q(self.meta.table)

        if not columns:
            sql = f"INSERT INTO {table_sql} DEFAULT VALUES"
            if self.meta.auto_pk and self.d.supports_returning:
                sql += self.d.returning_clause(self.meta.auto_pk) + ";"
                row = self.db.fetchone(sql)
                if row and self.meta.auto_pk in row:
                    setattr(obj, self.meta.auto_pk, row[self.meta.auto_pk])
                return obj

            sql += ";"
            cursor = self.db.execute(sql)
            if self.meta.auto_pk and getattr(obj, self.meta.auto_pk) is None:
                new_id = self.d.get_lastrowid(cursor)
                if new_id is not None:
                    setattr(obj, self.meta.auto_pk, new_id)
            return obj

        column_sql = ", ".join(self.d.q(name) for name in columns)

        if self.d.paramstyle == "named":
            placeholders = ", ".join(f":{name}" for name in columns)
            params = {name: data[name] for name in columns}
            sql = f"INSERT INTO {table_sql} ({column_sql}) VALUES ({placeholders})"

            if self.meta.auto_pk and self.d.supports_returning:
                sql += self.d.returning_clause(self.meta.auto_pk) + ";"
                row = self.db.fetchone(sql, params)
                if row and self.meta.auto_pk in row:
                    setattr(obj, self.meta.auto_pk, row[self.meta.auto_pk])
                return obj

            sql += ";"
            cursor = self.db.execute(sql, params)
        else:
            placeholders = ", ".join(self.d.placeholder(name) for name in columns)
            sql = f"INSERT INTO {table_sql} ({column_sql}) VALUES ({placeholders})"
            values = [data[name] for name in columns]

            if self.meta.auto_pk and self.d.supports_returning:
                sql += self.d.returning_clause(self.meta.auto_pk) + ";"
                row = self.db.fetchone(sql, values)
                if row and self.meta.auto_pk in row:
                    setattr(obj, self.meta.auto_pk, row[self.meta.auto_pk])
                return obj

            sql += ";"
            cursor = self.db.execute(sql, values)

        if self.meta.auto_pk and getattr(obj, self.meta.auto_pk) is None:
            new_id = self.d.get_lastrowid(cursor)
            if new_id is not None:
                setattr(obj, self.meta.auto_pk, new_id)

        return obj

    def update(self, obj: T) -> int:
        """Update one row identified by model primary key.

        Returns:
            Number of affected rows.

        Raises:
            ValueError: If primary key value is missing on the object.
        """

        data = to_dict(obj)
        pk_value = data.get(self.meta.pk)
        if pk_value is None:
            raise ValueError("Cannot UPDATE without PK set on object.")
        if not self.meta.writable_columns:
            raise ValueError(
                "Cannot UPDATE model with no writable columns besides primary key."
            )

        table_sql = self.d.q(self.meta.table)

        if self.d.paramstyle == "named":
            set_clause = ", ".join(
                f"{self.d.q(name)} = :{name}" for name in self.meta.writable_columns
            )
            sql = (
                f"UPDATE {table_sql} SET {set_clause} "
                f"WHERE {self.d.q(self.meta.pk)} = :{self.meta.pk};"
            )
            params = {name: data[name] for name in self.meta.writable_columns}
            params[self.meta.pk] = pk_value
            cursor = self.db.execute(sql, params)
            return cursor.rowcount

        set_clause = ", ".join(
            f"{self.d.q(name)} = {self.d.placeholder(name)}"
            for name in self.meta.writable_columns
        )
        sql = (
            f"UPDATE {table_sql} SET {set_clause} "
            f"WHERE {self.d.q(self.meta.pk)} = {self.d.placeholder(self.meta.pk)};"
        )
        values = [data[name] for name in self.meta.writable_columns] + [pk_value]
        cursor = self.db.execute(sql, values)
        return cursor.rowcount

    def delete(self, obj: T) -> int:
        """Delete one row identified by model primary key.

        Returns:
            Number of affected rows.

        Raises:
            ValueError: If primary key value is missing on the object.
        """

        data = to_dict(obj)
        pk_value = data.get(self.meta.pk)
        if pk_value is None:
            raise ValueError("Cannot DELETE without PK set on object.")

        table_sql = self.d.q(self.meta.table)

        if self.d.paramstyle == "named":
            sql = f"DELETE FROM {table_sql} WHERE {self.d.q(self.meta.pk)} = :pk;"
            cursor = self.db.execute(sql, {"pk": pk_value})
            return cursor.rowcount

        sql = (
            f"DELETE FROM {table_sql} WHERE {self.d.q(self.meta.pk)} "
            f"= {self.d.placeholder('pk')};"
        )
        cursor = self.db.execute(sql, [pk_value])
        return cursor.rowcount

    def get(self, pk_value: Any) -> Optional[T]:
        """Fetch one row by primary key and map it to the model type."""

        table_sql = self.d.q(self.meta.table)

        if self.d.paramstyle == "named":
            sql = f"SELECT * FROM {table_sql} WHERE {self.d.q(self.meta.pk)} = :pk LIMIT 1;"
            row = self.db.fetchone(sql, {"pk": pk_value})
        else:
            sql = (
                f"SELECT * FROM {table_sql} WHERE {self.d.q(self.meta.pk)} = "
                f"{self.d.placeholder('pk')} LIMIT 1;"
            )
            row = self.db.fetchone(sql, [pk_value])

        return row_to_model(self.model, row) if row else None

    def list(
        self,
        where: WhereInput = None,
        order_by: Optional[Sequence[OrderBy]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> List[T]:
        """List rows with optional filtering, sorting, and pagination.

        Args:
            where: One condition or list of conditions (joined with `AND`).
            order_by: Optional ordering definitions.
            limit: Optional max rows.
            offset: Optional starting offset.

        Returns:
            List of mapped model instances.
        """

        sql = f"SELECT * FROM {self.d.q(self.meta.table)}"

        where_fragment = compile_where(where, self.d)
        sql += where_fragment.sql
        sql += compile_order_by(order_by, self.d)

        sql, params = append_limit_offset(
            sql,
            where_fragment.params,
            limit=limit,
            offset=offset,
            dialect=self.d,
        )

        rows = self.db.fetchall(sql + ";", params)
        return [row_to_model(self.model, row) for row in rows]

    def count(self, where: WhereInput = None) -> int:
        """Count rows matching optional conditions."""

        sql = f'SELECT COUNT(*) AS "__count" FROM {self.d.q(self.meta.table)}'
        where_fragment = compile_where(where, self.d)
        sql += where_fragment.sql

        row = self.db.fetchone(sql + ";", where_fragment.params)
        if not row:
            return 0
        return int(row["__count"])

    def exists(self, where: WhereInput = None) -> bool:
        """Return whether at least one row matches optional conditions."""

        sql = f"SELECT 1 FROM {self.d.q(self.meta.table)}"
        where_fragment = compile_where(where, self.d)
        sql += where_fragment.sql
        sql += " LIMIT 1"

        row = self.db.fetchone(sql + ";", where_fragment.params)
        return row is not None

    def insert_many(self, objects: Sequence[T]) -> list[T]:
        """Insert many objects and return inserted objects."""

        inserted: list[T] = []
        for obj in objects:
            inserted.append(self.insert(obj))
        return inserted

    def update_where(
        self,
        values: Mapping[str, Any],
        *,
        where: WhereInput,
    ) -> int:
        """Update rows by conditions and return affected row count."""

        if not values:
            raise ValueError("update values must not be empty.")
        if where is None:
            raise ValueError("where is required for update_where().")

        invalid = [key for key in values if key not in self.meta.writable_columns]
        if invalid:
            raise ValueError(
                "update_where() only supports writable model columns. "
                f"Invalid: {invalid}"
            )

        table_sql = self.d.q(self.meta.table)
        where_fragment = compile_where(where, self.d)
        if not where_fragment.sql:
            raise ValueError("where is required for update_where().")

        if self.d.paramstyle == "named":
            set_clause = ", ".join(f"{self.d.q(key)} = :set_{key}" for key in values)
            sql = f"UPDATE {table_sql} SET {set_clause}{where_fragment.sql};"
            params = {f"set_{key}": value for key, value in values.items()}
            if isinstance(where_fragment.params, dict):
                params.update(where_fragment.params)
            cursor = self.db.execute(sql, params)
            return cursor.rowcount

        set_clause = ", ".join(
            f"{self.d.q(key)} = {self.d.placeholder(f'set_{key}')}" for key in values
        )
        sql = f"UPDATE {table_sql} SET {set_clause}{where_fragment.sql};"

        params: list[Any] = list(values.values())
        if isinstance(where_fragment.params, list):
            params.extend(where_fragment.params)

        cursor = self.db.execute(sql, params)
        return cursor.rowcount

    def delete_where(self, *, where: WhereInput) -> int:
        """Delete rows by conditions and return affected row count."""

        if where is None:
            raise ValueError("where is required for delete_where().")

        where_fragment = compile_where(where, self.d)
        if not where_fragment.sql:
            raise ValueError("where is required for delete_where().")

        sql = f"DELETE FROM {self.d.q(self.meta.table)}{where_fragment.sql};"
        cursor = self.db.execute(sql, where_fragment.params)
        return cursor.rowcount

    def get_or_create(
        self,
        *,
        lookup: Mapping[str, Any],
        defaults: Mapping[str, Any] | None = None,
    ) -> tuple[T, bool]:
        """Get first row by lookup fields or create a new object.

        Returns:
            `(obj, created)` where `created=True` means a new row was inserted.
        """

        if not lookup:
            raise ValueError("lookup must not be empty.")

        conditions = [C.eq(key, value) for key, value in lookup.items()]
        found = self.list(where=conditions, limit=1)
        if found:
            return found[0], False

        payload: dict[str, Any] = dict(lookup)
        if defaults:
            payload.update(defaults)

        obj = self.model(**payload)
        self.insert(obj)
        return obj, True
