"""DB-API adapter implementation for the core database port."""

from __future__ import annotations

import contextlib
from typing import Any, Mapping

from ...core.types import MaybeRow, QueryParams, RowMapping, Rows
from .dialects import Dialect


class Database:
    """Thin DB-API wrapper that normalizes execute and row mapping behavior."""

    def __init__(self, conn: Any, dialect: Dialect):
        """Create database adapter.

        Args:
            conn: DB-API connection object.
            dialect: Concrete SQL dialect instance.
        """

        self.conn = conn
        self.dialect = dialect

    def _should_begin_sqlite_transaction(self) -> bool:
        if getattr(self.dialect, "name", "").lower() != "sqlite":
            return False
        if getattr(self.conn, "isolation_level", None) is not None:
            return False
        return not bool(getattr(self.conn, "in_transaction", False))

    @contextlib.contextmanager
    def transaction(self):
        """Provide commit/rollback transaction scope."""

        try:
            if self._should_begin_sqlite_transaction():
                self.conn.execute("BEGIN")
            yield
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise

    def execute(self, sql: str, params: QueryParams = None) -> Any:
        """Execute SQL with optional parameters and return cursor."""

        cur = self.conn.cursor()
        if params is None:
            cur.execute(sql)
        else:
            cur.execute(sql, params)
        return cur

    def _row_to_mapping(self, cursor: Any, row: Any) -> RowMapping:
        """Normalize row object to mapping.

        Supports mapping rows directly and tuple/list rows via
        `cursor.description`.
        """

        if isinstance(row, Mapping):
            return row

        if isinstance(row, (tuple, list)):
            desc = getattr(cursor, "description", None)
            if not desc:
                raise TypeError(
                    "Cursor has no description; cannot map tuple rows to dict."
                )
            cols = [d[0] for d in desc]
            return dict(zip(cols, row))

        try:
            m = dict(row)
            if m:
                return m
        except Exception:
            pass

        raise TypeError(f"Unsupported row type: {type(row)}")

    def fetchone(self, sql: str, params: QueryParams = None) -> MaybeRow:
        """Execute query and return one normalized row mapping."""

        cur = self.execute(sql, params)
        row = cur.fetchone()
        if row is None:
            return None
        return self._row_to_mapping(cur, row)

    def fetchall(self, sql: str, params: QueryParams = None) -> Rows:
        """Execute query and return all rows as normalized mappings."""

        cur = self.execute(sql, params)
        rows = cur.fetchall()
        return [self._row_to_mapping(cur, r) for r in rows]
