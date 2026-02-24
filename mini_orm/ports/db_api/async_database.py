"""Async DB adapter implementation for the core async database port."""

from __future__ import annotations

import contextlib
import inspect
from typing import Any, Mapping

from ...core.types import MaybeRow, QueryParams, RowMapping, Rows
from .dialects import Dialect


class AsyncDatabase:
    """Async database wrapper that normalizes execute and row mapping behavior."""

    def __init__(self, conn: Any, dialect: Dialect):
        """Create async database adapter.

        Args:
            conn: Async (or sync) DB connection object.
            dialect: Concrete SQL dialect instance.
        """

        self.conn = conn
        self.dialect = dialect

    @contextlib.asynccontextmanager
    async def transaction(self):
        """Provide async commit/rollback transaction scope."""

        try:
            yield
            await _maybe_await(self.conn.commit())
        except Exception:
            await _maybe_await(self.conn.rollback())
            raise

    async def execute(self, sql: str, params: QueryParams = None) -> Any:
        """Execute SQL with optional parameters and return cursor."""

        cur = await _maybe_await(self.conn.cursor())
        if params is None:
            await _maybe_await(cur.execute(sql))
        else:
            await _maybe_await(cur.execute(sql, params))
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

    async def fetchone(self, sql: str, params: QueryParams = None) -> MaybeRow:
        """Execute query and return one normalized row mapping."""

        cur = await self.execute(sql, params)
        row = await _maybe_await(cur.fetchone())
        if row is None:
            return None
        return self._row_to_mapping(cur, row)

    async def fetchall(self, sql: str, params: QueryParams = None) -> Rows:
        """Execute query and return all rows as normalized mappings."""

        cur = await self.execute(sql, params)
        rows = await _maybe_await(cur.fetchall())
        return [self._row_to_mapping(cur, r) for r in rows]


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value
