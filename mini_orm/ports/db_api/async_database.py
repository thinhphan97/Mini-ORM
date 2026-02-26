"""Async DB adapter implementation for the core async database port."""

from __future__ import annotations

import contextlib
import inspect
from typing import Any, Mapping

from ...core._async_utils import _maybe_await
from ...core.types import MaybeRow, QueryParams, RowMapping, Rows
from .dialects import Dialect
from .pool_connector import PoolConnector


class AsyncDatabase:
    """Async database wrapper that normalizes execute and row mapping behavior."""

    def __init__(self, conn: Any | PoolConnector, dialect: Dialect):
        """Create async database adapter.

        Args:
            conn: Async (or sync) DB connection object or `PoolConnector`.
            dialect: Concrete SQL dialect instance.
        """

        self._pool: PoolConnector | None = None
        self._closed = False
        if isinstance(conn, PoolConnector):
            self._pool = conn
            self.conn = conn.acquire()
        else:
            self.conn = conn
        self.dialect = dialect

    def _should_begin_sqlite_transaction(self) -> bool:
        if getattr(self.dialect, "name", "").lower() != "sqlite":
            return False
        if getattr(self.conn, "isolation_level", None) is not None:
            return False
        return not bool(getattr(self.conn, "in_transaction", False))

    @contextlib.asynccontextmanager
    async def transaction(self):
        """Provide async commit/rollback transaction scope."""

        if self._should_begin_sqlite_transaction():
            await _maybe_await(self.conn.execute("BEGIN"))
        try:
            yield
        except BaseException:
            await _maybe_await(self.conn.rollback())
            raise
        else:
            await _maybe_await(self.conn.commit())

    async def execute(self, sql: str, params: QueryParams = None) -> Any:
        """Execute SQL with optional parameters and return cursor."""

        cur = await _maybe_await(self.conn.cursor())
        try:
            if params is None:
                await _maybe_await(cur.execute(sql))
            else:
                await _maybe_await(cur.execute(sql, params))
        except BaseException:
            close = getattr(cur, "close", None)
            if callable(close):
                await _maybe_await(close())
            raise
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
            # mini_orm targets Python 3.10+, where zip(..., strict=True) is available.
            return dict(zip(cols, row, strict=True))

        try:
            return dict(row)
        except (TypeError, ValueError):
            pass

        raise TypeError(f"Unsupported row type: {type(row)}")

    async def fetchone(self, sql: str, params: QueryParams = None) -> MaybeRow:
        """Execute query and return one normalized row mapping."""

        cur = await self.execute(sql, params)
        try:
            row = await _maybe_await(cur.fetchone())
            if row is None:
                return None
            return self._row_to_mapping(cur, row)
        finally:
            close = getattr(cur, "close", None)
            if callable(close):
                await _maybe_await(close())

    async def fetchall(self, sql: str, params: QueryParams = None) -> Rows:
        """Execute query and return all rows as normalized mappings."""

        cur = await self.execute(sql, params)
        try:
            rows = await _maybe_await(cur.fetchall())
            return [self._row_to_mapping(cur, r) for r in rows]
        finally:
            close = getattr(cur, "close", None)
            if callable(close):
                await _maybe_await(close())

    def close(self, *, close_pool: bool = False) -> None:
        """Release/close underlying connection.

        Args:
            close_pool: Also close pooled connector when this adapter uses `PoolConnector`.
        """

        if self._closed:
            if close_pool and self._pool is not None:
                self._pool.close()
            return
        self._closed = True
        if self._pool is not None:
            self._pool.release(self.conn)
            if close_pool:
                self._pool.close()
            return
        close = getattr(self.conn, "close", None)
        if callable(close):
            close_method = getattr(type(self.conn), "close", None)
            if inspect.iscoroutinefunction(close_method):
                return
            close()

    async def aclose(self, *, close_pool: bool = False) -> None:
        """Async release/close underlying connection.

        Args:
            close_pool: Also close pooled connector when this adapter uses `PoolConnector`.
        """

        if self._closed:
            if close_pool and self._pool is not None:
                self._pool.close()
            return
        self._closed = True
        if self._pool is not None:
            self._pool.release(self.conn)
            if close_pool:
                self._pool.close()
            return
        close = getattr(self.conn, "close", None)
        if callable(close):
            await _maybe_await(close())

    async def __aenter__(self) -> AsyncDatabase:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()
