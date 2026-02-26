"""Simple thread-safe DB-API connection pool."""

from __future__ import annotations

import contextlib
import threading
import time
from collections.abc import Callable, Iterator
from typing import Any
from urllib.parse import parse_qs, urlparse


class PoolConnector:
    """Small fixed-size pool for DB-API connection objects."""

    def __init__(
        self,
        connect: Callable[..., Any],
        *connect_args: Any,
        max_size: int = 5,
        transaction_guard: str = "rollback",
        strict_pool: bool = False,
        reset_session: bool = True,
        session_reset_hook: Callable[[Any], None] | None = None,
        enforce_sqlite_thread_check: bool = True,
        **connect_kwargs: Any,
    ):
        if max_size < 1:
            raise ValueError("max_size must be >= 1.")
        self._validate_transaction_guard(transaction_guard)
        normalized_connect, normalized_args, normalized_kwargs = self._normalize_connect_input(
            connect,
            connect_args,
            connect_kwargs,
        )
        self._validate_sqlite_pool_settings(
            normalized_connect,
            normalized_args,
            normalized_kwargs,
            max_size=max_size,
            enforce_thread_check=enforce_sqlite_thread_check,
        )

        self._connect = connect
        self._connect_args = connect_args
        self._connect_kwargs = connect_kwargs
        self._max_size = max_size
        self._transaction_guard = transaction_guard
        self._strict_pool = strict_pool
        self._reset_session = reset_session
        self._session_reset_hook = session_reset_hook

        self._idle: list[Any] = []
        self._borrowed_ids: set[int] = set()
        self._known_ids: set[int] = set()
        self._creating = 0
        self._in_use = 0
        self._closed = False
        self._condition = threading.Condition()

    def _validate_transaction_guard(self, transaction_guard: str) -> None:
        if transaction_guard not in {"rollback", "raise", "ignore", "discard"}:
            raise ValueError(
                "transaction_guard must be one of: "
                "'rollback', 'raise', 'ignore', 'discard'."
            )

    def _validate_sqlite_pool_settings(
        self,
        connect: Callable[..., Any],
        connect_args: tuple[Any, ...],
        connect_kwargs: dict[str, Any],
        *,
        max_size: int,
        enforce_thread_check: bool,
    ) -> None:
        if max_size <= 1:
            return

        if not self._is_sqlite_connect(connect):
            return

        check_same_thread = self._extract_sqlite_check_same_thread(connect_args, connect_kwargs)
        if enforce_thread_check and check_same_thread:
            raise ValueError(
                "PoolConnector detected sqlite with max_size > 1 and check_same_thread=True. "
                "Use check_same_thread=False for pooled multi-thread usage."
            )

        database = self._extract_sqlite_database(connect_args, connect_kwargs)
        if not isinstance(database, str):
            return

        uri = self._extract_sqlite_uri_flag(connect_args, connect_kwargs)
        if not self._is_private_sqlite_memory_database(database, uri=uri):
            return

        raise ValueError(
            "PoolConnector detected sqlite private in-memory database with max_size > 1. "
            "Use max_size=1, or shared-memory URI "
            '(e.g. "file:miniorm?mode=memory&cache=shared", uri=True).'
        )

    def _normalize_connect_input(
        self,
        connect: Callable[..., Any],
        connect_args: tuple[Any, ...],
        connect_kwargs: dict[str, Any],
    ) -> tuple[Callable[..., Any], tuple[Any, ...], dict[str, Any]]:
        base_connect = getattr(connect, "func", None)
        base_args = getattr(connect, "args", None)
        base_kwargs = getattr(connect, "keywords", None)
        if base_connect is None or not isinstance(base_args, tuple):
            return connect, connect_args, connect_kwargs

        merged_args = base_args + connect_args
        merged_kwargs = dict(base_kwargs or {})
        merged_kwargs.update(connect_kwargs)
        return base_connect, merged_args, merged_kwargs

    def _is_sqlite_connect(self, connect: Callable[..., Any]) -> bool:
        module_name = getattr(connect, "__module__", "")
        return module_name.startswith("sqlite3") or module_name.startswith("_sqlite3")

    def _extract_sqlite_database(
        self,
        connect_args: tuple[Any, ...],
        connect_kwargs: dict[str, Any],
    ) -> Any:
        if connect_args:
            return connect_args[0]
        return connect_kwargs.get("database")

    def _extract_sqlite_uri_flag(
        self,
        connect_args: tuple[Any, ...],
        connect_kwargs: dict[str, Any],
    ) -> bool:
        if "uri" in connect_kwargs:
            return bool(connect_kwargs["uri"])
        # sqlite3.connect positional index for `uri` is 7.
        if len(connect_args) >= 8:
            return bool(connect_args[7])
        return False

    def _extract_sqlite_check_same_thread(
        self,
        connect_args: tuple[Any, ...],
        connect_kwargs: dict[str, Any],
    ) -> bool:
        if "check_same_thread" in connect_kwargs:
            return bool(connect_kwargs["check_same_thread"])
        if len(connect_args) >= 5:
            return bool(connect_args[4])
        return True

    def _is_private_sqlite_memory_database(self, database: str, *, uri: bool) -> bool:
        if database == ":memory:":
            return True
        if not uri:
            return False
        if not database.startswith("file:"):
            return False

        lowered = database.lower()
        if lowered.startswith("file::memory:"):
            return "cache=shared" not in lowered

        parsed = urlparse(database)
        query = parse_qs(parsed.query)
        mode = (query.get("mode", [""])[0] or "").lower()
        cache = (query.get("cache", [""])[0] or "").lower()
        return mode == "memory" and cache != "shared"

    @property
    def max_size(self) -> int:
        return self._max_size

    def acquire(self, timeout: float | None = None) -> Any:
        """Borrow one connection from the pool."""

        with self._condition:
            deadline = None if timeout is None else (time.monotonic() + timeout)
            should_create = False

            while True:
                self._ensure_open()
                if self._idle:
                    conn = self._idle.pop()
                    conn_id = id(conn)
                    self._borrowed_ids.add(conn_id)
                    self._in_use += 1
                    return conn

                total_slots = len(self._known_ids) + self._creating
                if total_slots < self._max_size:
                    self._creating += 1
                    self._in_use += 1
                    should_create = True
                    break

                if timeout is None:
                    self._condition.wait()
                    continue

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("Timed out waiting for a pooled DB connection.")
                self._condition.wait(remaining)

        if should_create:
            try:
                conn = self._connect(*self._connect_args, **self._connect_kwargs)
            except BaseException:
                with self._condition:
                    self._creating -= 1
                    self._in_use -= 1
                    self._condition.notify()
                raise

            should_close_because_closed = False
            with self._condition:
                self._creating -= 1
                conn_id = id(conn)
                self._known_ids.add(conn_id)
                self._borrowed_ids.add(conn_id)
                if self._closed:
                    self._borrowed_ids.discard(conn_id)
                    self._known_ids.discard(conn_id)
                    self._in_use -= 1
                    self._condition.notify()
                    should_close_because_closed = True
            if should_close_because_closed:
                self._close_connection(conn)
                raise RuntimeError("PoolConnector is closed.")
            return conn

        raise RuntimeError("Unexpected pool acquire state.")

    def release(self, conn: Any) -> None:
        """Return one borrowed connection to the pool."""

        conn_id = id(conn)
        with self._condition:
            if conn_id not in self._borrowed_ids:
                raise ValueError("Connection was not acquired from this pool or already released.")

            self._borrowed_ids.remove(conn_id)
            self._in_use -= 1
            self._condition.notify()

        cleanup_error: Exception | None = None
        should_close = False
        try:
            in_transaction = self._connection_in_transaction(conn)
            if in_transaction:
                self._apply_transaction_guard(conn)
                if self._strict_pool or self._transaction_guard == "discard":
                    should_close = True
            should_skip_session_reset = should_close or (
                in_transaction and self._transaction_guard == "ignore"
            )
            if self._reset_session and not should_skip_session_reset:
                self._reset_connection_session(conn)
        except Exception as exc:
            cleanup_error = exc
            should_close = True

        with self._condition:
            if self._closed or should_close:
                self._known_ids.discard(conn_id)
                should_close = True
            else:
                self._idle.append(conn)
            self._condition.notify()

        if should_close:
            self._close_connection(conn)
        if cleanup_error is not None:
            raise RuntimeError(
                "Failed to clean pooled DB connection before returning it."
            ) from cleanup_error

    @contextlib.contextmanager
    def connection(self, timeout: float | None = None) -> Iterator[Any]:
        """Borrow and auto-release one connection with a context manager."""

        conn = self.acquire(timeout=timeout)
        try:
            yield conn
        finally:
            self.release(conn)

    def close(self) -> None:
        """Close all idle pooled connections and prevent future acquire."""

        with self._condition:
            if self._closed:
                return

            self._closed = True
            idle = list(self._idle)
            self._idle.clear()
            for conn in idle:
                self._known_ids.discard(id(conn))
            self._condition.notify_all()

        for conn in idle:
            self._close_connection(conn)

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("PoolConnector is closed.")

    def _close_connection(self, conn: Any) -> None:
        close = getattr(conn, "close", None)
        if callable(close):
            close()

    def _connection_in_transaction(self, conn: Any) -> bool:
        in_tx = getattr(conn, "in_transaction", None)
        if isinstance(in_tx, bool):
            return in_tx
        if callable(in_tx):
            try:
                value = in_tx()
                if isinstance(value, bool):
                    return value
            except Exception:
                pass

        info = getattr(conn, "info", None)
        tx_status = getattr(info, "transaction_status", None)
        if tx_status is not None:
            # psycopg3: 0 = idle.
            return tx_status != 0

        module_name = type(conn).__module__.lower()
        status = getattr(conn, "status", None)
        if status is not None and "psycopg2" in module_name:
            # psycopg2: STATUS_READY == 1 means idle.
            return status != 1

        return False

    def _apply_transaction_guard(self, conn: Any) -> None:
        if self._transaction_guard == "ignore":
            return
        if self._transaction_guard == "raise":
            raise RuntimeError(
                "Connection has an active transaction during release(). "
                "Commit/rollback before returning it to pool."
            )
        rollback = getattr(conn, "rollback", None)
        if not callable(rollback):
            raise RuntimeError("Connection has no rollback() for transaction cleanup.")
        rollback()

    def _reset_connection_session(self, conn: Any) -> None:
        if self._session_reset_hook is not None:
            self._session_reset_hook(conn)
            return

        statements = self._default_reset_statements(conn)
        if not statements:
            return

        cur = conn.cursor()
        try:
            for sql in statements:
                cur.execute(sql)
        finally:
            close = getattr(cur, "close", None)
            if callable(close):
                close()

        commit = getattr(conn, "commit", None)
        if callable(commit):
            commit()

    def _default_reset_statements(self, conn: Any) -> list[str]:
        module_name = type(conn).__module__.lower()
        if "psycopg" in module_name:
            return ["RESET ALL", "UNLISTEN *", "DEALLOCATE ALL"]
        if "mysql" in module_name or "pymysql" in module_name or "mysqldb" in module_name:
            return [
                "SET SESSION autocommit = 1",
                "SET SESSION sql_mode = DEFAULT",
                "SET SESSION time_zone = DEFAULT",
            ]
        return []
