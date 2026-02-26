from __future__ import annotations

import os
import sqlite3
import tempfile
import threading
import time
import unittest
from functools import partial

from mini_orm.ports.db_api.database import Database
from mini_orm.ports.db_api.dialects import Dialect, MySQLDialect, PostgresDialect, SQLiteDialect
from mini_orm.ports.db_api.pool_connector import PoolConnector


class _DummyCursor:
    def __init__(self, description=None, lastrowid=None):
        self.description = description
        self.lastrowid = lastrowid


class _QmarkDialect(Dialect):
    paramstyle = "qmark"


class _InvalidDialect(Dialect):
    paramstyle = "invalid"


class _FakeCleanupCursor:
    def __init__(self, conn):
        self._conn = conn
        self.closed = False

    def execute(self, sql, _params=None):  # noqa: ANN001,ANN201
        self._conn.executed_sql.append(sql)
        return None

    def close(self) -> None:
        self.closed = True


class _FakeBaseConn:
    def __init__(self, *, in_transaction: bool = False, has_rollback: bool = True):
        self.in_transaction = in_transaction
        self.executed_sql: list[str] = []
        self.rollback_calls = 0
        self.commit_calls = 0
        self.close_calls = 0
        self._has_rollback = has_rollback

    def cursor(self) -> _FakeCleanupCursor:
        return _FakeCleanupCursor(self)

    def rollback(self) -> None:
        if not self._has_rollback:
            raise AttributeError("rollback is unavailable")
        self.rollback_calls += 1
        self.in_transaction = False

    def commit(self) -> None:
        self.commit_calls += 1
        self.in_transaction = False

    def close(self) -> None:
        self.close_calls += 1


class _FakePgConn(_FakeBaseConn):
    __module__ = "psycopg"


class _FakeMySQLConn(_FakeBaseConn):
    __module__ = "pymysql.connections"


class _FakePsycopgInfo:
    def __init__(self, transaction_status: int):
        self.transaction_status = transaction_status


class _FakePsycopgInfoConn(_FakeBaseConn):
    __module__ = "psycopg"

    def __init__(self, *, transaction_status: int):
        super().__init__(in_transaction=False)
        self.in_transaction = None
        self.info = _FakePsycopgInfo(transaction_status)


class _FakePsycopg2StatusConn(_FakeBaseConn):
    __module__ = "psycopg2.extensions"

    def __init__(self, *, status: int):
        super().__init__(in_transaction=False)
        self.in_transaction = None
        self.status = status


class DialectTests(unittest.TestCase):
    def test_builtin_dialect_properties(self) -> None:
        self.assertEqual(SQLiteDialect().placeholder("x"), ":x")
        self.assertEqual(PostgresDialect().placeholder("x"), "%s")
        self.assertEqual(MySQLDialect().placeholder("x"), "%s")
        self.assertEqual(_QmarkDialect().placeholder("x"), "?")
        self.assertEqual(SQLiteDialect().auto_pk_sql("id"), '"id" INTEGER PRIMARY KEY')
        self.assertEqual(PostgresDialect().auto_pk_sql("id"), '"id" SERIAL PRIMARY KEY')
        self.assertEqual(
            MySQLDialect().auto_pk_sql("id"), "`id` INT AUTO_INCREMENT PRIMARY KEY"
        )

    def test_invalid_paramstyle_raises(self) -> None:
        with self.assertRaises(ValueError):
            _InvalidDialect().placeholder("x")

    def test_returning_clause_and_lastrowid(self) -> None:
        sqlite_dialect = SQLiteDialect()
        mysql_dialect = MySQLDialect()

        self.assertEqual(sqlite_dialect.returning_clause("id"), ' RETURNING "id"')
        self.assertEqual(mysql_dialect.returning_clause("id"), "")

        cursor = _DummyCursor(lastrowid=99)
        self.assertEqual(sqlite_dialect.get_lastrowid(cursor), 99)


class DatabaseAdapterTests(unittest.TestCase):
    def test_execute_fetchone_fetchall(self) -> None:
        conn = sqlite3.connect(":memory:")
        db = Database(conn, SQLiteDialect())
        db.execute('CREATE TABLE "t" ("id" INTEGER, "name" TEXT);')
        db.execute('INSERT INTO "t" ("id", "name") VALUES (:id, :name);', {"id": 1, "name": "a"})
        db.execute('INSERT INTO "t" ("id", "name") VALUES (:id, :name);', {"id": 2, "name": "b"})

        row = db.fetchone('SELECT * FROM "t" WHERE "id" = :id;', {"id": 1})
        rows = db.fetchall('SELECT * FROM "t" ORDER BY "id" ASC;')

        self.assertEqual(row["name"], "a")
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[1]["name"], "b")
        conn.close()

    def test_row_factory_mapping_is_supported(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        db = Database(conn, SQLiteDialect())
        db.execute('CREATE TABLE "t" ("id" INTEGER);')
        db.execute('INSERT INTO "t" ("id") VALUES (1);')
        row = db.fetchone('SELECT * FROM "t";')
        self.assertEqual(row["id"], 1)
        conn.close()

    def test_transaction_rolls_back_on_error(self) -> None:
        conn = sqlite3.connect(":memory:")
        db = Database(conn, SQLiteDialect())
        db.execute('CREATE TABLE "t" ("id" INTEGER);')

        with self.assertRaises(RuntimeError):
            with db.transaction():
                db.execute('INSERT INTO "t" ("id") VALUES (1);')
                raise RuntimeError("boom")

        count = db.fetchone('SELECT COUNT(*) AS "count" FROM "t";')
        self.assertEqual(count["count"], 0)
        conn.close()

    def test_row_to_mapping_tuple_without_description_raises(self) -> None:
        conn = sqlite3.connect(":memory:")
        db = Database(conn, SQLiteDialect())
        with self.assertRaises(TypeError):
            db._row_to_mapping(_DummyCursor(description=None), (1,))  # noqa: SLF001
        conn.close()

    def test_row_to_mapping_fallback_dict_and_unsupported_type(self) -> None:
        conn = sqlite3.connect(":memory:")
        db = Database(conn, SQLiteDialect())

        mapped = db._row_to_mapping(_DummyCursor(), {("id", 1)})  # noqa: SLF001
        self.assertEqual(mapped["id"], 1)

        with self.assertRaises(TypeError):
            db._row_to_mapping(_DummyCursor(), 12345)  # noqa: SLF001
        conn.close()

    def test_database_can_use_pool_connector(self) -> None:
        pool = PoolConnector(sqlite3.connect, ":memory:", max_size=1)
        db = Database(pool, SQLiteDialect())
        try:
            db.execute('CREATE TABLE "t" ("id" INTEGER, "name" TEXT);')
            db.execute(
                'INSERT INTO "t" ("id", "name") VALUES (:id, :name);',
                {"id": 1, "name": "pool"},
            )
            row = db.fetchone('SELECT * FROM "t" WHERE "id" = :id;', {"id": 1})
            self.assertEqual(row["name"], "pool")
        finally:
            db.close()
            pool.close()

    def test_database_close_returns_connection_to_pool(self) -> None:
        pool = PoolConnector(sqlite3.connect, ":memory:", max_size=1)
        db1 = Database(pool, SQLiteDialect())
        conn1 = db1.conn
        db1.close()
        with self.assertRaises(RuntimeError):
            db1.execute('SELECT 1;')

        db2 = Database(pool, SQLiteDialect())
        try:
            self.assertIs(db2.conn, conn1)
        finally:
            db2.close()
            pool.close()

    def test_database_close_with_close_pool_true_closes_pool(self) -> None:
        pool = PoolConnector(sqlite3.connect, ":memory:", max_size=1)
        db = Database(pool, SQLiteDialect())
        db.close(close_pool=True)
        with self.assertRaises(RuntimeError):
            db.execute('SELECT 1;')
        with self.assertRaises(RuntimeError):
            pool.acquire()


class PoolConnectorTests(unittest.TestCase):
    def _temp_db_path(self) -> str:
        fd, db_path = tempfile.mkstemp(prefix="pool_guard_", suffix=".db")
        os.close(fd)
        self.addCleanup(lambda: os.path.exists(db_path) and os.remove(db_path))
        return db_path

    def test_acquire_release_reuses_connection(self) -> None:
        created = 0

        def _factory() -> sqlite3.Connection:
            nonlocal created
            created += 1
            return sqlite3.connect(":memory:")

        pool = PoolConnector(_factory, max_size=1)
        first = pool.acquire()
        pool.release(first)
        second = pool.acquire()
        self.assertIs(first, second)
        pool.release(second)
        pool.close()
        self.assertEqual(created, 1)

    def test_max_size_zero_raises(self) -> None:
        with self.assertRaises(ValueError):
            PoolConnector(sqlite3.connect, ":memory:", max_size=0)

    def test_invalid_transaction_guard_raises(self) -> None:
        with self.assertRaises(ValueError):
            PoolConnector(sqlite3.connect, ":memory:", transaction_guard="invalid")

    def test_acquire_timeout_raises_when_pool_is_exhausted(self) -> None:
        pool = PoolConnector(sqlite3.connect, ":memory:", max_size=1)
        conn = pool.acquire()
        try:
            with self.assertRaises(TimeoutError):
                pool.acquire(timeout=0.01)
        finally:
            pool.release(conn)
            pool.close()

    def test_acquire_factory_error_does_not_poison_pool_state(self) -> None:
        calls = 0

        def _factory() -> _FakeBaseConn:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("connect failed")
            return _FakeBaseConn(in_transaction=False)

        pool = PoolConnector(_factory, max_size=1, reset_session=False)
        with self.assertRaises(RuntimeError):
            pool.acquire()

        conn = pool.acquire()
        pool.release(conn)
        pool.close()
        self.assertEqual(calls, 2)

    def test_acquire_respects_max_size_under_concurrent_creators(self) -> None:
        create_gate = threading.Event()
        first_started = threading.Event()
        created = 0

        def _factory() -> _FakeBaseConn:
            nonlocal created
            created += 1
            if created == 1:
                first_started.set()
            create_gate.wait(0.5)
            return _FakeBaseConn(in_transaction=False)

        pool = PoolConnector(_factory, max_size=1, reset_session=False)
        acquired: list[_FakeBaseConn] = []
        second_error: list[type[BaseException]] = []

        def _first_worker() -> None:
            conn = pool.acquire(timeout=1)
            acquired.append(conn)

        def _second_worker() -> None:
            try:
                pool.acquire(timeout=0.05)
            except BaseException as exc:  # noqa: BLE001
                second_error.append(type(exc))

        t1 = threading.Thread(target=_first_worker)
        t1.start()
        self.assertTrue(first_started.wait(0.2))

        t2 = threading.Thread(target=_second_worker)
        t2.start()
        t2.join(timeout=1)
        self.assertFalse(t2.is_alive())

        create_gate.set()
        t1.join(timeout=1)
        self.assertFalse(t1.is_alive())
        self.assertEqual(created, 1)
        self.assertEqual(second_error, [TimeoutError])

        if acquired:
            pool.release(acquired[0])
        pool.close()

    def test_acquire_after_close_raises(self) -> None:
        pool = PoolConnector(sqlite3.connect, ":memory:", max_size=1)
        pool.close()
        with self.assertRaises(RuntimeError):
            pool.acquire()

    def test_waiting_acquire_is_unblocked_when_pool_closes(self) -> None:
        pool = PoolConnector(_FakeBaseConn, max_size=1, reset_session=False)
        conn = pool.acquire()
        errors: list[type[BaseException]] = []

        def _waiter() -> None:
            try:
                pool.acquire()
            except BaseException as exc:  # noqa: BLE001
                errors.append(type(exc))

        waiter = threading.Thread(target=_waiter)
        waiter.start()
        time.sleep(0.05)
        pool.close()
        # Intentional ordering: join waiter first, then release-after-close.
        # Related behavior assertion is covered in
        # `test_release_after_pool_close_closes_borrowed_connection`.
        waiter.join(timeout=1)
        self.assertFalse(waiter.is_alive())
        self.assertEqual(errors, [RuntimeError])
        pool.release(conn)

    def test_connection_context_manager_releases_connection(self) -> None:
        pool = PoolConnector(sqlite3.connect, ":memory:", max_size=1)
        with pool.connection() as borrowed:
            self.assertIsNotNone(borrowed)
        reacquired = pool.acquire()
        self.assertIs(reacquired, borrowed)
        pool.release(reacquired)
        pool.close()

    def test_release_unknown_connection_raises(self) -> None:
        pool = PoolConnector(sqlite3.connect, ":memory:", max_size=1)
        outside_conn = sqlite3.connect(":memory:")
        try:
            with self.assertRaises(ValueError):
                pool.release(outside_conn)
        finally:
            outside_conn.close()
            pool.close()

    def test_sqlite_private_memory_with_max_size_gt_1_raises(self) -> None:
        with self.assertRaises(ValueError):
            PoolConnector(sqlite3.connect, ":memory:", max_size=2)

    def test_sqlite_memory_uri_without_shared_cache_with_max_size_gt_1_raises(self) -> None:
        with self.assertRaises(ValueError):
            PoolConnector(
                sqlite3.connect,
                "file:private_memdb?mode=memory",
                uri=True,
                max_size=2,
            )

    def test_sqlite_shared_memory_uri_allows_max_size_gt_1(self) -> None:
        db_name = f"file:shared_memdb_{time.monotonic_ns()}?mode=memory&cache=shared"
        pool = PoolConnector(
            sqlite3.connect,
            db_name,
            uri=True,
            check_same_thread=False,
            max_size=2,
        )
        conn1 = pool.acquire()
        conn2 = pool.acquire()
        try:
            conn1.execute('CREATE TABLE "t" ("id" INTEGER);')
            conn1.execute('INSERT INTO "t" ("id") VALUES (1);')
            conn1.commit()
            row = conn2.execute('SELECT COUNT(*) FROM "t";').fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row[0], 1)
        finally:
            pool.release(conn2)
            pool.release(conn1)
            pool.close()

    def test_partial_sqlite_connect_still_validates_memory_pool(self) -> None:
        sqlite_partial = partial(sqlite3.connect, ":memory:")
        with self.assertRaises(ValueError):
            PoolConnector(sqlite_partial, max_size=2)

    def test_sqlite_uri_positional_argument_index_is_handled(self) -> None:
        # uri=False provided positionally should not trigger memory URI guard.
        PoolConnector(
            sqlite3.connect,
            "file:not_uri_memory?mode=memory",
            5.0,
            0,
            None,
            False,
            sqlite3.Connection,
            128,
            False,
            max_size=2,
        )

    def test_sqlite_max_size_gt_1_requires_check_same_thread_false(self) -> None:
        db_path = self._temp_db_path()
        with self.assertRaises(ValueError):
            PoolConnector(sqlite3.connect, db_path, max_size=2)

    def test_sqlite_max_size_gt_1_allows_check_same_thread_false(self) -> None:
        db_name = self._temp_db_path()
        pool = PoolConnector(sqlite3.connect, db_name, check_same_thread=False, max_size=2)
        conn1 = pool.acquire()
        conn2 = pool.acquire()
        pool.release(conn2)
        pool.release(conn1)
        pool.close()

    def test_can_disable_sqlite_thread_check_guard(self) -> None:
        db_path = self._temp_db_path()
        pool = PoolConnector(
            sqlite3.connect,
            db_path,
            max_size=2,
            enforce_sqlite_thread_check=False,
        )
        conn = pool.acquire()
        pool.release(conn)
        pool.close()

    def test_dirty_transaction_is_rolled_back_by_default(self) -> None:
        created = 0

        def _factory() -> _FakeBaseConn:
            nonlocal created
            created += 1
            return _FakeBaseConn(in_transaction=False)

        pool = PoolConnector(_factory, max_size=1, reset_session=False)
        conn = pool.acquire()
        conn.in_transaction = True
        pool.release(conn)

        self.assertEqual(conn.rollback_calls, 1)
        reused = pool.acquire()
        self.assertIs(reused, conn)
        pool.release(reused)
        pool.close()
        self.assertEqual(created, 1)

    def test_transaction_guard_raise_discards_dirty_connection(self) -> None:
        created = 0

        def _factory() -> _FakeBaseConn:
            nonlocal created
            created += 1
            return _FakeBaseConn(in_transaction=False)

        pool = PoolConnector(
            _factory,
            max_size=1,
            transaction_guard="raise",
            reset_session=False,
        )
        conn = pool.acquire()
        conn.in_transaction = True

        with self.assertRaises(RuntimeError):
            pool.release(conn)
        self.assertEqual(conn.close_calls, 1)

        conn2 = pool.acquire()
        self.assertIsNot(conn2, conn)
        pool.release(conn2)
        pool.close()
        self.assertEqual(created, 2)

    def test_strict_pool_discards_dirty_connection_even_after_rollback(self) -> None:
        created = 0

        def _factory() -> _FakeBaseConn:
            nonlocal created
            created += 1
            return _FakeBaseConn(in_transaction=False)

        pool = PoolConnector(
            _factory,
            max_size=1,
            strict_pool=True,
            reset_session=False,
        )
        conn = pool.acquire()
        conn.in_transaction = True
        pool.release(conn)

        self.assertEqual(conn.rollback_calls, 1)
        self.assertEqual(conn.close_calls, 1)
        conn2 = pool.acquire()
        self.assertIsNot(conn2, conn)
        pool.release(conn2)
        pool.close()
        self.assertEqual(created, 2)

    def test_transaction_guard_discard_discards_dirty_connection(self) -> None:
        created = 0

        def _factory() -> _FakeBaseConn:
            nonlocal created
            created += 1
            return _FakeBaseConn(in_transaction=False)

        pool = PoolConnector(
            _factory,
            max_size=1,
            transaction_guard="discard",
            reset_session=False,
        )
        conn = pool.acquire()
        conn.in_transaction = True
        pool.release(conn)

        self.assertEqual(conn.rollback_calls, 1)
        self.assertEqual(conn.close_calls, 1)
        conn2 = pool.acquire()
        self.assertIsNot(conn2, conn)
        pool.release(conn2)
        pool.close()
        self.assertEqual(created, 2)

    def test_transaction_guard_ignore_keeps_dirty_connection(self) -> None:
        pool = PoolConnector(_FakeBaseConn, max_size=1, transaction_guard="ignore", reset_session=False)
        conn = pool.acquire()
        conn.in_transaction = True
        pool.release(conn)

        self.assertEqual(conn.rollback_calls, 0)
        reused = pool.acquire()
        self.assertIs(reused, conn)
        self.assertTrue(reused.in_transaction)
        pool.release(reused)
        pool.close()

    def test_transaction_guard_ignore_skips_session_reset(self) -> None:
        pool = PoolConnector(_FakePgConn, max_size=1, transaction_guard="ignore")
        conn = pool.acquire()
        conn.in_transaction = True
        pool.release(conn)

        self.assertEqual(conn.rollback_calls, 0)
        self.assertEqual(conn.executed_sql, [])
        self.assertEqual(conn.commit_calls, 0)
        pool.close()

    def test_session_reset_hook_is_called_on_release(self) -> None:
        calls: list[_FakeBaseConn] = []

        def _factory() -> _FakeBaseConn:
            return _FakeBaseConn(in_transaction=False)

        def _reset(conn) -> None:  # noqa: ANN001,ANN202
            calls.append(conn)

        pool = PoolConnector(_factory, max_size=1, session_reset_hook=_reset)
        conn = pool.acquire()
        pool.release(conn)
        self.assertEqual(calls, [conn])
        pool.close()

    def test_session_reset_hook_error_discards_connection(self) -> None:
        created = 0
        reset_calls = 0

        def _factory() -> _FakeBaseConn:
            nonlocal created
            created += 1
            return _FakeBaseConn(in_transaction=False)

        def _reset(_conn) -> None:  # noqa: ANN001,ANN202
            nonlocal reset_calls
            reset_calls += 1
            if reset_calls == 1:
                raise ValueError("reset failed")

        pool = PoolConnector(_factory, max_size=1, session_reset_hook=_reset)
        conn = pool.acquire()
        with self.assertRaises(RuntimeError):
            pool.release(conn)
        self.assertEqual(conn.close_calls, 1)

        fresh = pool.acquire()
        self.assertIsNot(fresh, conn)
        pool.release(fresh)
        pool.close()
        self.assertEqual(created, 2)

    def test_release_after_pool_close_closes_borrowed_connection(self) -> None:
        pool = PoolConnector(_FakeBaseConn, max_size=1, reset_session=False)
        conn = pool.acquire()
        pool.close()
        pool.release(conn)
        self.assertEqual(conn.close_calls, 1)

    def test_non_sqlite_pool_allows_max_size_gt_1(self) -> None:
        pool = PoolConnector(_FakeBaseConn, max_size=2, reset_session=False)
        conn1 = pool.acquire()
        conn2 = pool.acquire()
        pool.release(conn2)
        pool.release(conn1)
        pool.close()

    def test_dirty_detection_uses_psycopg_info_transaction_status(self) -> None:
        pool = PoolConnector(
            lambda: _FakePsycopgInfoConn(transaction_status=2),
            max_size=1,
            reset_session=False,
        )
        conn = pool.acquire()
        pool.release(conn)
        self.assertEqual(conn.rollback_calls, 1)
        pool.close()

    def test_dirty_detection_uses_psycopg2_status(self) -> None:
        pool = PoolConnector(
            lambda: _FakePsycopg2StatusConn(status=2),
            max_size=1,
            reset_session=False,
        )
        conn = pool.acquire()
        pool.release(conn)
        self.assertEqual(conn.rollback_calls, 1)
        pool.close()

    def test_default_postgres_session_reset_runs_statements(self) -> None:
        pool = PoolConnector(_FakePgConn, max_size=1)
        conn = pool.acquire()
        pool.release(conn)
        self.assertEqual(
            conn.executed_sql,
            ["RESET ALL", "UNLISTEN *", "DEALLOCATE ALL"],
        )
        self.assertEqual(conn.commit_calls, 1)
        pool.close()

    def test_default_mysql_session_reset_runs_statements(self) -> None:
        pool = PoolConnector(_FakeMySQLConn, max_size=1)
        conn = pool.acquire()
        pool.release(conn)
        self.assertEqual(
            conn.executed_sql,
            [
                "SET SESSION autocommit = 1",
                "SET SESSION sql_mode = DEFAULT",
                "SET SESSION time_zone = DEFAULT",
            ],
        )
        self.assertEqual(conn.commit_calls, 1)
        pool.close()


if __name__ == "__main__":
    unittest.main()
