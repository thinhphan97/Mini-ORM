"""Async Postgres example (optional dependency + running server)."""

from __future__ import annotations

import asyncio
import importlib
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

# Allow running this script directly from repository root.
PROJECT_ROOT = next(
    (parent for parent in Path(__file__).resolve().parents if (parent / "mini_orm").exists()),
    None,
)
if PROJECT_ROOT and str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from mini_orm import (
    AsyncDatabase,
    AsyncRepository,
    PostgresDialect,
    apply_schema_async,
)


def _load_connect() -> Any:
    for module_name in ("psycopg", "psycopg2"):
        try:
            module = importlib.import_module(module_name)
        except (ModuleNotFoundError, ImportError):
            continue
        connect = getattr(module, "connect", None)
        if connect is not None:
            return connect
    return None


@dataclass
class User:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = field(default="")
    age: Optional[int] = None


async def main() -> None:
    connect = _load_connect()
    if connect is None:
        print("Postgres async example skipped: psycopg/psycopg2 not installed.")
        print("Install dependency: pip install psycopg")
        return

    password = os.getenv(
        "MINI_ORM_PG_PASSWORD",
        os.getenv("PGPASSWORD", os.getenv("POSTGRES_PASSWORD", "password")),
    )
    params = {
        "host": os.getenv("MINI_ORM_PG_HOST", os.getenv("PGHOST", "localhost")),
        "port": int(os.getenv("MINI_ORM_PG_PORT", os.getenv("PGPORT", "5432"))),
        "user": os.getenv("MINI_ORM_PG_USER", os.getenv("PGUSER", "postgres")),
        "password": password,
        "dbname": os.getenv("MINI_ORM_PG_DATABASE", os.getenv("PGDATABASE", "postgres")),
    }

    try:
        conn = connect(**params)
    except Exception as exc:
        print("Postgres async example skipped:", exc)
        return

    # This wraps a synchronous psycopg/psycopg2 connection in AsyncDatabase.
    # For true async network I/O with psycopg3, use psycopg.AsyncConnection.connect(...).
    db = AsyncDatabase(conn, PostgresDialect())
    repo = AsyncRepository[User](db, User)

    try:
        async with db.transaction():
            await db.execute('DROP TABLE IF EXISTS "user";')
        await apply_schema_async(db, User)

        async with db.transaction():
            alice = await repo.insert(User(email="alice@example.com", age=25))
            bob = await repo.insert(User(email="bob@example.com", age=30))
            bob.age = 31
            await repo.update(bob)

        print("Inserted:", alice, bob)
        print("All users:", await repo.list())

        async with db.transaction():
            await repo.delete(alice)
        print("After delete:", await repo.list())
    finally:
        conn.close()


if __name__ == "__main__":
    asyncio.run(main())
