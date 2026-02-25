"""Async MySQL example (optional dependency + running server)."""

from __future__ import annotations

import asyncio
import importlib
import os
import re
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

from mini_orm import AsyncDatabase, AsyncRepository, MySQLDialect


def _load_mysql_driver() -> tuple[str, Any] | tuple[None, None]:
    for module_name in ("MySQLdb", "pymysql", "mysql.connector"):
        try:
            module = importlib.import_module(module_name)
        except ImportError:
            continue
        connect = getattr(module, "connect", None)
        if connect is not None:
            return module_name, connect
    return None, None


def _mysql_connect(
    *,
    driver_name: str,
    connect: Any,
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
) -> Any:
    if driver_name == "MySQLdb":
        return connect(
            host=host,
            port=port,
            user=user,
            passwd=password,
            db=database,
            charset="utf8mb4",
        )
    if driver_name == "pymysql":
        return connect(
            host=host,
            port=port,
            user=user,
            password=password,
            db=database,
            charset="utf8mb4",
        )
    return connect(  # mysql.connector
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
    )


@dataclass
class User:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = ""
    age: Optional[int] = None


async def main() -> None:
    driver_name, connect = _load_mysql_driver()
    if connect is None:
        print("MySQL async example skipped: no mysql driver installed.")
        print("Install one of: pip install mysqlclient / pymysql / mysql-connector-python")
        return

    host = os.getenv("MINI_ORM_MYSQL_HOST", os.getenv("MYSQL_HOST", "localhost"))
    port = int(os.getenv("MINI_ORM_MYSQL_PORT", os.getenv("MYSQL_PORT", "3306")))
    user = os.getenv("MINI_ORM_MYSQL_USER", os.getenv("MYSQL_USER", "root"))
    password = os.getenv(
        "MINI_ORM_MYSQL_PASSWORD",
        os.getenv("MYSQL_ROOT_PASSWORD", os.getenv("MYSQL_PASSWORD", "password")),
    )
    database = os.getenv(
        "MINI_ORM_MYSQL_DATABASE",
        os.getenv("MYSQL_DATABASE", "mini_orm_test"),
    )
    if not re.fullmatch(r"[A-Za-z0-9_]+", database):
        raise ValueError(
            "Invalid MINI_ORM_MYSQL_DATABASE/MYSQL_DATABASE value. "
            "Use only letters, numbers, and underscores."
        )
    bootstrap_db = os.getenv("MINI_ORM_MYSQL_BOOTSTRAP_DB", "mysql")

    try:
        bootstrap_conn = _mysql_connect(
            driver_name=driver_name,  # type: ignore[arg-type]
            connect=connect,
            host=host,
            port=port,
            user=user,
            password=password,
            database=bootstrap_db,
        )
        cur = None
        try:
            cur = bootstrap_conn.cursor()
            cur.execute(f"CREATE DATABASE IF NOT EXISTS `{database}`;")
            bootstrap_conn.commit()
        finally:
            if cur is not None:
                cur.close()
            bootstrap_conn.close()

        conn = _mysql_connect(
            driver_name=driver_name,  # type: ignore[arg-type]
            connect=connect,
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
        )
    # Intentional broad catch for demo portability across mysqlclient/pymysql/mysql-connector.
    # Production code should catch driver-specific exceptions.
    except Exception as exc:  # noqa: BLE001
        print("MySQL async example skipped:", exc)
        return

    try:
        db = AsyncDatabase(conn, MySQLDialect())
        repo = AsyncRepository[User](db, User, auto_schema=True)

        async with db.transaction():
            await db.execute("DROP TABLE IF EXISTS `user`;")

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
