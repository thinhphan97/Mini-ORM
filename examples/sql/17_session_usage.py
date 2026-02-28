"""Session example: transaction-scoped sync and async SQL flows."""

from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import dataclass, field
from typing import Optional

from mini_orm import (
    C,
    AsyncDatabase,
    AsyncSession,
    Database,
    OrderBy,
    SQLiteDialect,
    Session,
)


@dataclass
class User:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = ""
    age: Optional[int] = None


def sync_demo() -> None:
    print("=== Sync Session ===")
    conn = sqlite3.connect(":memory:")
    try:
        db = Database(conn, SQLiteDialect())
        session = Session(db, auto_schema=True)

        with session:
            session.insert(User(email="alice@example.com", age=25))
            session.insert(User(email="bob@example.com", age=30))
            session.update_where(
                User,
                {"age": 31},
                where=C.eq("email", "bob@example.com"),
            )

        rows = session.list(User, order_by=[OrderBy("id")])
        print("After committed transaction:", rows)

        try:
            with session.begin():
                session.insert(User(email="rollback@example.com", age=99))
                raise RuntimeError("force rollback")
        except RuntimeError:
            print("Rollback transaction executed as expected.")

        print("Count after rollback:", session.count(User))
    finally:
        conn.close()


async def async_demo() -> None:
    print("\n=== Async Session ===")
    conn = sqlite3.connect(":memory:")
    try:
        db = AsyncDatabase(conn, SQLiteDialect())
        session = AsyncSession(db, auto_schema=True)

        async with session:
            await session.insert(User(email="carol@example.com", age=22))
            await session.insert(User(email="dave@example.com", age=28))
            await session.update_where(
                User,
                {"age": 29},
                where=C.eq("email", "dave@example.com"),
            )

        rows = await session.list(User, order_by=[OrderBy("id")])
        print("After committed async transaction:", rows)

        try:
            async with session.begin():
                await session.insert(User(email="rollback-async@example.com", age=77))
                raise RuntimeError("force async rollback")
        except RuntimeError:
            print("Async rollback transaction executed as expected.")

        print("Count after async rollback:", await session.count(User))
    finally:
        conn.close()


def main() -> None:
    sync_demo()
    asyncio.run(async_demo())


if __name__ == "__main__":
    main()
