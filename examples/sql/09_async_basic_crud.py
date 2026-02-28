from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import dataclass, field
from typing import Optional

from mini_orm import C, AsyncDatabase, AsyncRepository, SQLiteDialect

@dataclass
class User:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = ""
    age: Optional[int] = None

async def main() -> None:
    conn = sqlite3.connect(":memory:")
    try:
        db = AsyncDatabase(conn, SQLiteDialect())
        repo = AsyncRepository[User](db, User, auto_schema=True)

        alice = await repo.insert(User(email="alice@example.com", age=20))
        print("inserted:", alice)

        found = await repo.get(alice.id)
        print("found:", found)
        if found is None:
            raise RuntimeError("Inserted user was not found.")

        found.age = 21
        await repo.update(found)
        print("after update:", await repo.get(found.id))

        rows = await repo.list(
            where=C.like("email", "%@example.com"),
        )
        print("rows:", rows)

        await repo.delete(found)
        print("count after delete:", await repo.count())
    finally:
        conn.close()

if __name__ == "__main__":
    asyncio.run(main())
