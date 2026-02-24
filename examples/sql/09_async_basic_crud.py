from __future__ import annotations

import asyncio
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# Allow running this script directly from repository root.
PROJECT_ROOT = next(
    (parent for parent in Path(__file__).resolve().parents if (parent / "mini_orm").exists()),
    None,
)
if PROJECT_ROOT and str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from mini_orm import C, AsyncDatabase, AsyncRepository, SQLiteDialect, apply_schema_async


@dataclass
class User:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = ""
    age: Optional[int] = None


async def main() -> None:
    conn = sqlite3.connect(":memory:")
    try:
        db = AsyncDatabase(conn, SQLiteDialect())
        await apply_schema_async(db, User)

        repo = AsyncRepository[User](db, User)

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
