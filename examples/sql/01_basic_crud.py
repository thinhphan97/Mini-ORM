"""Basic SQL CRUD example for mini_orm Repository."""

from __future__ import annotations

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

from mini_orm import Database, Repository, SQLiteDialect, apply_schema


@dataclass
class User:
    # Auto primary key: mini_orm will set this after insert.
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = ""
    age: Optional[int] = None


def main() -> None:
    # 1) Create DB adapter and repository.
    conn = sqlite3.connect(":memory:")
    db = Database(conn, SQLiteDialect())
    repo = Repository[User](db, User)

    try:
        # 2) Create table from dataclass metadata.
        apply_schema(db, User)

        # 3) Insert rows.
        alice = repo.insert(User(email="alice@example.com", age=25))
        bob = repo.insert(User(email="bob@example.com", age=30))
        print("Inserted:", alice, bob)

        # 4) Get by PK.
        fetched = repo.get(alice.id)
        print("Fetched by PK:", fetched)

        # 5) Update by PK (model instance must include PK).
        bob.age = 31
        updated = repo.update(bob)
        print("Updated row count:", updated)

        # 6) List all rows.
        print("All users:", repo.list())

        # 7) Delete by PK.
        deleted = repo.delete(alice)
        print("Deleted row count:", deleted)
        print("After delete:", repo.list())
    finally:
        conn.close()


if __name__ == "__main__":
    main()
