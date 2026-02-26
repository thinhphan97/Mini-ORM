"""ValidatedModel + SQLite Repository integration example."""

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

from mini_orm import Database, Repository, SQLiteDialect, ValidationError, ValidatedModel


@dataclass
class Customer(ValidatedModel):
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = field(
        default="",
        metadata={
            "unique_index": True,
            "non_empty": True,
            "pattern": r"[^@]+@[^@]+\.[^@]+",
        },
    )
    full_name: str = field(default="", metadata={"non_empty": True, "min_len": 2})
    age: int = field(default=0, metadata={"ge": 13, "le": 120})


def create_customer(
    repo: Repository[Customer],
    *,
    email: str,
    full_name: str,
    age: int,
) -> None:
    try:
        created = repo.insert(
            Customer(
                email=email,
                full_name=full_name,
                age=age,
            )
        )
    except ValidationError as exc:
        print(f"Rejected input for {email!r}: {exc}")
        return

    print("Created:", created)


def main() -> None:
    conn = sqlite3.connect(":memory:")
    try:
        db = Database(conn, SQLiteDialect())
        # auto_schema=True keeps the first-use flow simple for users.
        repo = Repository[Customer](db, Customer, auto_schema=True)

        create_customer(
            repo,
            email="alice@example.com",
            full_name="Alice",
            age=22,
        )
        create_customer(
            repo,
            email="not-an-email",
            full_name="A",
            age=10,
        )

        all_customers = repo.list()
        print("Stored customers:", all_customers)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
