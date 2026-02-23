"""Validation and error case examples for SQL repository and schema APIs."""

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

from mini_orm import C, Database, Repository, SQLiteDialect, apply_schema


class PlainModel:
    """Intentionally not a dataclass."""


@dataclass
class NoPkModel:
    email: str = ""


@dataclass
class MultiPkModel:
    id1: int = field(default=0, metadata={"pk": True})
    id2: int = field(default=0, metadata={"pk": True})


@dataclass
class User:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = ""
    age: Optional[int] = None


@dataclass
class OnlyPkModel:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})


def expect_error(label: str, fn) -> None:  # noqa: ANN001
    try:
        fn()
    except Exception as exc:  # noqa: BLE001
        print(f"[OK] {label}: {type(exc).__name__}: {exc}")
    else:
        print(f"[UNEXPECTED] {label}: no exception raised")


def main() -> None:
    conn = sqlite3.connect(":memory:")
    db = Database(conn, SQLiteDialect())
    repo = Repository[User](db, User)

    try:
        apply_schema(db, User)
        apply_schema(db, OnlyPkModel)

        # Model-level validations.
        expect_error("Repository requires dataclass model", lambda: Repository(db, PlainModel))
        expect_error("Model must have PK", lambda: Repository(db, NoPkModel))
        expect_error("Model must have exactly one PK", lambda: Repository(db, MultiPkModel))

        # Seed one row for update/delete and where validations.
        row = repo.insert(User(email="seed@example.com", age=20))

        # update/delete require PK on object.
        expect_error("update requires PK", lambda: repo.update(User(email="x@example.com", age=1)))
        expect_error("delete requires PK", lambda: repo.delete(User(email="x@example.com", age=1)))

        # list validates pagination values.
        expect_error("limit must be > 0", lambda: repo.list(limit=0))
        expect_error("offset must be >= 0", lambda: repo.list(offset=-1))

        # update_where validations.
        expect_error("update_where empty values", lambda: repo.update_where({}, where=C.eq("id", row.id)))
        expect_error("update_where missing where", lambda: repo.update_where({"age": 30}, where=None))
        expect_error("update_where unknown column", lambda: repo.update_where({"unknown": 1}, where=C.eq("id", row.id)))
        expect_error("update_where cannot update PK", lambda: repo.update_where({"id": 999}, where=C.eq("id", row.id)))

        # delete_where validation.
        expect_error("delete_where missing where", lambda: repo.delete_where(where=None))

        # get_or_create validation.
        expect_error("get_or_create requires non-empty lookup", lambda: repo.get_or_create(lookup={}))

        # Updating a model with only PK and no writable columns is rejected.
        only_pk_repo = Repository[OnlyPkModel](db, OnlyPkModel)
        only = only_pk_repo.insert(OnlyPkModel())
        expect_error("update rejected when no writable columns", lambda: only_pk_repo.update(only))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
