"""Query condition examples: where/group/order/limit/offset/count/exists."""

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

from mini_orm import C, Database, OrderBy, Repository, SQLiteDialect, apply_schema


@dataclass
class Account:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = ""
    age: Optional[int] = None
    role: str = "user"
    active: bool = True
    deleted_at: Optional[str] = None


def seed(repo: Repository[Account]) -> None:
    repo.insert_many(
        [
            Account(email="alice@example.com", age=24, role="admin", active=True),
            Account(email="bob@example.com", age=30, role="owner", active=True),
            Account(email="charlie@example.com", age=17, role="user", active=True),
            Account(email="dana@sample.com", age=35, role="user", active=False),
            Account(email="erin@example.com", age=None, role="auditor", active=True),
            Account(
                email="frank@example.com",
                age=40,
                role="user",
                active=False,
                deleted_at="2026-01-01T00:00:00",
            ),
        ]
    )


def main() -> None:
    conn = sqlite3.connect(":memory:")
    db = Database(conn, SQLiteDialect())
    repo = Repository[Account](db, Account)

    try:
        apply_schema(db, Account)
        seed(repo)

        # One condition.
        admins = repo.list(where=C.eq("role", "admin"))
        print("Admins:", admins)

        # Multiple conditions in a list => joined with AND.
        adults_at_example = repo.list(
            where=[C.ge("age", 18), C.like("email", "%@example.com")]
        )
        print("Adults at @example.com:", adults_at_example)

        # Grouped condition:
        # active = true AND (role = admin OR role = owner) AND NOT deleted.
        privileged_active = repo.list(
            where=C.and_(
                C.eq("active", True),
                C.or_(C.eq("role", "admin"), C.eq("role", "owner")),
                C.not_(C.is_not_null("deleted_at")),
            ),
            order_by=[OrderBy("id")],
        )
        print("Privileged active accounts:", privileged_active)

        # IN condition.
        in_roles = repo.list(
            where=C.in_("role", ["admin", "auditor"]),
            order_by=[OrderBy("role"), OrderBy("id")],
        )
        print("IN role(admin, auditor):", in_roles)

        # NULL / NOT NULL checks.
        with_null_age = repo.list(where=C.is_null("age"))
        with_deleted_at = repo.list(where=C.is_not_null("deleted_at"))
        print("Rows where age IS NULL:", with_null_age)
        print("Rows where deleted_at IS NOT NULL:", with_deleted_at)

        # Basic operators: ne, lt, le, gt.
        not_users = repo.list(where=C.ne("role", "user"))
        age_lt_30 = repo.list(where=C.lt("age", 30))
        age_le_30 = repo.list(where=C.le("age", 30))
        age_gt_30 = repo.list(where=C.gt("age", 30))
        print("role <> user:", not_users)
        print("age < 30:", age_lt_30)
        print("age <= 30:", age_le_30)
        print("age > 30:", age_gt_30)

        # Sorting + pagination.
        page = repo.list(
            where=C.like("email", "%@example.com"),
            order_by=[OrderBy("age", desc=True), OrderBy("id", desc=False)],
            limit=2,
            offset=1,
        )
        print("Paged rows (limit=2, offset=1):", page)

        # Count / exists utilities.
        count_example = repo.count(where=C.like("email", "%@example.com"))
        has_minor = repo.exists(where=C.lt("age", 18))
        has_superadmin = repo.exists(where=C.eq("role", "superadmin"))
        print("Count @example.com:", count_example)
        print("Exists minor:", has_minor)
        print("Exists superadmin:", has_superadmin)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
