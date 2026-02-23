"""Show SQL generation differences across SQLite/Postgres/MySQL dialects."""

from __future__ import annotations

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

from mini_orm import C, OrderBy, create_table_sql
from mini_orm.core.query_builder import append_limit_offset, compile_order_by, compile_where
from mini_orm.ports.db_api.dialects import MySQLDialect, PostgresDialect, SQLiteDialect


@dataclass
class PreviewUser:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = ""
    age: Optional[int] = None


def show_for_dialect(name: str, dialect) -> None:  # noqa: ANN001
    print(f"\n===== {name} =====")

    where = C.and_(
        C.like("email", "%@example.com"),
        C.or_(C.ge("age", 18), C.is_null("age")),
        C.not_(C.eq("email", "blocked@example.com")),
    )
    order_by = [OrderBy("age", desc=True), OrderBy("id")]

    where_fragment = compile_where(where, dialect)
    order_fragment = compile_order_by(order_by, dialect)
    sql = f"SELECT * FROM {dialect.q('previewuser')}{where_fragment.sql}{order_fragment}"
    sql, params = append_limit_offset(sql, where_fragment.params, limit=5, offset=10, dialect=dialect)

    print("SQL:", sql)
    print("Params:", params)
    print("DDL:", create_table_sql(PreviewUser, dialect))


def main() -> None:
    show_for_dialect("SQLiteDialect", SQLiteDialect())
    show_for_dialect("PostgresDialect", PostgresDialect())
    show_for_dialect("MySQLDialect", MySQLDialect())


if __name__ == "__main__":
    main()
