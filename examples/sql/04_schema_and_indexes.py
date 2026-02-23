"""Schema and index generation/apply examples."""

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

from mini_orm import (
    Database,
    SQLiteDialect,
    apply_schema,
    create_index_sql,
    create_indexes_sql,
    create_schema_sql,
    create_table_sql,
)


@dataclass
class Article:
    # PK + auto for SQLite integer primary key behavior.
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})

    # Field-level index metadata.
    slug: str = field(default="", metadata={"unique_index": True, "index_name": "uidx_article_slug"})
    author_id: int = field(default=0, metadata={"index": True})
    title: str = ""
    published: bool = False

    # Multi-column index declarations.
    __indexes__ = [
        ("published", "author_id"),
        {"columns": ("author_id", "title"), "name": "idx_article_author_title"},
    ]


def print_sql_preview(dialect: SQLiteDialect) -> None:
    print("\n--- create_table_sql ---")
    print(create_table_sql(Article, dialect))

    print("\n--- create_index_sql (single column) ---")
    print(create_index_sql(Article, dialect, "title"))

    print("\n--- create_indexes_sql (from metadata + __indexes__) ---")
    for sql in create_indexes_sql(Article, dialect):
        print(sql)

    print("\n--- create_schema_sql (table + indexes) ---")
    for sql in create_schema_sql(Article, dialect):
        print(sql)


def main() -> None:
    dialect = SQLiteDialect()
    print_sql_preview(dialect)

    conn = sqlite3.connect(":memory:")
    db = Database(conn, dialect)
    try:
        # apply_schema executes table + indexes in one transaction.
        statements = apply_schema(db, Article, if_not_exists=True)
        print("\nApplied statements count:", len(statements))

        # Calling again with if_not_exists=True is safe and idempotent.
        apply_schema(db, Article, if_not_exists=True)
        print("Applied schema twice safely with if_not_exists=True")

        # Inspect created indexes in SQLite.
        index_rows = conn.execute("PRAGMA index_list('article');").fetchall()
        index_names = [row[1] for row in index_rows]
        print("SQLite index names:", index_names)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
