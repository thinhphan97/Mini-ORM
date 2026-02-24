"""Enum/JSON codec serialize-deserialize example for SQL repository."""

from __future__ import annotations

import sqlite3
import sys
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Optional

# Allow running this script directly from repository root.
PROJECT_ROOT = next(
    (parent for parent in Path(__file__).resolve().parents if (parent / "mini_orm").exists()),
    None,
)
if PROJECT_ROOT and str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from mini_orm import C, Database, Repository, SQLiteDialect, apply_schema


class ArticleStatus(str, Enum):
    DRAFT = "draft"
    PUBLISHED = "published"


@dataclass
class Article:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    title: str = ""
    status: ArticleStatus = ArticleStatus.DRAFT
    meta: dict[str, Any] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)
    extra: Any = field(default_factory=dict, metadata={"codec": "json"})


def main() -> None:
    conn = sqlite3.connect(":memory:")
    db = Database(conn, SQLiteDialect())
    repo = Repository[Article](db, Article)

    try:
        apply_schema(db, Article)

        # Input object: rich Python values.
        inserted = repo.insert(
            Article(
                title="Codec demo",
                status=ArticleStatus.PUBLISHED,
                meta={"views": 100, "featured": True},
                tags=["orm", "codec"],
                extra={"author": {"name": "Alice"}},
            )
        )
        print("Inserted model:", inserted)

        # Raw DB values after serialize phase.
        raw = conn.execute(
            'SELECT "status", "meta", "tags", "extra" FROM "article" WHERE "id" = ?;',
            (inserted.id,),
        ).fetchone()
        print("Raw DB row (serialized):", raw)

        # Output model after deserialize phase.
        loaded = repo.get(inserted.id)
        print("Loaded model (deserialized):", loaded)
        print(
            "Loaded value types:",
            type(loaded.status).__name__,
            type(loaded.meta).__name__,
            type(loaded.tags).__name__,
            type(loaded.extra).__name__,
        )

        # Query input also supports codec conversion (Enum in WHERE).
        matched = repo.list(where=C.eq("status", ArticleStatus.PUBLISHED))
        print("Matched by enum status:", matched)

        # Update input with JSON payload; read back as Python dict.
        repo.update_where(
            {"meta": {"views": 101, "featured": False}},
            where=C.eq("status", ArticleStatus.PUBLISHED),
        )
        print("After update_where:", repo.get(inserted.id))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
