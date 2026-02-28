"""Unified repository example: one hub object for multiple model classes."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Optional

from mini_orm import Database, OrderBy, SQLiteDialect, UnifiedRepository

@dataclass
class Author:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    name: str = ""

@dataclass
class Post:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    author_id: Optional[int] = field(
        default=None,
        metadata={"fk": (Author, "id"), "relation": "author", "related_name": "posts"},
    )
    title: str = ""

def main() -> None:
    conn = sqlite3.connect(":memory:")
    try:
        db = Database(conn, SQLiteDialect())
        hub = UnifiedRepository(db, auto_schema=True, require_registration=True)
        hub.register_many([Author, Post])

        author = hub.create(
            Author(name="Alice"),
            relations={"posts": [Post(title="P1"), Post(title="P2")]},
        )
        print("Created author:", author)
        print("Post count:", hub.count(Post))

        related = hub.get_related(Author, author.id, include=["posts"])
        print("Author with posts:", related)

        posts = hub.list_related(Post, include=["author"], order_by=[OrderBy("id")])
        print("Posts with author:", posts)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
