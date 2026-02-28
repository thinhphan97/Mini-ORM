"""Async unified repository example: one async hub for multiple model classes."""

from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import dataclass, field
from typing import Optional

from mini_orm import (
    AsyncDatabase,
    AsyncUnifiedRepository,
    OrderBy,
    SQLiteDialect,
)

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

async def main() -> None:
    conn = sqlite3.connect(":memory:")
    try:
        db = AsyncDatabase(conn, SQLiteDialect())
        hub = AsyncUnifiedRepository(db, auto_schema=True, require_registration=True)
        await hub.register_many([Author, Post])

        author = await hub.create(
            Author(name="Alice"),
            relations={"posts": [Post(title="P1"), Post(title="P2")]},
        )
        print("Created author:", author)
        print("Post count:", await hub.count(Post))

        related = await hub.get_related(Author, author.id, include=["posts"])
        print("Author with posts:", related)

        posts = await hub.list_related(Post, include=["author"], order_by=[OrderBy("id")])
        print("Posts with author:", posts)
    finally:
        conn.close()

if __name__ == "__main__":
    asyncio.run(main())
