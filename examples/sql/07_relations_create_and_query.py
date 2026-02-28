"""Relation example: create nested records and query with included relations."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Optional

from mini_orm import C, Database, OrderBy, Repository, SQLiteDialect

@dataclass
class Author:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    name: str = ""

@dataclass
class Post:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    author_id: Optional[int] = field(
        default=None,
        metadata={
            "fk": (Author, "id"),
            "relation": "author",
            "related_name": "posts",
        },
    )
    title: str = ""
    published: bool = False

def main() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON;")
    db = Database(conn, SQLiteDialect())
    author_repo = Repository[Author](db, Author, auto_schema=True)
    post_repo = Repository[Post](db, Post, auto_schema=True)

    try:
        # 1) Create author with related posts in one call (has_many).
        alice = Author(name="Alice")
        author_repo.create(
            alice,
            relations={
                "posts": [
                    Post(title="Mini ORM Basics", published=True),
                    Post(title="Mini ORM Relations", published=False),
                ]
            },
        )
        print("Created author with posts:", alice)

        # 2) Create one post with nested author (belongs_to).
        bonus_post = Post(title="Belongs To Flow", published=True)
        post_repo.create(
            bonus_post,
            relations={"author": Author(name="Bob")},
        )
        print("Created post with nested author:", bonus_post)

        # 3) Fetch one author with included posts.
        author_with_posts = author_repo.get_related(alice.id, include=["posts"])
        if author_with_posts is not None:
            print("Author:", author_with_posts.obj)
            print("Author posts:", author_with_posts.relations["posts"])

        # 4) List posts with included author.
        posts_with_author = post_repo.list_related(
            include=["author"],
            where=C.eq("published", True),
            order_by=[OrderBy("id")],
        )
        print("Published posts with author:")
        for item in posts_with_author:
            print("-", item.obj.title, "| author:", item.relations["author"])
    finally:
        conn.close()

if __name__ == "__main__":
    main()
