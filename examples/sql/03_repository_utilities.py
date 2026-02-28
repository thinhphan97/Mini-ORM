"""Repository utility APIs: insert_many/update_where/delete_where/get_or_create."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Optional

from mini_orm import C, Database, Repository, SQLiteDialect, apply_schema

@dataclass
class Product:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    sku: str = field(default="", metadata={"unique_index": True})
    category: str = ""
    price: float = 0.0
    stock: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.sku, str) or not self.sku.strip():
            raise ValueError("sku must be a non-empty string.")

def main() -> None:
    conn = sqlite3.connect(":memory:")
    db = Database(conn, SQLiteDialect())
    repo = Repository[Product](db, Product)

    try:
        apply_schema(db, Product)

        # insert_many is a convenience wrapper that calls insert for each object.
        inserted = repo.insert_many(
            [
                Product(sku="MUG-BLACK", category="kitchen", price=9.9, stock=50),
                Product(sku="MUG-WHITE", category="kitchen", price=10.9, stock=80),
                Product(sku="BOOK-ORM", category="books", price=29.9, stock=20),
            ]
        )
        print("Inserted products:", inserted)

        # count / exists utility methods.
        print("Count all:", repo.count())
        print("Exists books:", repo.exists(where=C.eq("category", "books")))
        print("Exists toys:", repo.exists(where=C.eq("category", "toys")))

        # update_where updates all rows matching the where expression.
        affected_update = repo.update_where(
            {"price": 12.5},
            where=C.eq("category", "kitchen"),
        )
        print("update_where affected rows:", affected_update)
        print("After bulk update:", repo.list(where=C.eq("category", "kitchen")))

        # delete_where deletes all rows matching the where expression.
        affected_delete = repo.delete_where(where=C.lt("stock", 30))
        print("delete_where affected rows:", affected_delete)
        print("After bulk delete:", repo.list())

        # get_or_create: first call creates, second call reuses.
        first, created_first = repo.get_or_create(
            lookup={"sku": "LAMP-RED"},
            defaults={"category": "home", "price": 49.9, "stock": 10},
        )
        second, created_second = repo.get_or_create(
            lookup={"sku": "LAMP-RED"},
            defaults={"category": "ignored", "price": 999.0, "stock": 0},
        )
        print("get_or_create first:", first, "created=", created_first)
        print("get_or_create second:", second, "created=", created_second)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
