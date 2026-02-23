# mini_orm

Lightweight Python ORM-style toolkit.

## What it supports

- Dataclass-based SQL models.
- Single-table CRUD via `Repository[T]`.
- Safe query building (`where`, `order by`, `limit`, `offset`).
- Schema generation from model metadata.
- Index support:
  - Field metadata (`index`, `unique_index`, `index_name`)
  - Multi-column indexes via `__indexes__`
  - One-call schema apply with `apply_schema(...)`
- SQL dialect adapters: SQLite, Postgres, MySQL.
- Vector abstraction via `VectorRepository`:
  - `InMemoryVectorStore` (built-in)
  - `QdrantVectorStore` (optional, requires `qdrant-client`)
  - `ChromaVectorStore` (optional, requires `chromadb`)
  - `FaissVectorStore` (optional, requires `faiss-cpu` and `numpy`)

## Quick usage (SQL)

```python
from dataclasses import dataclass, field
from typing import Optional
import sqlite3

from mini_orm import Database, SQLiteDialect, Repository, C, apply_schema

@dataclass
class User:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = field(default="", metadata={"index": True})

conn = sqlite3.connect(":memory:")
db = Database(conn, SQLiteDialect())
repo = Repository[User](db, User)

apply_schema(db, User)
repo.insert(User(email="alice@example.com"))
rows = repo.list(where=C.eq("email", "alice@example.com"))
```

## Quick usage (Vector)

```python
from mini_orm import InMemoryVectorStore, VectorMetric, VectorRepository, VectorRecord

store = InMemoryVectorStore()
repo = VectorRepository(store, "items", dimension=3, metric=VectorMetric.COSINE)
repo.upsert([VectorRecord(id="1", vector=[0.1, 0.2, 0.3])])
hits = repo.query([0.1, 0.2, 0.25], top_k=5)
```

## Run tests

```bash
python -m unittest discover -s tests -p "test_*.py"
```

## Continue reading

- [Getting Started](getting-started.md)
- [SQL Overview](sql/overview.md)
- [Vector Overview](vector/overview.md)
- [API Reference](api/package.md)
