# mini_orm

Lightweight Python ORM-style toolkit

## What it supports

- Dataclass-based SQL models.
- Single-table CRUD via `Repository[T]`.
- Model relations via `__relations__` (`belongs_to`, `has_many`) with:
  - create with nested relation data (`repo.create(..., relations=...)`)
  - eager loading (`get_related`, `list_related`)
- Safe query building (`where`, `AND/OR/NOT`, `order by`, `limit`, `offset`).
- Repository utility APIs: `count`, `exists`, `insert_many`, `update_where`, `delete_where`, `get_or_create`.
- Schema generation from model metadata.
- Foreign keys via field metadata `fk`.
- Index support:
  - Field metadata (`index`, `unique_index`, `index_name`)
  - Multi-column indexes via `__indexes__`
  - One-call schema apply with `apply_schema(...)`
  - Idempotent mode with `if_not_exists=True`
- SQL dialect adapters: SQLite, Postgres, MySQL (DB-API style).
- Vector abstraction via `VectorRepository`:
  - `InMemoryVectorStore` (built-in)
  - `QdrantVectorStore` (optional, requires `qdrant-client`)
  - `ChromaVectorStore` (optional, requires `chromadb`)
  - `FaissVectorStore` (optional, requires `faiss-cpu` and `numpy`)
  - ID policy:
    - Qdrant requires UUID string IDs.
    - InMemory/Chroma/Faiss accept generic string IDs.
  - Filter policy (`query(..., filters={...})`):
    - InMemory/Chroma/Qdrant support basic payload equality filters.
    - Faiss does not support payload filters and raises `NotImplementedError`.

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

rows = repo.list(
    where=C.or_(
        C.eq("email", "alice@example.com"),
        C.eq("email", "bob@example.com"),
    ),
    limit=10,
)
total = repo.count(where=C.like("email", "%@example.com"))
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

## Build library

```bash
pip install -r requirements-build.txt
./scripts/build_lib.sh
```

## MySQL note

When using `MySQLDialect`, current type mapping treats Python `str` fields as SQL `TEXT`.
MySQL cannot create `INDEX`/`UNIQUE` directly on `TEXT` columns without a key length,
so schema apply may fail with errors like:

`BLOB/TEXT column '...' used in key specification without a key length`

To avoid this:

- do not set `index=True` or `unique_index=True` on `str` fields in MySQL right now, or
- customize your schema/type mapping to use `VARCHAR(n)` for indexed string columns.

## Documentation site (MkDocs)

```bash
pip install -r requirements-docs.txt
mkdocs serve
```

More docs are in `docs/`.
