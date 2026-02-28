# mini_orm

Lightweight Python ORM-style toolkit.

## What it supports

- Dataclass-based SQL models.
- Single-table CRUD via `Repository[T]`.
- Multi-model routing with one hub object via `UnifiedRepository`
  (object-only mutation support included).
- Async SQL flow via `AsyncRepository[T]`, `AsyncUnifiedRepository`, and `AsyncDatabase`.
- Optional schema auto-sync with `auto_schema=True` and conflict policy `schema_conflict`.
- Optional strict registration with `require_registration=True` and `register(..., ensure=...)`.
- Model relations inferred from FK metadata (`fk`, `relation`, `related_name`) with
  nested create and eager-loading (`get_related`, `list_related`).
- Safe query building (`where`, `AND/OR/NOT`, `order by`, `limit`, `offset`).
- Repository utility APIs: `count`, `exists`, `insert_many`, `update_where`, `delete_where`, `get_or_create`.
- Field codecs for DB I/O:
  - Enum <-> scalar (`Enum.value`)
  - JSON <-> Python structures (`dict`/`list`, or explicit `metadata={"codec": "json"}`)
- Schema generation from model metadata.
- Index support:
  - Field metadata (`index`, `unique_index`, `index_name`)
  - Multi-column indexes via `__indexes__`
  - One-call schema apply with `apply_schema(...)`
  - Async schema apply with `apply_schema_async(...)`
  - Idempotent mode with `if_not_exists=True`
- SQL dialect adapters: SQLite, Postgres, MySQL.
- Vector abstraction via `VectorRepository` / `AsyncVectorRepository`:
  - `InMemoryVectorStore` (built-in)
  - `PgVectorStore` (PostgreSQL + pgvector extension)
  - `QdrantVectorStore` (optional, requires `qdrant-client`)
  - `ChromaVectorStore` (optional, requires `chromadb`)
  - `FaissVectorStore` (optional, requires `faiss-cpu` and `numpy`)
  - Optional payload codec for metadata/filter I/O
    (`IdentityVectorPayloadCodec`, `JsonVectorPayloadCodec`)

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

## Quick usage (Async SQL)

```python
import asyncio
import sqlite3
from dataclasses import dataclass, field
from typing import Optional

from mini_orm import AsyncDatabase, AsyncRepository, SQLiteDialect

@dataclass
class User:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = field(default="", metadata={"index": True})

async def main() -> None:
    conn = sqlite3.connect(":memory:")
    db = AsyncDatabase(conn, SQLiteDialect())
    try:
        repo = AsyncRepository[User](db, User, auto_schema=True)
        await repo.insert(User(email="alice@example.com"))
        rows = await repo.list()
        print(rows)
    finally:
        conn.close()

asyncio.run(main())
```

## Relations via metadata (quick view)

```python
from dataclasses import dataclass, field
from typing import Optional

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
```

Inferred relations:
- `Post.author` (`belongs_to`)
- `Author.posts` (`has_many`)

Detailed guide:
- [`docs/sql/repository.md`](sql/repository.md#relations-create-and-query) section "Relations (create and query)"

## Quick usage (Vector)

```python
from mini_orm import InMemoryVectorStore, VectorMetric, VectorRepository, VectorRecord

store = InMemoryVectorStore()
repo = VectorRepository(store, "items", dimension=3, metric=VectorMetric.COSINE)
repo.upsert([VectorRecord(id="1", vector=[0.1, 0.2, 0.3])])
hits = repo.query([0.1, 0.2, 0.25], top_k=5)
```

## Quick usage (Async Vector)

```python
import asyncio
from mini_orm import AsyncVectorRepository, InMemoryVectorStore, VectorRecord

async def main() -> None:
    store = InMemoryVectorStore()
    repo = AsyncVectorRepository(store, "items", dimension=3)
    await repo.upsert([VectorRecord(id="1", vector=[0.1, 0.2, 0.3])])
    hits = await repo.query([0.1, 0.2, 0.25], top_k=5)
    print(hits)

asyncio.run(main())
```

## Run tests

```bash
make test
make test-vector
```

## Continue reading

- [Getting Started](getting-started.md)
- [Examples](examples.md)
- [SQL Overview](sql/overview.md)
- [Vector Overview](vector/overview.md)
- [API Reference](api/package.md)
