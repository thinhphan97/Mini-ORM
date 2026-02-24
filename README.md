# mini_orm

Lightweight Python ORM-style toolkit

## What it supports

- Dataclass-based SQL models.
- Single-table CRUD via `Repository[T]`.
- Model relations inferred from `fk` metadata (or explicit `__relations__` override) with:
  - create with nested relation data (`repo.create(..., relations=...)`)
  - eager loading (`get_related`, `list_related`)
- Safe query building (`where`, `AND/OR/NOT`, `order by`, `limit`, `offset`).
- Repository utility APIs: `count`, `exists`, `insert_many`, `update_where`, `delete_where`, `get_or_create`.
- Field codecs for DB I/O:
  - Enum <-> scalar (`Enum.value`)
  - JSON <-> Python structures (`dict`/`list`, or explicit `metadata={"codec": "json"}`)
- Schema generation from model metadata.
- Foreign keys via field metadata `fk`.
- Index support:
  - Field metadata (`index`, `unique_index`, `index_name`)
  - Multi-column indexes via `__indexes__`
  - One-call schema apply with `apply_schema(...)`
  - Idempotent mode with `if_not_exists=True`
- SQL dialect adapters: SQLite, Postgres, MySQL (DB-API style).
- Async SQL APIs with same repository method names:
  - `AsyncDatabase`, `AsyncRepository[T]`, `apply_schema_async(...)`
- Async vector APIs with same repository method names:
  - `AsyncVectorRepository`
- Vector abstraction via `VectorRepository`:
  - `InMemoryVectorStore` (built-in)
  - `QdrantVectorStore` (optional, requires `qdrant-client`)
  - `ChromaVectorStore` (optional, requires `chromadb`)
  - `FaissVectorStore` (optional, requires `faiss-cpu` and `numpy`)
  - Optional payload codec for metadata/filter I/O
    (`IdentityVectorPayloadCodec`, `JsonVectorPayloadCodec`)
    - Enum decode is best-effort (falls back to scalar if enum class is not resolvable at runtime)
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

## Quick usage (Async SQL)

```python
import asyncio
import sqlite3
from dataclasses import dataclass, field
from typing import Optional

from mini_orm import AsyncDatabase, AsyncRepository, SQLiteDialect, apply_schema_async

@dataclass
class User:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = ""

async def main() -> None:
    conn = sqlite3.connect(":memory:")
    db = AsyncDatabase(conn, SQLiteDialect())
    await apply_schema_async(db, User)

    repo = AsyncRepository[User](db, User)
    await repo.insert(User(email="alice@example.com"))
    rows = await repo.list()
    print(rows)
    conn.close()

asyncio.run(main())
```

Async API keeps the same method names as sync (`insert`, `get`, `list`,
`update`, `delete`, `count`, `exists`, `create`, `get_related`, ...). The only
difference is `await` / `async with`.

## Relations via metadata

Declare FK metadata on child model and let mini_orm infer both relation sides.

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
            "relation": "author",      # optional: belongs_to name on Post
            "related_name": "posts",   # optional: has_many name on Author
        },
    )
    title: str = ""
```

This infers:
- `Post.author` (`belongs_to`)
- `Author.posts` (`has_many`)

Use with repository APIs:

```python
author_repo.create(
    Author(name="alice"),
    relations={"posts": [Post(title="p1"), Post(title="p2")]},
)

post_repo.create(
    Post(title="hello"),
    relations={"author": Author(name="bob")},
)

author_with_posts = author_repo.get_related(1, include=["posts"])
posts_with_author = post_repo.list_related(include=["author"])
```

If you need manual control, explicit `__relations__` declarations are still supported
and will override equivalent inferred specs.
This is recommended when related models are defined across different modules and
you need deterministic reverse `has_many` discovery.

See full relation guide: `docs/sql/repository.md`.

## Field codec (Enum/JSON)

`mini_orm` automatically serializes/deserializes common rich types during repository I/O.

```python
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

class Status(str, Enum):
    DRAFT = "draft"
    PUBLISHED = "published"

@dataclass
class Article:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    status: Status = Status.DRAFT
    payload: dict[str, Any] = field(default_factory=dict)  # auto JSON codec
    tags: list[str] = field(default_factory=list)          # auto JSON codec
    extra: Any = field(default_factory=dict, metadata={"codec": "json"})  # explicit codec

row = repo.insert(Article(status=Status.PUBLISHED, payload={"views": 1}, tags=["orm"]))
loaded = repo.get(row.id)

assert loaded.status is Status.PUBLISHED
assert loaded.payload == {"views": 1}
assert loaded.tags == ["orm"]
```

Runnable example:
- `examples/sql/08_codec_serialize_deserialize.py`

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
python -m unittest discover -s tests -p "test_*.py"
```

## Build library

```bash
pip install -r requirements-build.txt
./scripts/build_lib.sh
```

## Publish library

```bash
pip install -r requirements-publish.txt
python3 -m twine upload dist/*
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
