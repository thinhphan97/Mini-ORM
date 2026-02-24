# Getting Started

## Requirements

- Python 3.10+

## Install project dependencies

```bash
pip install -r requirements-docs.txt
```

For runtime SQL usage, install your DB-API driver (for example `sqlite3` is built in, `psycopg`, `mysqlclient`, etc.).

For Qdrant support:

```bash
pip install qdrant-client
```

For Chroma support:

```bash
pip install chromadb
```

For Faiss support:

```bash
pip install faiss-cpu numpy
```

## Quick start (SQLite)

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

## Quick start (Async SQL, same API names)

```python
import asyncio
import sqlite3
from dataclasses import dataclass, field
from typing import Optional

from mini_orm import C, AsyncDatabase, AsyncRepository, SQLiteDialect, apply_schema_async

@dataclass
class User:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = field(default="", metadata={"index": True})

async def main() -> None:
    conn = sqlite3.connect(":memory:")
    db = AsyncDatabase(conn, SQLiteDialect())
    await apply_schema_async(db, User)
    repo = AsyncRepository[User](db, User)

    await repo.insert(User(email="alice@example.com"))
    rows = await repo.list(where=C.eq("email", "alice@example.com"))
    print(rows)
    conn.close()

asyncio.run(main())
```

Async SQL keeps the same repository method names as sync (`insert`, `get`, `list`,
`update`, `delete`, `count`, `exists`, `create`, `get_related`, ...), only adding
`await` and `async with`.

## Quick relation setup (metadata-based)

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
            "relation": "author",     # optional
            "related_name": "posts",  # optional
        },
    )
    title: str = ""
```

From this metadata, mini_orm infers:
- `Post.author` (`belongs_to`)
- `Author.posts` (`has_many`)

Then use:
- `repo.create(..., relations=...)` for nested create
- `repo.get_related(...)` / `repo.list_related(...)` for eager loading

For full options and troubleshooting, see [`docs/sql/repository.md`](sql/repository.md).

## Quick codec setup (Enum/JSON)

Before calling `repo.insert(...)` / `repo.list(...)`, obtain a `Repository[Article]`
instance from your MiniORM database/session and bind it to `repo`.

```python
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from mini_orm import C

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

repo.insert(Article(status=Status.PUBLISHED, payload={"views": 1}, tags=["orm"]))
loaded = repo.list(where=C.eq("status", Status.PUBLISHED))[0]
```

## Quick start (Async Vector, same API names)

```python
import asyncio

from mini_orm import AsyncVectorRepository, InMemoryVectorStore, VectorRecord

async def main() -> None:
    store = InMemoryVectorStore()
    repo = AsyncVectorRepository(store, "users", dimension=3)

    await repo.upsert(
        [
            VectorRecord(id="u1", vector=[0.1, 0.2, 0.3], payload={"name": "alice"}),
            VectorRecord(id="u2", vector=[0.2, 0.1, 0.5], payload={"name": "bob"}),
        ]
    )
    hits = await repo.query([0.1, 0.2, 0.25], top_k=2)
    print(hits)

asyncio.run(main())
```

Async vector keeps method names from sync (`create_collection`, `upsert`, `query`,
`fetch`, `delete`), only adding `await`.

## Build documentation

```bash
mkdocs serve
```

or:

```bash
mkdocs build
```
