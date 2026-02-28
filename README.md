<p align="center">
  <img src="docs/assets/icon.png" width="160" alt="mini_orm logo" />
</p>

<h1 align="center">mini_orm</h1>

<p align="center">
  Dataclass-first repositories for SQL and vector stores (sync + async).
</p>

<p align="center">
  <a href="https://thinhphan97.github.io/Mini-ORM/">Docs</a> ·
  <a href="https://thinhphan97.github.io/Mini-ORM/examples/">Examples</a> ·
  <a href="https://thinhphan97.github.io/Mini-ORM/api/package/">API</a>
</p>

<p align="center">
  <img alt="python" src="https://img.shields.io/badge/python-3.10%2B-blue" />
  <img alt="license" src="https://img.shields.io/badge/license-MIT-green" />
  <img alt="status" src="https://img.shields.io/badge/status-experimental-orange" />
</p>

## Features

- SQL repositories over DB-API connections (SQLite, Postgres, MySQL).
- Dataclass models + schema generation (`apply_schema`, `auto_schema`).
- Relations from FK metadata (`create(..., relations=...)`, `get_related`, `list_related`).
- Safe query building (`C.*`, `OrderBy`, pagination).
- Sync + async APIs with matching method names.
- Vector repositories with multiple backends (InMemory, PgVector, Qdrant, Chroma, Faiss).
- Optional dataclass validation via `ValidatedModel`.

## Install

From source:

```bash
pip install -e .
```

Optional extras:

```bash
pip install -e '.[qdrant]'
pip install -e '.[chroma]'
pip install -e '.[faiss]'
pip install -e '.[docs]'
```

## Docker Compose (services for examples/tests)

The repository includes `docker-compose.yml` for the external services used by examples and integration tests:

- `postgres` (`pgvector/pgvector:pg16`) for Postgres + PgVector examples.
- `mysql` (`mysql:8.4`) for MySQL examples.
- `qdrant` (`qdrant/qdrant:v1.17`) for host-server Qdrant tests.
- `chroma` (`chromadb/chroma:1.5.2`) for host-server Chroma tests.

Start services:

```bash
make compose-up
make compose-ps
```

Stop services:

```bash
make compose-down
# reset all compose volumes:
make compose-reset
```

Environment variables read by examples/tests:

```bash
export MINI_ORM_PG_HOST=localhost
export MINI_ORM_PG_PORT=5432
export MINI_ORM_PG_USER=postgres
export MINI_ORM_PG_PASSWORD=password
export MINI_ORM_PG_DATABASE=postgres

export MINI_ORM_MYSQL_HOST=localhost
export MINI_ORM_MYSQL_PORT=3306
export MINI_ORM_MYSQL_USER=root
export MINI_ORM_MYSQL_PASSWORD=password
export MINI_ORM_MYSQL_DATABASE=mini_orm_test

export MINI_ORM_QDRANT_HTTP_PORT=6333
export MINI_ORM_QDRANT_GRPC_PORT=6334
export MINI_ORM_QDRANT_URL=http://localhost:6333

export MINI_ORM_CHROMA_PORT=8000
export MINI_ORM_CHROMA_HOST=localhost
```

## Quick usage (SQL)

```python
from dataclasses import dataclass, field
from typing import Optional
import sqlite3

from mini_orm import C, Database, Repository, SQLiteDialect

@dataclass
class User:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = field(default="", metadata={"index": True})

conn = sqlite3.connect(":memory:")
db = Database(conn, SQLiteDialect())
repo = Repository[User](db, User, auto_schema=True)
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

## Connection pooling (`PoolConnector`)

```python
import sqlite3
from mini_orm import Database, PoolConnector, SQLiteDialect

pool = PoolConnector(
    sqlite3.connect,
    "app.db",
    check_same_thread=False,
    max_size=10,
    transaction_guard="rollback",  # rollback | raise | ignore | discard
    strict_pool=False,             # True: discard dirty connections
)
db = Database(pool, SQLiteDialect())
try:
    db.execute('CREATE TABLE IF NOT EXISTS "t" ("id" INTEGER);')
finally:
    db.close(close_pool=True)  # release adapter connection + close whole pool
```

Note: for SQLite `:memory:`, `PoolConnector` rejects `max_size > 1` because each
connection would otherwise get an isolated in-memory database.  
Use shared-memory URI (`file:...mode=memory&cache=shared`, `uri=True`) or `max_size=1`.

## Quick usage (Unified SQL hub)

```python
from mini_orm import UnifiedRepository

hub = UnifiedRepository(db, auto_schema=True, require_registration=True)
hub.register_many([User])
hub.insert(User(email="alice@example.com"))  # model inferred from object
rows = hub.list(User)
```

## Quick usage (Session)

```python
from mini_orm import Session

session = Session(db, auto_schema=True)
with session:
    alice = session.insert(User(email="alice@example.com"))
    _ = session.insert(User(email="bob@example.com"))

rows = session.list(User)
```

Async session:

```python
from mini_orm import AsyncSession

async def main() -> None:
    session = AsyncSession(async_db, auto_schema=True)
    async with session:
        await session.insert(User(email="alice@example.com"))
        await session.insert(User(email="bob@example.com"))
    rows = await session.list(User)
```

## SQL Repository Args (Sync + Async)

`Repository[T]` / `AsyncRepository[T]` constructor:

- `db`: database adapter (`Database` or `AsyncDatabase`).
- `model`: dataclass model class handled by repository.
- `auto_schema` (default `False`):
  - `True`: auto create/sync schema for model on first action.
  - `False`: no schema sync; you manage schema manually.
- `schema_conflict` (default `"raise"`):
  - `"raise"`: raise error when incompatible schema changes are detected.
  - `"recreate"`: drop and recreate table when incompatible changes are detected.
- `require_registration` (default `False`):
  - `True`: must call `register(...)` before CRUD actions.
  - `False`: repository auto-registers model on first action.

`UnifiedRepository` / `AsyncUnifiedRepository` constructor uses the same three
flags: `auto_schema`, `schema_conflict`, `require_registration`.

Registration helpers (different signatures by repository type):

- `Repository[T]` / `AsyncRepository[T]` (model is bound in constructor):
  - `repo.register(*, ensure=None)` / `await repo.register(*, ensure=None)`
  - `repo.register_many(*, ensure=None)` / `await repo.register_many(*, ensure=None)`
  - `register_many` is an intentional single-model alias for API consistency and
    delegates to `register(ensure=...)` internally
    (`mini_orm.core.repositories.repository.Repository.register_many` and
    `mini_orm.core.repositories.repository_async.AsyncRepository.register_many`).
- `UnifiedRepository` / `AsyncUnifiedRepository` (model is passed per call):
  - `hub.register(model, ensure=None)` / `await hub.register(model, ensure=None)`
  - `hub.register_many([ModelA, ModelB], ensure=None)` / async equivalent
- `ensure=None` (default): follow `auto_schema`.
- `ensure=True`: force schema ensure now, then register.
- `ensure=False`: register only (and because model is already registered, first action also skips auto schema ensure).

Unified mutation methods support 2 call styles:

- Explicit model: `hub.insert(User, user_obj)` (backward compatible).
- Object-only: `hub.insert(user_obj)` (model inferred from object type).

This applies to mutation APIs: `insert`, `update`, `delete`, `create`, `insert_many`.
Read/query APIs still use explicit model class: `get/list/count/exists/...`.

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

`AsyncUnifiedRepository` is available with the same style and also supports
inferring model from object for mutation methods:
`await hub.insert(User(...))`, `await hub.update(user_obj)`, `await hub.delete(user_obj)`.
Read/list/get still use model-class-first:
`await hub.list(User)`, `await hub.get(User, id)`.
You can enable schema auto-sync with `auto_schema=True` for both `Repository`
and `AsyncRepository`/`AsyncUnifiedRepository`.
For strict behavior, set `require_registration=True` and call `register(...)`
or `register_many(...)` before actions.

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

## Dataclass Input Validation (Pydantic-like Basics)

```python
from dataclasses import dataclass, field
from mini_orm import ValidatedModel, ValidationError

@dataclass
class CreateUserInput(ValidatedModel):
    email: str = field(default="", metadata={"non_empty": True, "pattern": r"[^@]+@[^@]+\.[^@]+"})
    age: int = field(default=0, metadata={"ge": 0, "le": 130})

payload = CreateUserInput(email="alice@example.com", age=20)  # ok

try:
    CreateUserInput(email="bad-email", age=-1)
except ValidationError as exc:
    print(exc)
```

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

# host-server vector integration tests (Qdrant/Chroma/PgVector SQL+vector)
make test-vector-host

# PgVector SQL+vector integration tests on host PostgreSQL
make test-pgvector-host
```

Notes:
- `test-vector-host` and `test-pgvector-host` set `MINI_ORM_VECTOR_HOST_TESTS=1`.
- For PgVector host tests, make sure PostgreSQL env vars are set
  (`MINI_ORM_PG_HOST`, `MINI_ORM_PG_PORT`, `MINI_ORM_PG_USER`, `MINI_ORM_PG_PASSWORD`, `MINI_ORM_PG_DATABASE`).

## Build library

```bash
make build-lib
```

## Publish library

```bash
make release-check
make release-lib
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

Deployed docs: [https://thinhphan97.github.io/Mini-ORM/](https://thinhphan97.github.io/Mini-ORM/)

```bash
pip install -r requirements-docs.txt
mkdocs serve
```

More docs are in `docs/`.

## Disclaimer

This codebase is provided for learning and educational purposes only.
The author/maintainers are not liable for any incidents, data loss, or damages
resulting from using, integrating, or deploying this code in real environments.
