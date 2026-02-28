<p align="center">
  <img src="assets/icon.png" width="160" alt="mini_orm logo" />
</p>

<h1 align="center">mini_orm</h1>

<p align="center">
  Dataclass-first repositories for SQL and vector stores (sync + async).
</p>

<p align="center">
  <a href="./">Docs</a> ·
  <a href="examples/">Examples</a> ·
  <a href="api/package/">API</a>
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

## Quick Usage (SQL)

```python
from dataclasses import dataclass, field
from typing import Optional
import sqlite3

from mini_orm import C, Database, Repository, SQLiteDialect

@dataclass
class User:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = ""

conn = sqlite3.connect(":memory:")
db = Database(conn, SQLiteDialect())
repo = Repository[User](db, User, auto_schema=True)
repo.insert(User(email="alice@example.com"))
rows = repo.list(where=C.like("email", "%@example.com"))
```

## Quick Usage (Session)

```python
from mini_orm import Session

session = Session(db, auto_schema=True)
with session:
    session.insert(User(email="alice@example.com"))
    session.insert(User(email="bob@example.com"))
rows = session.list(User)
```

## Quick Usage (Vector)

```python
from mini_orm import InMemoryVectorStore, VectorMetric, VectorRecord, VectorRepository

store = InMemoryVectorStore()
repo = VectorRepository(store, "items", dimension=3, metric=VectorMetric.COSINE)
repo.upsert([VectorRecord(id="1", vector=[0.1, 0.2, 0.3])])
hits = repo.query([0.1, 0.2, 0.25], top_k=5)
```

## Dev Commands

```bash
make test
make test-vector
make compose-up
make compose-down
```

## Continue Reading

- [Getting Started](getting-started.md)
- [Examples](examples.md)
- [SQL Overview](sql/overview.md)
- [Vector Overview](vector/overview.md)
- [API Reference](api/package.md)
