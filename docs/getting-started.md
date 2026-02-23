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

## Build documentation

```bash
mkdocs serve
```

or:

```bash
mkdocs build
```
