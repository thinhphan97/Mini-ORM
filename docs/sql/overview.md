# SQL ORM Overview

## Core flow

1. Define a dataclass model.
2. Create a `Database` adapter with a SQL dialect.
3. Apply schema (`apply_schema`).
4. Use `Repository[T]` for CRUD and filtering.

## Async flow (same method names)

1. Define a dataclass model.
2. Create an `AsyncDatabase` adapter with a SQL dialect.
3. Apply schema (`await apply_schema_async(...)`).
4. Use `AsyncRepository[T]` with the same method names as sync
   (`insert`, `get`, `list`, `update`, `delete`, ...), but with `await`.

## Relations via metadata

`Repository` relation APIs support `belongs_to` and `has_many`.
Relation specs can be inferred directly from FK field metadata:

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
            "relation": "author",      # optional belongs_to name
            "related_name": "posts",   # optional reverse has_many name
        },
    )
```

Inference result:
- child side: `Post.author` (`belongs_to`)
- parent side: `Author.posts` (`has_many`)

Use with:
- `repo.create(..., relations=...)`
- `repo.get_related(pk, include=[...])`
- `repo.list_related(include=[...])`

The same relation APIs are available in async form:
- `await repo.create(..., relations=...)`
- `await repo.get_related(pk, include=[...])`
- `await repo.list_related(include=[...])`

Default names when omitted:
- `author_id` -> `author`
- child model `Post` -> `posts`

Explicit `__relations__` is still supported when you need full manual control.
For end-to-end examples and error cases, see `docs/sql/repository.md`.

Common repository helpers:

- `count(where=...)`, `exists(where=...)`
- `insert_many(...)`
- `update_where(values, where=...)`, `delete_where(where=...)`
- `get_or_create(lookup=..., defaults=...)`

Async repository provides the same helper names with `await`.

## Field codec flow (Enum/JSON)

Repository I/O supports automatic value conversion:

- Enum fields are stored using `Enum.value` and reconstructed as Enum on read.
- `dict`/`list` fields are stored as JSON text and reconstructed on read.
- You can force JSON handling with `metadata={"codec": "json"}`.

See runnable example:
- `examples/sql/08_codec_serialize_deserialize.py`

## Layer boundaries

- `mini_orm.core`
  - Conditions, query builder, metadata, repository, async repository, schema.
- `mini_orm.ports.db_api`
  - `Database` / `AsyncDatabase` adapters and dialect implementations.

This separation keeps SQL generation logic backend-agnostic and lets you replace adapters without changing core behavior.

## Dialects

Built-in dialects:

- `SQLiteDialect`
- `PostgresDialect`
- `MySQLDialect`

Dialect controls:

- Identifier quoting
- Placeholder style (`:name`, `%s`, `?`)
- `RETURNING` support
