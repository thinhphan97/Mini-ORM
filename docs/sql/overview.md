# SQL ORM Overview

## Core flow

1. Define a dataclass model.
2. Create a `Database` adapter with a SQL dialect.
3. Apply schema (`apply_schema`).
4. Use `Repository[T]` for single-model access, or `UnifiedRepository` when one
   hub object should route by model class.
   Mutation methods can infer model from object (`hub.insert(User(...))`).
   Optionally set `auto_schema=True` to create/sync schema automatically on first action
   (or when calling `register(...)`).
   Set `require_registration=True` if you want explicit model registration before actions.

## Async flow (same method names)

1. Define a dataclass model.
2. Create an `AsyncDatabase` adapter with a SQL dialect.
3. Apply schema (`await apply_schema_async(...)`).
4. Use `AsyncRepository[T]` for single-model access, or `AsyncUnifiedRepository`
   for one async hub object routing by model class, with the same method names as sync
   (`insert`, `get`, `list`, `update`, `delete`, ...), but with `await`.
   Async unified mutation methods can infer model from object as well.
   Optionally set `auto_schema=True` for automatic schema ensure on first action
   (or when calling `register(...)`).
   Set `require_registration=True` for explicit registration workflow.

## Unified hub (multi-model)

```python
import asyncio

from mini_orm import UnifiedRepository, AsyncUnifiedRepository

# sync
hub = UnifiedRepository(db, auto_schema=True, require_registration=True)
hub.register(User)
user = hub.insert(User(email="alice@example.com"))  # infer model from object
rows = hub.list(User)

# async
async def main() -> None:
    async_hub = AsyncUnifiedRepository(
        async_db,
        auto_schema=True,
        require_registration=True,
    )
    await async_hub.register(User)
    user = await async_hub.insert(User(email="alice@example.com"))
    rows = await async_hub.list(User)
    print(user, rows)

asyncio.run(main())
```

`schema_conflict` controls incompatible changes:
- `"raise"` (default): raise clear error.
- `"recreate"`: drop/recreate table for that model.

## Args quick reference

Repository constructors (`Repository`, `AsyncRepository`):

- `auto_schema: bool = False`
  - `True`: ensure schema automatically on first action.
  - `False`: do not auto ensure schema.
- `schema_conflict: str = "raise"`
  - `"raise"`: incompatible schema -> error.
  - `"recreate"`: incompatible schema -> drop/recreate table.
- `require_registration: bool = False`
  - `True`: require explicit `register(...)` before actions.
  - `False`: model auto-registers on first action.

Unified constructors (`UnifiedRepository`, `AsyncUnifiedRepository`) use the same args.

Registration helpers:

- `register(model, ensure=None)` / `await register(model, ensure=None)`
  - `ensure=None`: follow `auto_schema`.
  - `ensure=True`: ensure schema immediately, then register.
  - `ensure=False`: only register.
- `register_many(models, ensure=None)` / async equivalent: batch version.

Unified mutation calls support:

- explicit model: `hub.insert(User, user_obj)`.
- inferred model: `hub.insert(user_obj)`.

Inference also applies to: `update`, `delete`, `create`, `insert_many`.

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
