# SQL ORM Overview

## Core flow

1. Define a dataclass model.
2. Create a `Database` adapter with a SQL dialect.
3. Apply schema (`apply_schema`).
4. Use `Repository[T]` for CRUD and filtering.

## Relations via metadata

`Repository` relation APIs support `belongs_to` and `has_many`.
Relation specs can be inferred directly from FK field metadata:

```python
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

## Layer boundaries

- `mini_orm.core`
  - Conditions, query builder, metadata, repository, schema.
- `mini_orm.ports.db_api`
  - `Database` adapter and dialect implementations.

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
