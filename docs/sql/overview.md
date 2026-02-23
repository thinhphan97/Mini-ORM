# SQL ORM Overview

## Core flow

1. Define a dataclass model.
2. Create a `Database` adapter with a SQL dialect.
3. Apply schema (`apply_schema`).
4. Use `Repository[T]` for CRUD and filtering.

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
