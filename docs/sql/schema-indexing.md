# Schema and Indexing

`mini_orm` can generate table and index SQL directly from dataclass definitions.

## Field-level index

```python
from dataclasses import dataclass, field
from typing import Optional

@dataclass
class User:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = field(default="", metadata={"index": True})
    username: str = field(default="", metadata={"unique_index": True})
```

Supported metadata keys:

- `pk=True`: primary key
- `auto=True`: auto-increment PK behavior
- `index=True`: normal index
- `unique_index=True`: unique index
- `index_name="..."`: custom single-column index name
- `fk=...`: foreign key reference (`"table.column"`, `(ModelOrTable, "column")`, or `{"model": Model, "column": "id"}`)

## MySQL limitation for string indexes

With `MySQLDialect`, Python `str` currently maps to SQL `TEXT`.
MySQL requires a key length when indexing `TEXT`, so `index=True` or
`unique_index=True` on `str` fields can fail during `apply_schema(...)` with:

`BLOB/TEXT column '...' used in key specification without a key length`

Recommended workaround:

- avoid `index`/`unique_index` on `str` fields when using MySQL, or
- use custom schema/type mapping so indexed string columns become `VARCHAR(n)`.

## Multi-column index

```python
@dataclass
class User:
    ...
    __indexes__ = [
        ("email", "username"),
        {"columns": ("email", "age"), "unique": False, "name": "idx_user_email_age"},
    ]
```

## Apply table + indexes in one call

```python
from mini_orm import apply_schema

apply_schema(db, User)
# idempotent mode (safe to run repeatedly):
apply_schema(db, User, if_not_exists=True)
```

`apply_schema` executes table creation first, then all configured indexes in the same transaction.

## Manual SQL generation

```python
from mini_orm import create_table_sql, create_indexes_sql

table_sql = create_table_sql(User, db.dialect)
index_sql_list = create_indexes_sql(User, db.dialect)

# generate CREATE ... IF NOT EXISTS (supported by sqlite/postgres for indexes)
table_sql_safe = create_table_sql(User, db.dialect, if_not_exists=True)
index_sql_safe = create_indexes_sql(User, db.dialect, if_not_exists=True)
```
