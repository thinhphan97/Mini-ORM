# PgVector Adapter

`PgVectorStore` is an adapter for PostgreSQL with the
[pgvector](https://github.com/pgvector/pgvector) extension.

## Requirements

- PostgreSQL with `vector` extension available.
- A DB-API PostgreSQL driver (for example `psycopg` / `psycopg2`).

## Usage

```python
import psycopg

from mini_orm import Database, PgVectorStore, PostgresDialect, VectorRecord, VectorRepository

conn = psycopg.connect("host=localhost port=5432 dbname=postgres user=postgres password=password")
db = Database(conn, PostgresDialect())

store = PgVectorStore(db)
repo = VectorRepository(store, "vector_items", dimension=4, metric="cosine")

repo.upsert(
    [
        VectorRecord(id="1", vector=[0.1, 0.2, 0.3, 0.4], payload={"type": "doc"}),
    ]
)

top = repo.query([0.1, 0.2, 0.25, 0.4], top_k=5)
print(top)
```

## Notes

- Supported metrics: `cosine`, `dot`, `l2`.
- `filters` are translated into JSONB exact-match checks.
- Vector and SQL repositories can share the same `Database`/transaction source.
- Collection name format supports `table` or `schema.table`.
