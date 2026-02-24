# Mini ORM Examples

This folder contains runnable scripts that demonstrate every major feature
currently supported by this codebase.

Run examples from project root:

```bash
python examples/sql/01_basic_crud.py
python examples/sql/02_query_conditions.py
python examples/sql/03_repository_utilities.py
python examples/sql/04_schema_and_indexes.py
python examples/sql/05_validation_and_error_cases.py
python examples/sql/06_dialect_preview.py
python examples/sql/07_relations_create_and_query.py

python examples/vector/01_inmemory_basic.py
python examples/vector/02_inmemory_metrics_and_filters.py
python examples/vector/03_repository_lifecycle_and_errors.py
python examples/vector/04_qdrant_example.py
python examples/vector/05_chroma_example.py
python examples/vector/06_faiss_example.py
```

## SQL examples

- `examples/sql/01_basic_crud.py`
  - Dataclass model + auto PK.
  - `apply_schema(...)`.
  - `insert`, `get`, `update`, `delete`, `list`.

- `examples/sql/02_query_conditions.py`
  - Full condition factory coverage:
    `eq`, `ne`, `lt`, `le`, `gt`, `ge`, `like`, `is_null`, `is_not_null`, `in_`.
  - Group expressions: `and_`, `or_`, `not_`.
  - Sorting + pagination.
  - `count` and `exists`.

- `examples/sql/03_repository_utilities.py`
  - `insert_many`, `update_where`, `delete_where`, `get_or_create`.

- `examples/sql/04_schema_and_indexes.py`
  - `create_table_sql`, `create_index_sql`, `create_indexes_sql`, `create_schema_sql`.
  - Field-level index metadata + `__indexes__`.
  - `apply_schema(..., if_not_exists=True)` idempotent flow.

- `examples/sql/05_validation_and_error_cases.py`
  - Common error/validation paths with expected exceptions.

- `examples/sql/06_dialect_preview.py`
  - SQL generation differences between `SQLiteDialect`, `PostgresDialect`,
    and `MySQLDialect`.

- `examples/sql/07_relations_create_and_query.py`
  - Declare relation intent on FK metadata:
    - minimum: `metadata={"fk": (Author, "id")}`
    - optional naming: `relation="author"`, `related_name="posts"`
  - Demonstrates inferred relations:
    - child side `Post.author` (`belongs_to`)
    - parent side `Author.posts` (`has_many`)
  - Create parent/child graph in one call via `repo.create(..., relations=...)`.
  - Query rows with included relations via `get_related(...)` and `list_related(...)`.

## Vector examples

- `examples/vector/01_inmemory_basic.py`
  - Basic `VectorRepository` flow with `InMemoryVectorStore`.

- `examples/vector/02_inmemory_metrics_and_filters.py`
  - Metric behavior (`cosine`, `dot`, `l2`) and payload filters.

- `examples/vector/03_repository_lifecycle_and_errors.py`
  - `auto_create=False`, manual `create_collection`, `overwrite`,
    dimension checks, filter support, UUID policy.

- `examples/vector/04_qdrant_example.py`
  - Qdrant usage and UUID ID policy behavior.
  - Optional dependency script (safe to run without qdrant-client installed).

- `examples/vector/05_chroma_example.py`
  - Chroma usage with in-memory collection and filters.
  - Optional dependency script.

- `examples/vector/06_faiss_example.py`
  - Faiss usage and unsupported-filter behavior.
  - Optional dependency script.

## Optional dependency install

```bash
pip install qdrant-client
pip install chromadb
pip install faiss-cpu numpy
```
