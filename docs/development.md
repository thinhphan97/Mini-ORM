# Development

## Run tests

```bash
make test
make test-vector
```

## Start external services for examples/tests

`docker-compose.yml` includes:

- PostgreSQL + pgvector (`postgres` service)
- MySQL (`mysql` service)
- Qdrant (`qdrant` service)
- Chroma (`chroma` service)

Start services:

```bash
make compose-up
make compose-ps
```

Stop services:

```bash
make compose-down
```

If you need to reset DB data volumes:

```bash
make compose-reset
```

Run host-server vector integration tests (Qdrant/Chroma/PgVector):

```bash
# Required for tests.test_vector_pgvector_store:
export MINI_ORM_PG_HOST=localhost
export MINI_ORM_PG_PORT=5432
export MINI_ORM_PG_USER=postgres
export MINI_ORM_PG_PASSWORD=password
export MINI_ORM_PG_DATABASE=postgres

make test-vector-host
```

The target above automatically sets `MINI_ORM_VECTOR_HOST_TESTS=1`
(`RUN_HOST_VECTOR_TESTS` in the test modules is derived from this flag).

Run only PgVector host SQL+vector integration test:

```bash
make test-pgvector-host
```

Vector tests are split by backend for easier observation:

- `tests/test_vector_inmemory_repository.py`
- `tests/test_vector_pgvector_store.py`
- `tests/test_vector_qdrant_store.py`
- `tests/test_vector_chroma_store.py`
- `tests/test_vector_faiss_store.py`

## Build library artifacts

```bash
make build-lib
```

Artifacts are generated under `dist/` (`.whl` and `.tar.gz`).

## Publish artifacts

```bash
make release-check
make release-lib
```

## Build docs locally

```bash
make deps-docs
mkdocs serve
```

## Project layout

```text
mini_orm/
  core/            # business logic, contracts, query/schema/repository
  ports/           # adapters: SQL DB-API and vector backends
docs/              # MkDocs source
tests/             # unit tests
```
