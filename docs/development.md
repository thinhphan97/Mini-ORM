# Development

## Run tests

```bash
python -m unittest discover -s tests -p "test_*.py"
```

## Start external services for examples/tests

`docker-compose.yml` includes:

- PostgreSQL + pgvector (`postgres` service)
- MySQL (`mysql` service)
- Qdrant (`qdrant` service)
- Chroma (`chroma` service)

Start:

```bash
docker compose up -d
docker compose ps
```

Stop:

```bash
docker compose down
```

If you need to reset DB data volumes:

```bash
docker compose down -v
```

Run host-server vector integration tests (Qdrant/Chroma):

```bash
MINI_ORM_VECTOR_HOST_TESTS=1 python -m unittest \
  tests.test_vector_qdrant_store \
  tests.test_vector_chroma_store
```

Vector tests are split by backend for easier observation:

- `tests/test_vector_inmemory_repository.py`
- `tests/test_vector_pgvector_store.py`
- `tests/test_vector_qdrant_store.py`
- `tests/test_vector_chroma_store.py`
- `tests/test_vector_faiss_store.py`

## Build library artifacts

```bash
pip install -r requirements-build.txt
./scripts/build_lib.sh
```

Artifacts are generated under `dist/` (`.whl` and `.tar.gz`).

## Publish artifacts

```bash
pip install -r requirements-publish.txt
python3 -m twine upload dist/*
```

## Build docs locally

```bash
pip install -r requirements-docs.txt
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
