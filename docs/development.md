# Development

## Run tests

```bash
python -m unittest discover -s tests -p "test_*.py"
```

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
