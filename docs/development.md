# Development

## Run tests

```bash
python -m unittest discover -s tests -p "test_*.py"
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
