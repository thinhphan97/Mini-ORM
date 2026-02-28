SHELL := /usr/bin/env bash
.SHELLFLAGS := -eo pipefail -c

PYTHON ?= python3
PIP ?= $(PYTHON) -m pip
UNITTEST ?= $(PYTHON) -m unittest
DOCKER_COMPOSE ?= docker compose
PROJECT_ROOT := $(CURDIR)

SQL_EXAMPLES := $(sort $(wildcard examples/sql/[0-9][0-9]_*.py))
VECTOR_EXAMPLES := $(sort $(wildcard examples/vector/[0-9][0-9]_*.py))

.DEFAULT_GOAL := help

.PHONY: help \
	deps-build deps-docs deps-release \
	test test-vector test-vector-host test-pgvector-host \
	build-lib release-check release-lib \
	compose-up compose-down compose-reset compose-ps compose-logs \
	examples examples-sql examples-vector \
	example-sql example-vector

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage: make <target>\n\nTargets:\n"} /^[a-zA-Z0-9_.-]+:.*##/ {printf "  %-22s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

deps-build: ## Install build dependencies
	$(PIP) install -r requirements-build.txt

deps-docs: ## Install documentation dependencies
	$(PIP) install -r requirements-docs.txt

deps-release: ## Install release/publish dependencies
	$(PIP) install -r requirements-publish.txt

test: ## Run full test suite
	$(UNITTEST) discover -s tests -p "test_*.py"

test-vector: ## Run vector-focused tests only
	$(UNITTEST) discover -s tests -p "test_vector_*.py"

test-vector-host: ## Run host-server vector tests (Qdrant/Chroma/PgVector SQL+vector)
	MINI_ORM_VECTOR_HOST_TESTS=1 $(UNITTEST) \
		tests.test_vector_qdrant_store \
		tests.test_vector_chroma_store \
		tests.test_vector_pgvector_store

test-pgvector-host: ## Run PgVector SQL+vector integration tests on host PostgreSQL
	MINI_ORM_VECTOR_HOST_TESTS=1 $(UNITTEST) tests.test_vector_pgvector_store

build-lib: deps-build ## Build wheel and sdist into dist/
	./scripts/build_lib.sh

release-check: ## Validate built artifacts with twine check
	$(PYTHON) -m twine check dist/*

release-lib: build-lib deps-release release-check ## Upload package artifacts to index (twine upload)
	$(PYTHON) -m twine upload dist/*

compose-up: ## Start Docker compose services (postgres/mysql/qdrant/chroma)
	$(DOCKER_COMPOSE) up -d

compose-down: ## Stop and remove Docker compose services
	$(DOCKER_COMPOSE) down

compose-reset: ## Stop services and remove all compose volumes
	$(DOCKER_COMPOSE) down -v

compose-ps: ## Show compose service status
	$(DOCKER_COMPOSE) ps

compose-logs: ## Show compose logs (set SERVICE=name, default all)
	@if [[ -n "$(SERVICE)" ]]; then \
		$(DOCKER_COMPOSE) logs -f "$(SERVICE)"; \
	else \
		$(DOCKER_COMPOSE) logs -f; \
	fi

examples: examples-sql examples-vector ## Run all SQL and vector examples

examples-sql: ## Run all SQL examples
	@for file in $(SQL_EXAMPLES); do \
		echo "==> Running $$file"; \
		PYTHONPATH="$(PROJECT_ROOT):$${PYTHONPATH:-}" $(PYTHON) "$$file"; \
	done

examples-vector: ## Run all vector examples
	@for file in $(VECTOR_EXAMPLES); do \
		echo "==> Running $$file"; \
		PYTHONPATH="$(PROJECT_ROOT):$${PYTHONPATH:-}" $(PYTHON) "$$file"; \
	done

example-sql: ## Run one SQL example (usage: make example-sql FILE=examples/sql/01_basic_crud.py)
	@if [[ -z "$(FILE)" ]]; then \
		echo "Missing FILE. Example: make example-sql FILE=examples/sql/01_basic_crud.py"; \
		exit 1; \
	fi
	PYTHONPATH="$(PROJECT_ROOT):$${PYTHONPATH:-}" $(PYTHON) "$(FILE)"

example-vector: ## Run one vector example (usage: make example-vector FILE=examples/vector/01_inmemory_basic.py)
	@if [[ -z "$(FILE)" ]]; then \
		echo "Missing FILE. Example: make example-vector FILE=examples/vector/01_inmemory_basic.py"; \
		exit 1; \
	fi
	PYTHONPATH="$(PROJECT_ROOT):$${PYTHONPATH:-}" $(PYTHON) "$(FILE)"
