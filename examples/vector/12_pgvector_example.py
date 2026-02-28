"""PgVector adapter example (requires PostgreSQL + pgvector extension)."""

from __future__ import annotations

import importlib
import os
from typing import Any

from mini_orm import (
    Database,
    PgVectorStore,
    PostgresDialect,
    VectorMetric,
    VectorRecord,
    VectorRepository,
)

def _load_connect() -> Any:
    for module_name in ("psycopg", "psycopg2"):
        try:
            module = importlib.import_module(module_name)
        except (ModuleNotFoundError, ImportError):
            continue
        connect = getattr(module, "connect", None)
        if connect is not None:
            return connect
    return None

def main() -> None:
    connect = _load_connect()
    if connect is None:
        print("PgVector example skipped: psycopg/psycopg2 not installed.")
        print("Install dependency: pip install psycopg")
        return

    password = os.getenv(
        "MINI_ORM_PG_PASSWORD",
        os.getenv("PGPASSWORD", os.getenv("POSTGRES_PASSWORD", "password")),
    )
    params = {
        "host": os.getenv("MINI_ORM_PG_HOST", os.getenv("PGHOST", "localhost")),
        "port": int(os.getenv("MINI_ORM_PG_PORT", os.getenv("PGPORT", "5432"))),
        "user": os.getenv("MINI_ORM_PG_USER", os.getenv("PGUSER", "postgres")),
        "password": password,
        "dbname": os.getenv("MINI_ORM_PG_DATABASE", os.getenv("PGDATABASE", "postgres")),
    }

    try:
        conn = connect(**params)
    except Exception as exc:  # noqa: BLE001 - pragmatic cross-driver OperationalError handling
        print("PgVector example skipped:", exc)
        return

    db = Database(conn, PostgresDialect())
    try:
        store = PgVectorStore(db)
        repo = VectorRepository(
            store,
            "pgvector_users",
            dimension=3,
            metric=VectorMetric.COSINE,
            auto_create=True,
            overwrite=True,
        )

        repo.upsert(
            [
                VectorRecord("u1", [1.0, 0.0, 0.0], {"group": "a"}),
                VectorRecord("u2", [0.0, 1.0, 0.0], {"group": "b"}),
                VectorRecord("u3", [0.9, 0.1, 0.0], {"group": "a"}),
            ]
        )

        print("Fetch by IDs:", repo.fetch(ids=["u2", "u1"]))
        print("Top hits:", repo.query([1.0, 0.0, 0.0], top_k=2))
        print(
            "Filtered:",
            repo.query([1.0, 0.0, 0.0], top_k=5, filters={"group": "a"}),
        )
    finally:
        db.close()

if __name__ == "__main__":
    main()
