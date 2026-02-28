"""PostgreSQL integration example: SQL Repository + PgVectorStore."""

from __future__ import annotations

import importlib
import os
from dataclasses import dataclass, field
from typing import Any, Optional

from mini_orm import (
    Database,
    PgVectorStore,
    PostgresDialect,
    Repository,
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

@dataclass
class Document:
    __table__ = "pgvector_docs"

    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    title: str = field(default="")
    category: str = field(default="")

def main() -> None:
    connect = _load_connect()
    if connect is None:
        print("Integration example skipped: psycopg/psycopg2 not installed.")
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
        print("Integration example skipped:", exc)
        return

    db = Database(conn, PostgresDialect())
    try:
        sql_repo = Repository[Document](db, Document, auto_schema=True)
        vector_store = PgVectorStore(db)
        vector_repo = VectorRepository(
            vector_store,
            "pgvector_doc_embeddings",
            dimension=3,
            metric=VectorMetric.COSINE,
            auto_create=True,
            overwrite=True,
        )

        doc1 = sql_repo.insert(Document(title="ORM intro", category="guide"))
        doc2 = sql_repo.insert(Document(title="API reference", category="docs"))
        doc3 = sql_repo.insert(Document(title="ORM advanced", category="guide"))

        vector_repo.upsert(
            [
                VectorRecord(str(doc1.id), [1.0, 0.0, 0.0], {"category": doc1.category}),
                VectorRecord(str(doc2.id), [0.0, 1.0, 0.0], {"category": doc2.category}),
                VectorRecord(str(doc3.id), [0.9, 0.1, 0.0], {"category": doc3.category}),
            ]
        )

        hits = vector_repo.query([1.0, 0.0, 0.0], top_k=3, filters={"category": "guide"})
        docs_by_id = {str(doc.id): doc for doc in sql_repo.list()}

        print("Vector hits mapped to SQL rows:")
        for hit in hits:
            print("-", docs_by_id.get(hit.id), "score=", round(hit.score, 6))
    finally:
        db.close()

if __name__ == "__main__":
    main()
