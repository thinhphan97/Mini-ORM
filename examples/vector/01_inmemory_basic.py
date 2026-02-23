"""Basic vector flow with InMemoryVectorStore."""

from __future__ import annotations

import sys
from pathlib import Path

# Allow running this script directly from repository root.
PROJECT_ROOT = next(
    (parent for parent in Path(__file__).resolve().parents if (parent / "mini_orm").exists()),
    None,
)
if PROJECT_ROOT and str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from mini_orm import InMemoryVectorStore, VectorMetric, VectorRecord, VectorRepository


def main() -> None:
    # Create in-memory store and repository.
    store = InMemoryVectorStore()
    repo = VectorRepository(
        store,
        "users",
        dimension=3,
        metric=VectorMetric.COSINE,
        auto_create=True,
    )

    # Upsert records (insert new and update existing by ID).
    repo.upsert(
        [
            VectorRecord(id="u1", vector=[1.0, 0.0, 0.0], payload={"group": "a"}),
            VectorRecord(id="u2", vector=[0.0, 1.0, 0.0], payload={"group": "b"}),
            VectorRecord(id="u3", vector=[0.9, 0.1, 0.0], payload={"group": "a"}),
        ]
    )

    print("All records:", repo.fetch())
    print("Selected records:", repo.fetch(ids=["u2", "u1", "missing"]))

    # Query nearest vectors.
    print("Top 2 for [1,0,0]:", repo.query([1.0, 0.0, 0.0], top_k=2))

    # Filter by payload equality.
    print(
        "Filtered by payload group=a:",
        repo.query([1.0, 0.0, 0.0], top_k=5, filters={"group": "a"}),
    )

    # Delete by IDs; return count of deleted rows.
    deleted = repo.delete(["u2", "missing"])
    print("Deleted count:", deleted)
    print("Remaining records:", repo.fetch())


if __name__ == "__main__":
    main()
