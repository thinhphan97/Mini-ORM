"""Qdrant adapter example (optional dependency)."""

from __future__ import annotations

from mini_orm import QdrantVectorStore, VectorMetric, VectorRecord, VectorRepository

def main() -> None:
    try:
        # location=":memory:" keeps everything in-process for easy local demo.
        store = QdrantVectorStore(location=":memory:")
    except ImportError as exc:
        print("Qdrant example skipped:", exc)
        print("Install dependency: pip install qdrant-client")
        return

    repo = VectorRepository(
        store,
        "qdrant_users",
        dimension=3,
        metric=VectorMetric.COSINE,
        auto_create=True,
        overwrite=True,
    )

    # Qdrant requires UUID string IDs.
    u1 = "11111111-1111-1111-1111-111111111111"
    u2 = "22222222-2222-2222-2222-222222222222"
    u3 = "33333333-3333-3333-3333-333333333333"

    repo.upsert(
        [
            VectorRecord(u1, [1.0, 0.0, 0.0], {"group": "a"}),
            VectorRecord(u2, [0.0, 1.0, 0.0], {"group": "b"}),
            VectorRecord(u3, [0.9, 0.1, 0.0], {"group": "a"}),
        ]
    )

    print("Fetch by IDs:", repo.fetch(ids=[u2, u1]))
    print("Top hits:", repo.query([1.0, 0.0, 0.0], top_k=2))
    print(
        "Filtered hits group=a:",
        repo.query([1.0, 0.0, 0.0], top_k=5, filters={"group": "a"}),
    )

    deleted = repo.delete([u2, "44444444-4444-4444-4444-444444444444"])
    print("Deleted count:", deleted)
    print("Remaining:", repo.fetch())

    # Example validation: invalid UUID should raise ValueError.
    try:
        repo.upsert([VectorRecord("not-a-uuid", [1.0, 0.0, 0.0], {"group": "x"})])
    except ValueError as exc:
        print("Expected UUID policy error:", exc)

    # For persistent local storage, use for example:
    # store = QdrantVectorStore(location="./.qdrant")

if __name__ == "__main__":
    main()
