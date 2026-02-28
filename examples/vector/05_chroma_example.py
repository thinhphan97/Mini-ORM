"""Chroma adapter example (optional dependency)."""

from __future__ import annotations

from mini_orm import ChromaVectorStore, VectorMetric, VectorRecord, VectorRepository

def metric_preview(metric: VectorMetric) -> None:
    store = ChromaVectorStore(path=":memory:")
    repo = VectorRepository(
        store,
        f"chroma_metric_{metric.value}",
        dimension=2,
        metric=metric,
        auto_create=True,
        overwrite=True,
    )
    repo.upsert(
        [
            VectorRecord("a", [1.0, 0.0], {"kind": "a"}),
            VectorRecord("b", [2.0, 0.0], {"kind": "b"}),
            VectorRecord("c", [0.0, 1.0], {"kind": "c"}),
        ]
    )
    print(f"Metric={metric.value} ->", [hit.id for hit in repo.query([1.0, 0.0], top_k=3)])

def main() -> None:
    try:
        # path=":memory:" uses EphemeralClient in this adapter.
        store = ChromaVectorStore(path=":memory:")
    except ImportError as exc:
        print("Chroma example skipped:", exc)
        print("Install dependency: pip install chromadb")
        return

    repo = VectorRepository(
        store,
        "chroma_users",
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
            # payload=None is also supported by this adapter.
            VectorRecord("u4", [0.8, 0.2, 0.0], None),
        ]
    )

    print("Fetch by IDs:", repo.fetch(ids=["u2", "u1"]))
    print("Top hits:", repo.query([1.0, 0.0, 0.0], top_k=3))
    print(
        "Filtered hits group=a:",
        repo.query([1.0, 0.0, 0.0], top_k=5, filters={"group": "a"}),
    )

    deleted = repo.delete(["u2", "missing"])
    print("Deleted count:", deleted)
    print("Remaining:", repo.fetch())

    # Optional metric demos.
    metric_preview(VectorMetric.DOT)
    metric_preview(VectorMetric.L2)

    # For persistent local storage:
    # persistent_store = ChromaVectorStore(path="./.chroma")

if __name__ == "__main__":
    main()
