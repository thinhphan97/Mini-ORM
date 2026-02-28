"""Faiss adapter example (optional dependency)."""

from __future__ import annotations

from mini_orm import FaissVectorStore, VectorMetric, VectorRecord, VectorRepository

def metric_preview(metric: VectorMetric) -> None:
    store = FaissVectorStore()
    repo = VectorRepository(
        store,
        f"faiss_metric_{metric.value}",
        dimension=2,
        metric=metric,
        auto_create=True,
        overwrite=True,
    )
    repo.upsert(
        [
            VectorRecord("a", [1.0, 0.0]),
            VectorRecord("b", [2.0, 0.0]),
            VectorRecord("c", [0.0, 1.0]),
        ]
    )
    print(f"Metric={metric.value} ->", [hit.id for hit in repo.query([1.0, 0.0], top_k=3)])

def main() -> None:
    try:
        store = FaissVectorStore()
    except ImportError as exc:
        print("Faiss example skipped:", exc)
        print("Install dependency: pip install faiss-cpu numpy")
        return

    repo = VectorRepository(
        store,
        "faiss_users",
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

    deleted = repo.delete(["u2", "missing"])
    print("Deleted count:", deleted)
    print("Remaining:", repo.fetch())

    # Faiss adapter does not support payload filters.
    try:
        repo.query([1.0, 0.0, 0.0], top_k=1, filters={"group": "a"})
    except NotImplementedError as exc:
        print("Expected filter unsupported error:", exc)

    # Optional metric demos.
    metric_preview(VectorMetric.DOT)
    metric_preview(VectorMetric.L2)

if __name__ == "__main__":
    main()
