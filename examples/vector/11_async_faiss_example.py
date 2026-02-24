"""Async Faiss adapter example (optional dependency)."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

# Allow running this script directly from repository root.
PROJECT_ROOT = next(
    (parent for parent in Path(__file__).resolve().parents if (parent / "mini_orm").exists()),
    None,
)
if PROJECT_ROOT and str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from mini_orm import AsyncVectorRepository, FaissVectorStore, VectorMetric, VectorRecord


async def metric_preview(metric: VectorMetric) -> None:
    """Preview metric behavior for `AsyncVectorRepository` with `FaissVectorStore`."""
    try:
        store = FaissVectorStore()
    except ImportError as exc:
        print("Faiss metric preview skipped:", exc)
        return
    repo = AsyncVectorRepository(
        store,
        f"faiss_async_metric_{metric.value}",
        dimension=2,
        metric=metric,
        auto_create=True,
        overwrite=True,
    )
    await repo.upsert(
        [
            VectorRecord("a", [1.0, 0.0]),
            VectorRecord("b", [2.0, 0.0]),
            VectorRecord("c", [0.0, 1.0]),
        ]
    )
    hits = await repo.query([1.0, 0.0], top_k=3)
    print(f"Metric={metric.value} ->", [hit.id for hit in hits])


async def main() -> None:
    try:
        store = FaissVectorStore()
    except ImportError as exc:
        print("Faiss async example skipped:", exc)
        print("Install dependency: pip install faiss-cpu numpy")
        return

    repo = AsyncVectorRepository(
        store,
        "faiss_async_users",
        dimension=3,
        metric=VectorMetric.COSINE,
        auto_create=True,
        overwrite=True,
    )

    await repo.upsert(
        [
            VectorRecord("u1", [1.0, 0.0, 0.0], {"group": "a"}),
            VectorRecord("u2", [0.0, 1.0, 0.0], {"group": "b"}),
            VectorRecord("u3", [0.9, 0.1, 0.0], {"group": "a"}),
        ]
    )

    print("Fetch by IDs:", await repo.fetch(ids=["u2", "u1"]))
    print("Top hits:", await repo.query([1.0, 0.0, 0.0], top_k=2))

    deleted = await repo.delete(["u2", "missing"])
    print("Deleted count:", deleted)
    print("Remaining:", await repo.fetch())

    try:
        await repo.query([1.0, 0.0, 0.0], top_k=1, filters={"group": "a"})
    except NotImplementedError as exc:
        print("Expected filter unsupported error:", exc)

    await metric_preview(VectorMetric.DOT)
    await metric_preview(VectorMetric.L2)


if __name__ == "__main__":
    asyncio.run(main())
