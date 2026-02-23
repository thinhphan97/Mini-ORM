"""In-memory vector metric behavior and filter examples."""

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


def metric_demo(metric: VectorMetric) -> None:
    # Each metric can produce different ranking for the same vectors.
    store = InMemoryVectorStore()
    repo = VectorRepository(store, f"metric_{metric.value}", dimension=2, metric=metric)

    repo.upsert(
        [
            VectorRecord("a", [1.0, 0.0], {"kind": "alpha"}),
            VectorRecord("b", [2.0, 0.0], {"kind": "beta"}),
            VectorRecord("c", [0.0, 1.0], {"kind": "alpha"}),
        ]
    )

    hits = repo.query([1.0, 0.0], top_k=3)
    print(f"Metric={metric.value} -> order={[hit.id for hit in hits]}")


def filter_demo() -> None:
    store = InMemoryVectorStore()
    repo = VectorRepository(store, "filters", dimension=3, metric=VectorMetric.COSINE)
    repo.upsert(
        [
            VectorRecord("u1", [1.0, 0.0, 0.0], {"team": "red", "level": 1}),
            VectorRecord("u2", [0.9, 0.1, 0.0], {"team": "red", "level": 2}),
            VectorRecord("u3", [0.0, 1.0, 0.0], {"team": "blue", "level": 1}),
        ]
    )

    # Exact equality filters over payload.
    red_hits = repo.query([1.0, 0.0, 0.0], top_k=5, filters={"team": "red"})
    red_level_1 = repo.query(
        [1.0, 0.0, 0.0],
        top_k=5,
        filters={"team": "red", "level": 1},
    )

    # top_k <= 0 returns [] by design.
    empty_hits = repo.query([1.0, 0.0, 0.0], top_k=0)

    print("Filter team=red ->", [h.id for h in red_hits])
    print("Filter team=red, level=1 ->", [h.id for h in red_level_1])
    print("top_k=0 ->", empty_hits)


def main() -> None:
    metric_demo(VectorMetric.COSINE)
    metric_demo(VectorMetric.DOT)
    metric_demo(VectorMetric.L2)
    filter_demo()


if __name__ == "__main__":
    main()
