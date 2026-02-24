"""Async in-memory vector repository example."""

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

from mini_orm import AsyncVectorRepository, InMemoryVectorStore, VectorRecord


async def main() -> None:
    store = InMemoryVectorStore()
    repo = AsyncVectorRepository(store, "items", dimension=3)

    await repo.upsert(
        [
            VectorRecord(id="1", vector=[0.1, 0.2, 0.3], payload={"group": "a"}),
            VectorRecord(id="2", vector=[0.9, 0.1, 0.0], payload={"group": "b"}),
        ]
    )
    hits = await repo.query([0.1, 0.2, 0.25], top_k=2)
    print("hits:", hits)


if __name__ == "__main__":
    asyncio.run(main())
