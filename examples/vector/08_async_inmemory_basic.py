"""Async in-memory vector repository example."""

from __future__ import annotations

import asyncio

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
