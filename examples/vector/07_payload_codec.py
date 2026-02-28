"""Vector payload codec example (serialize/deserialize for metadata)."""

from __future__ import annotations

from enum import Enum

from mini_orm import (
    InMemoryVectorStore,
    JsonVectorPayloadCodec,
    VectorRecord,
    VectorRepository,
)

class Status(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"

def main() -> None:
    store = InMemoryVectorStore()
    repo = VectorRepository(
        store,
        "users",
        dimension=2,
        payload_codec=JsonVectorPayloadCodec(),
    )

    repo.upsert(
        [
            VectorRecord(
                id="u1",
                vector=[1.0, 0.0],
                payload={"status": Status.ACTIVE, "profile": {"tier": "gold"}},
            ),
            VectorRecord(
                id="u2",
                vector=[0.0, 1.0],
                payload={"status": Status.INACTIVE, "profile": {"tier": "silver"}},
            ),
        ]
    )

    raw = store.fetch("users", ids=["u1"])[0]
    print("Raw backend payload:", raw.payload)

    loaded = repo.fetch(ids=["u1"])[0]
    print("Decoded payload:", loaded.payload)
    print("Decoded status type:", type(loaded.payload["status"]).__name__)

    filtered = repo.query([1.0, 0.0], top_k=5, filters={"status": Status.ACTIVE})
    print("Filter by enum status:", [item.id for item in filtered])

if __name__ == "__main__":
    main()
