"""Vector payload codec example (serialize/deserialize for metadata)."""

from __future__ import annotations

import sys
from enum import Enum
from pathlib import Path

# Allow running this script directly from repository root.
PROJECT_ROOT = next(
    (parent for parent in Path(__file__).resolve().parents if (parent / "mini_orm").exists()),
    None,
)
if PROJECT_ROOT and str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

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
