"""VectorRepository lifecycle and expected error cases."""

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

from mini_orm import InMemoryVectorStore, VectorIdPolicy, VectorRecord, VectorRepository


def expect_error(label: str, fn) -> None:  # noqa: ANN001
    try:
        fn()
    except Exception as exc:  # noqa: BLE001
        print(f"[OK] {label}: {type(exc).__name__}: {exc}")
    else:
        print(f"[UNEXPECTED] {label}: no exception raised")


def lifecycle_demo() -> None:
    store = InMemoryVectorStore()

    # auto_create=False means collection is not created until create_collection().
    repo = VectorRepository(store, "lazy_collection", dimension=2, auto_create=False)
    expect_error("upsert before create_collection", lambda: repo.upsert([VectorRecord("x", [1, 0])]))

    repo.create_collection()
    repo.upsert([VectorRecord("x", [1, 0], {"source": "manual_create"})])
    print("After manual create:", repo.fetch())

    # overwrite=True recreates collection and clears previous records.
    recreated = VectorRepository(
        store,
        "lazy_collection",
        dimension=2,
        auto_create=True,
        overwrite=True,
    )
    print("After overwrite recreate:", recreated.fetch())


def validation_demo() -> None:
    repo = VectorRepository(InMemoryVectorStore(), "dim_case", dimension=2, auto_create=True)
    expect_error("dimension mismatch on upsert", lambda: repo.upsert([VectorRecord("bad", [1, 0, 0])]))
    expect_error("dimension mismatch on query", lambda: repo.query([1, 0, 0], top_k=1))


def filter_capability_demo() -> None:
    class NoFilterStore(InMemoryVectorStore):
        supports_filters = False

    repo = VectorRepository(NoFilterStore(), "no_filters", dimension=2, auto_create=True)
    repo.upsert([VectorRecord("r1", [1, 0], {"group": "a"})])
    expect_error(
        "query with filters on unsupported backend",
        lambda: repo.query([1, 0], top_k=1, filters={"group": "a"}),
    )


def id_policy_demo() -> None:
    class UUIDOnlyStore(InMemoryVectorStore):
        id_policy = VectorIdPolicy.UUID

    repo = VectorRepository(UUIDOnlyStore(), "uuid_only", dimension=2, auto_create=True)
    expect_error("upsert invalid UUID id", lambda: repo.upsert([VectorRecord("not-a-uuid", [1, 0])]))
    expect_error("fetch invalid UUID id", lambda: repo.fetch(ids=["not-a-uuid"]))
    expect_error("delete invalid UUID id", lambda: repo.delete(["not-a-uuid"]))


def main() -> None:
    lifecycle_demo()
    validation_demo()
    filter_capability_demo()
    id_policy_demo()


if __name__ == "__main__":
    main()
