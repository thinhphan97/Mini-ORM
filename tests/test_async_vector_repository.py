from __future__ import annotations

import importlib
import inspect
import tempfile
import unittest
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from unittest.mock import patch
from uuid import UUID

from mini_orm import (
    AsyncVectorRepository,
    ChromaVectorStore,
    FaissVectorStore,
    IdentityVectorPayloadCodec,
    InMemoryVectorStore,
    JsonVectorPayloadCodec,
    QdrantVectorStore,
    VectorIdPolicy,
    VectorMetric,
    VectorPayloadCodec,
    VectorRecord,
)


class PayloadStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"


def _module_available(name: str) -> bool:
    try:
        importlib.import_module(name)
    except Exception:
        return False
    return True


HAS_QDRANT = _module_available("qdrant_client")
HAS_CHROMA = _module_available("chromadb")
HAS_FAISS = _module_available("faiss") and _module_available("numpy")


class _AsyncInMemoryVectorStore:
    supports_filters = True
    id_policy = VectorIdPolicy.ANY

    def __init__(self) -> None:
        self._delegate = InMemoryVectorStore()

    async def create_collection(self, *args, **kwargs) -> None:  # noqa: ANN002,ANN003
        self._delegate.create_collection(*args, **kwargs)

    async def upsert(self, collection, records) -> None:  # noqa: ANN001
        self._delegate.upsert(collection, records)

    async def query(self, collection, vector, *, top_k=10, filters=None):  # noqa: ANN001,ANN201
        return self._delegate.query(collection, vector, top_k=top_k, filters=filters)

    async def fetch(self, collection, ids=None):  # noqa: ANN001,ANN201
        return self._delegate.fetch(collection, ids=ids)

    async def delete(self, collection, ids):  # noqa: ANN001,ANN201
        return self._delegate.delete(collection, ids)


class AsyncVectorRepositoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_repository_surface_matches_sync_names(self) -> None:
        method_names = ["create_collection", "upsert", "query", "fetch", "delete"]
        for name in method_names:
            self.assertTrue(hasattr(AsyncVectorRepository, name))
            self.assertTrue(inspect.iscoroutinefunction(getattr(AsyncVectorRepository, name)))

    async def test_works_with_existing_sync_vector_store(self) -> None:
        store = InMemoryVectorStore()
        repo = AsyncVectorRepository(
            store,
            "products",
            dimension=3,
            metric=VectorMetric.COSINE,
            auto_create=True,
        )
        await repo.upsert(
            [
                VectorRecord("p1", [1, 0, 0], {"group": "a"}),
                VectorRecord("p2", [0, 1, 0], {"group": "b"}),
            ]
        )
        hits = await repo.query([1, 0, 0], top_k=1)
        loaded = await repo.fetch(ids=["p1"])
        deleted = await repo.delete(["p2"])

        self.assertEqual([item.id for item in hits], ["p1"])
        self.assertEqual([item.id for item in loaded], ["p1"])
        self.assertEqual(deleted, 1)

    async def test_async_store_requires_manual_create_when_auto_create_enabled(self) -> None:
        store = _AsyncInMemoryVectorStore()
        with self.assertRaises(ValueError):
            AsyncVectorRepository(store, "items", dimension=2, auto_create=True)

        repo = AsyncVectorRepository(store, "items", dimension=2, auto_create=False)
        await repo.create_collection()
        await repo.upsert([VectorRecord("x", [1, 0], {"status": "ok"})])
        self.assertEqual((await repo.fetch(ids=["x"]))[0].id, "x")

    async def test_payload_codec_and_filters(self) -> None:
        store = InMemoryVectorStore()
        repo = AsyncVectorRepository(
            store,
            "json_codec_async",
            dimension=2,
            auto_create=True,
            payload_codec=JsonVectorPayloadCodec(),
        )
        await repo.upsert(
            [
                VectorRecord(
                    "r1",
                    [1, 0],
                    {"status": PayloadStatus.ACTIVE, "meta": {"tier": "gold"}},
                )
            ]
        )

        raw = store.fetch("json_codec_async", ids=["r1"])[0]
        self.assertIsInstance(raw.payload["status"], str)

        hits = await repo.query([1, 0], top_k=1, filters={"status": PayloadStatus.ACTIVE})
        self.assertEqual([item.id for item in hits], ["r1"])
        self.assertEqual(hits[0].payload["status"], PayloadStatus.ACTIVE)

    async def test_uuid_policy_validation(self) -> None:
        class _UUIDOnlyStore(InMemoryVectorStore):
            id_policy = VectorIdPolicy.UUID

        store = _UUIDOnlyStore()
        repo = AsyncVectorRepository(store, "uuid_policy", dimension=2, auto_create=True)

        with self.assertRaisesRegex(ValueError, "requires UUID"):
            await repo.upsert([VectorRecord("not-a-uuid", [1, 0])])

        with self.assertRaisesRegex(ValueError, "requires UUID"):
            await repo.fetch(ids=["not-a-uuid"])
        with self.assertRaisesRegex(ValueError, "requires UUID"):
            await repo.delete(["not-a-uuid"])

    async def test_vector_repository_validates_uuid_policy(self) -> None:
        class _UUIDOnlyStore(InMemoryVectorStore):
            id_policy = VectorIdPolicy.UUID

        store = _UUIDOnlyStore()
        repo = AsyncVectorRepository(store, "uuid_policy_2", dimension=2, auto_create=True)

        with self.assertRaisesRegex(ValueError, "requires UUID"):
            await repo.upsert([VectorRecord("not-a-uuid", [1, 0])])
        with self.assertRaisesRegex(ValueError, "requires UUID"):
            await repo.fetch(ids=["not-a-uuid"])
        with self.assertRaisesRegex(ValueError, "requires UUID"):
            await repo.delete(["not-a-uuid"])

    async def test_create_collection_validation(self) -> None:
        with self.assertRaises(ValueError):
            AsyncVectorRepository(InMemoryVectorStore(), "bad_dim", dimension=0)
        with self.assertRaises(ValueError):
            AsyncVectorRepository(InMemoryVectorStore(), "bad_metric", dimension=2, metric="manhattan")

        store = InMemoryVectorStore()
        repo = AsyncVectorRepository(store, "users", dimension=3, auto_create=False)
        await repo.create_collection()
        with self.assertRaises(ValueError):
            await repo.create_collection()
        await repo.create_collection(overwrite=True)

    async def test_upsert_fetch_query_delete_flow(self) -> None:
        store = InMemoryVectorStore()
        repo = AsyncVectorRepository(store, "users", dimension=3, auto_create=True)
        await repo.upsert(
            [
                VectorRecord("u1", [1, 0, 0], {"group": "a"}),
                VectorRecord("u2", [0, 1, 0], {"group": "b"}),
                VectorRecord("u3", [0.9, 0.1, 0], {"group": "a"}),
            ]
        )

        all_records = await repo.fetch()
        selected_records = await repo.fetch(ids=["u2", "u1", "missing"])
        hits = await repo.query([1, 0, 0], top_k=2)
        filtered_hits = await repo.query([1, 0, 0], top_k=5, filters={"group": "a"})
        deleted = await repo.delete(["u2", "missing"])
        remaining = await repo.fetch()

        self.assertEqual(len(all_records), 3)
        self.assertEqual([item.id for item in selected_records], ["u2", "u1"])
        self.assertEqual([hit.id for hit in hits], ["u1", "u3"])
        self.assertEqual([hit.id for hit in filtered_hits], ["u1", "u3"])
        self.assertEqual(deleted, 1)
        self.assertEqual([item.id for item in remaining], ["u1", "u3"])

    async def test_dimension_and_collection_validation(self) -> None:
        store = InMemoryVectorStore()
        repo = AsyncVectorRepository(store, "users", dimension=3, auto_create=True)
        with self.assertRaises(ValueError):
            await repo.upsert([VectorRecord("x", [1, 2], None)])
        with self.assertRaises(ValueError):
            await repo.query([1, 2], top_k=3)

        lazy_repo = AsyncVectorRepository(InMemoryVectorStore(), "missing_collection", dimension=3, auto_create=False)
        with self.assertRaises(KeyError):
            await lazy_repo.fetch()

    async def test_query_behaviors_with_metrics_and_topk(self) -> None:
        store = InMemoryVectorStore()
        repo = AsyncVectorRepository(store, "users", dimension=3, auto_create=True)
        await repo.upsert(
            [
                VectorRecord("u1", [1, 0, 0]),
                VectorRecord("u2", [0.5, 0.5, 0]),
                VectorRecord("u3", [0, 1, 0]),
            ]
        )
        self.assertEqual(await repo.query([1, 0, 0], top_k=0), [])

    async def test_all_supported_metrics_with_enum(self) -> None:
        for metric in (VectorMetric.COSINE, VectorMetric.DOT, VectorMetric.L2):
            store = InMemoryVectorStore()
            repo = AsyncVectorRepository(store, "metric_case", dimension=2, metric=metric, auto_create=True)
            await repo.upsert(
                [
                    VectorRecord("a", [1, 0]),
                    VectorRecord("b", [2, 0]),
                    VectorRecord("c", [0, 1]),
                ]
            )
            hits = await repo.query([1, 0], top_k=3)
            self.assertEqual(len(hits), 3)
            if metric == VectorMetric.DOT:
                self.assertEqual([hit.id for hit in hits], ["b", "a", "c"])
            elif metric == VectorMetric.L2:
                self.assertEqual([hit.id for hit in hits], ["a", "b", "c"])
            else:
                self.assertEqual(hits[0].id, "a")

    async def test_auto_create_and_manual_create(self) -> None:
        store = InMemoryVectorStore()
        repo = AsyncVectorRepository(store, "products", dimension=4, auto_create=True)
        await repo.upsert([VectorRecord("p1", [1, 0, 0, 0], {"kind": "a"})])
        hits = await repo.query([1, 0, 0, 0], top_k=1)
        self.assertEqual(hits[0].id, "p1")

        missing_store = InMemoryVectorStore()
        lazy_repo = AsyncVectorRepository(missing_store, "lazy", dimension=2, auto_create=False)
        with self.assertRaises(KeyError):
            await lazy_repo.upsert([VectorRecord("x", [1, 0])])

        await lazy_repo.create_collection()
        await lazy_repo.upsert([VectorRecord("x", [1, 0])])
        self.assertEqual((await lazy_repo.fetch())[0].id, "x")

    async def test_metric_enum_support(self) -> None:
        store = InMemoryVectorStore()
        repo = AsyncVectorRepository(
            store,
            "enum_metric",
            dimension=2,
            metric=VectorMetric.COSINE,
            auto_create=True,
        )
        await repo.upsert([VectorRecord("e1", [1, 0])])
        self.assertEqual((await repo.query([1, 0], top_k=1))[0].id, "e1")

    async def test_overwrite_recreates_collection(self) -> None:
        store = InMemoryVectorStore()
        repo = AsyncVectorRepository(store, "events", dimension=2, auto_create=True)
        await repo.upsert([VectorRecord("e1", [1, 1])])
        self.assertEqual(len(await repo.fetch()), 1)

        recreated = AsyncVectorRepository(
            store,
            "events",
            dimension=2,
            auto_create=True,
            overwrite=True,
        )
        self.assertEqual(await recreated.fetch(), [])

    async def test_query_filters_raise_when_backend_unsupported(self) -> None:
        class NoFilterInMemoryStore(InMemoryVectorStore):
            supports_filters = False

        store = NoFilterInMemoryStore()
        repo = AsyncVectorRepository(store, "no_filters", dimension=2, auto_create=True)
        await repo.upsert([VectorRecord("r1", [1, 0], {"group": "a"})])

        with self.assertRaises(NotImplementedError):
            await repo.query([1, 0], top_k=1, filters={"group": "a"})

    async def test_vector_repository_validates_dimension(self) -> None:
        store = InMemoryVectorStore()
        repo = AsyncVectorRepository(store, "dim", dimension=2, auto_create=True)
        with self.assertRaises(ValueError):
            await repo.upsert([VectorRecord("x", [1, 0, 0])])
        with self.assertRaises(ValueError):
            await repo.query([1, 0, 0], top_k=1)

    async def test_payload_codec_identity_keeps_behavior_unchanged(self) -> None:
        store = InMemoryVectorStore()
        repo = AsyncVectorRepository(
            store,
            "identity_codec",
            dimension=2,
            auto_create=True,
            payload_codec=IdentityVectorPayloadCodec(),
        )
        payload = {"group": "a", "meta": {"k": 1}}
        await repo.upsert([VectorRecord("r1", [1, 0], payload)])

        loaded = (await repo.fetch(ids=["r1"]))[0]
        self.assertEqual(loaded.payload, payload)

        raw = store.fetch("identity_codec", ids=["r1"])[0]
        self.assertEqual(raw.payload, payload)

    async def test_payload_codec_json_roundtrip_and_filter_encoding(self) -> None:
        store = InMemoryVectorStore()
        codec = JsonVectorPayloadCodec()
        repo = AsyncVectorRepository(
            store,
            "json_codec",
            dimension=2,
            auto_create=True,
            payload_codec=codec,
        )

        payload = {
            "status": PayloadStatus.ACTIVE,
            "meta": {"views": 10, "flags": ["a", "b"]},
            "created_at": datetime(2026, 2, 24, 12, 0, 0),
            "created_on": date(2026, 2, 24),
            "alarm_at": time(9, 30, 0),
            "price": Decimal("10.50"),
            "owner_id": UUID("11111111-1111-1111-1111-111111111111"),
            "coords": (1, 2, 3),
            "labels": {"x", "y"},
            "raw": b"\x01\x02",
        }

        await repo.upsert([VectorRecord("r1", [1, 0], payload)])

        raw = store.fetch("json_codec", ids=["r1"])[0]
        self.assertIsInstance(raw.payload["status"], str)
        self.assertTrue(raw.payload["status"].startswith(codec.prefix))
        self.assertIsInstance(raw.payload["meta"], str)
        self.assertTrue(raw.payload["meta"].startswith(codec.prefix))

        loaded = (await repo.fetch(ids=["r1"]))[0]
        self.assertIsInstance(loaded.payload["status"], PayloadStatus)
        self.assertEqual(loaded.payload["status"], PayloadStatus.ACTIVE)
        self.assertEqual(loaded.payload["meta"], {"views": 10, "flags": ["a", "b"]})
        self.assertEqual(loaded.payload["created_at"], datetime(2026, 2, 24, 12, 0, 0))
        self.assertEqual(loaded.payload["created_on"], date(2026, 2, 24))
        self.assertEqual(loaded.payload["alarm_at"], time(9, 30, 0))
        self.assertEqual(loaded.payload["price"], Decimal("10.50"))
        self.assertEqual(
            loaded.payload["owner_id"],
            UUID("11111111-1111-1111-1111-111111111111"),
        )
        self.assertEqual(loaded.payload["coords"], (1, 2, 3))
        self.assertEqual(set(loaded.payload["labels"]), {"x", "y"})
        self.assertEqual(loaded.payload["raw"], b"\x01\x02")

        hits = await repo.query([1, 0], top_k=1, filters={"status": PayloadStatus.ACTIVE})
        self.assertEqual([item.id for item in hits], ["r1"])
        self.assertIsInstance(hits[0].payload["status"], PayloadStatus)
        self.assertEqual(hits[0].payload["status"], PayloadStatus.ACTIVE)

        complex_hits = await repo.query(
            [1, 0], top_k=1, filters={"meta": {"views": 10, "flags": ["a", "b"]}}
        )
        self.assertEqual([item.id for item in complex_hits], ["r1"])

    async def test_payload_codec_json_escapes_prefixed_plain_string(self) -> None:
        codec = JsonVectorPayloadCodec()
        prefixed = f"{codec.prefix}already-prefixed-string"
        store = InMemoryVectorStore()
        repo = AsyncVectorRepository(
            store,
            "json_codec_escape",
            dimension=2,
            auto_create=True,
            payload_codec=codec,
        )
        await repo.upsert([VectorRecord("r1", [1, 0], {"note": prefixed})])
        loaded = (await repo.fetch(ids=["r1"]))[0]
        self.assertEqual(loaded.payload["note"], prefixed)

    async def test_payload_codec_json_preserves_user_dict_with_codec_key(self) -> None:
        codec = JsonVectorPayloadCodec()
        user_dict = {"__miniorm_codec__": "date", "value": "2026-02-24"}
        store = InMemoryVectorStore()
        repo = AsyncVectorRepository(
            store,
            "json_codec_dict_collision",
            dimension=2,
            auto_create=True,
            payload_codec=codec,
        )
        await repo.upsert([VectorRecord("r1", [1, 0], {"meta": user_dict})])

        raw = store.fetch("json_codec_dict_collision", ids=["r1"])[0]
        self.assertIsInstance(raw.payload["meta"], str)
        self.assertIn('"__miniorm_codec__":"dict"', raw.payload["meta"])

        loaded = (await repo.fetch(ids=["r1"]))[0]
        self.assertEqual(loaded.payload["meta"], user_dict)

        hits = await repo.query([1, 0], top_k=1, filters={"meta": user_dict})
        self.assertEqual([item.id for item in hits], ["r1"])

    async def test_payload_codec_json_rejects_unsupported_value_type(self) -> None:
        class Unsupported:
            pass

        store = InMemoryVectorStore()
        repo = AsyncVectorRepository(
            store,
            "json_codec_invalid",
            dimension=2,
            auto_create=True,
            payload_codec=JsonVectorPayloadCodec(),
        )
        with self.assertRaisesRegex(TypeError, "cannot serialize payload value"):
            await repo.upsert([VectorRecord("r1", [1, 0], {"bad": Unsupported()})])

    async def test_payload_codec_json_enum_falls_back_when_type_unresolvable(self) -> None:
        codec = JsonVectorPayloadCodec()
        payload = {
            "status": (
                '__miniorm_json__:{"__miniorm_codec__":"enum",'
                '"class":"unknown.module:MissingEnum","value":"active"}'
            )
        }
        decoded = codec.deserialize(payload)
        self.assertEqual(decoded["status"], "active")

    async def test_custom_payload_codec_is_used_for_payload_and_filters(self) -> None:
        class UpperCaseCodec:
            def serialize(self, payload):
                if payload is None:
                    return None
                return {
                    key: value.upper() if isinstance(value, str) else value
                    for key, value in payload.items()
                }

            def deserialize(self, payload):
                if payload is None:
                    return None
                return {
                    key: value.lower() if isinstance(value, str) else value
                    for key, value in payload.items()
                }

            def serialize_filters(self, filters):
                if filters is None:
                    return None
                return {
                    key: value.upper() if isinstance(value, str) else value
                    for key, value in filters.items()
                }

        codec: VectorPayloadCodec = UpperCaseCodec()
        store = InMemoryVectorStore()
        repo = AsyncVectorRepository(
            store,
            "custom_codec",
            dimension=2,
            auto_create=True,
            payload_codec=codec,
        )
        await repo.upsert([VectorRecord("r1", [1, 0], {"name": "alice"})])

        raw = store.fetch("custom_codec", ids=["r1"])[0]
        self.assertEqual(raw.payload["name"], "ALICE")

        loaded = (await repo.fetch(ids=["r1"]))[0]
        self.assertEqual(loaded.payload["name"], "alice")

        hits = await repo.query([1, 0], top_k=1, filters={"name": "alice"})
        self.assertEqual([item.id for item in hits], ["r1"])


@unittest.skipUnless(HAS_QDRANT, "qdrant_client is not installed")
class AsyncQdrantVectorFlowTests(unittest.IsolatedAsyncioTestCase):
    async def test_qdrant_memory_flow_upsert_query_fetch_delete(self) -> None:
        store = QdrantVectorStore(location=":memory:")
        repo = AsyncVectorRepository(
            store,
            "qdrant_async_users",
            dimension=3,
            metric=VectorMetric.COSINE,
            auto_create=True,
            overwrite=True,
        )

        u1 = "11111111-1111-1111-1111-111111111111"
        u2 = "22222222-2222-2222-2222-222222222222"
        u3 = "33333333-3333-3333-3333-333333333333"
        await repo.upsert(
            [
                VectorRecord(u1, [1, 0, 0], {"group": "a"}),
                VectorRecord(u2, [0, 1, 0], {"group": "b"}),
                VectorRecord(u3, [0.9, 0.1, 0], {"group": "a"}),
            ]
        )

        fetched = await repo.fetch(ids=[u2, u1])
        hits = await repo.query([1, 0, 0], top_k=2)
        filtered = await repo.query([1, 0, 0], top_k=5, filters={"group": "a"})
        deleted = await repo.delete([u2])
        remaining = await repo.fetch()

        self.assertEqual([item.id for item in fetched], [u2, u1])
        self.assertEqual([hit.id for hit in hits], [u1, u3])
        self.assertEqual([hit.id for hit in filtered], [u1, u3])
        self.assertEqual(deleted, 1)
        self.assertEqual({item.id for item in remaining}, {u1, u3})

    async def test_qdrant_path_flow_upsert_query_fetch_delete(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mini_orm_qdrant_path_") as db_path:
            store = QdrantVectorStore(location=db_path)
            repo = AsyncVectorRepository(
                store,
                "qdrant_async_path_users",
                dimension=3,
                metric=VectorMetric.COSINE,
                auto_create=True,
                overwrite=True,
            )

            u1 = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
            u2 = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
            await repo.upsert(
                [
                    VectorRecord(u1, [1, 0, 0], {"kind": "x"}),
                    VectorRecord(u2, [0, 1, 0], {"kind": "y"}),
                ]
            )

            fetched = await repo.fetch(ids=[u1])
            hits = await repo.query([1, 0, 0], top_k=1)
            deleted = await repo.delete([u2])
            remaining = await repo.fetch()

            self.assertEqual([item.id for item in fetched], [u1])
            self.assertEqual([hit.id for hit in hits], [u1])
            self.assertEqual(deleted, 1)
            self.assertEqual([item.id for item in remaining], [u1])

    async def test_qdrant_supported_metrics_dot_and_l2(self) -> None:
        metric_cases = [
            (
                VectorMetric.DOT,
                [
                    VectorRecord("11111111-1111-1111-1111-111111111111", [1, 0]),
                    VectorRecord("22222222-2222-2222-2222-222222222222", [2, 0]),
                    VectorRecord("33333333-3333-3333-3333-333333333333", [0, 1]),
                ],
                [1, 0],
                "22222222-2222-2222-2222-222222222222",
            ),
            (
                VectorMetric.L2,
                [
                    VectorRecord("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", [1, 0]),
                    VectorRecord("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", [2, 0]),
                    VectorRecord("cccccccc-cccc-cccc-cccc-cccccccccccc", [0, 1]),
                ],
                [1, 0],
                "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            ),
        ]

        for metric, records, query_vector, expected_first in metric_cases:
            store = QdrantVectorStore(location=":memory:")
            repo = AsyncVectorRepository(
                store,
                f"qdrant_async_metric_{metric.value}",
                dimension=2,
                metric=metric,
                auto_create=True,
                overwrite=True,
            )
            await repo.upsert(records)
            hits = await repo.query(query_vector, top_k=3)
            self.assertEqual(hits[0].id, expected_first)
            self.assertEqual(len(hits), 3)

    async def test_qdrant_delete_counts_only_existing_records(self) -> None:
        store = QdrantVectorStore(location=":memory:")
        repo = AsyncVectorRepository(
            store,
            "qdrant_async_delete_count",
            dimension=2,
            metric=VectorMetric.COSINE,
            auto_create=True,
            overwrite=True,
        )
        existing = "11111111-1111-1111-1111-111111111111"
        missing = "22222222-2222-2222-2222-222222222222"
        await repo.upsert([VectorRecord(existing, [1, 0], {"kind": "x"})])

        deleted = await repo.delete([existing, missing])
        self.assertEqual(deleted, 1)

    async def test_qdrant_requires_uuid_ids(self) -> None:
        store = QdrantVectorStore(location=":memory:")
        repo = AsyncVectorRepository(
            store,
            "qdrant_async_uuid_policy",
            dimension=2,
            metric=VectorMetric.COSINE,
            auto_create=True,
            overwrite=True,
        )

        with self.assertRaisesRegex(ValueError, "requires UUID"):
            await repo.upsert([VectorRecord("not-a-uuid", [1, 0], {"kind": "x"})])
        with self.assertRaisesRegex(ValueError, "requires UUID"):
            await repo.fetch(ids=["not-a-uuid"])
        with self.assertRaisesRegex(ValueError, "requires UUID"):
            await repo.delete(["not-a-uuid"])


@unittest.skipUnless(HAS_CHROMA, "chromadb is not installed")
class AsyncChromaVectorFlowTests(unittest.IsolatedAsyncioTestCase):
    async def test_chroma_memory_flow_upsert_query_fetch_delete(self) -> None:
        store = ChromaVectorStore(path=":memory:")
        repo = AsyncVectorRepository(
            store,
            "chroma_async_users",
            dimension=3,
            metric=VectorMetric.COSINE,
            auto_create=True,
            overwrite=True,
        )

        await repo.upsert(
            [
                VectorRecord("u1", [1, 0, 0], {"group": "a"}),
                VectorRecord("u2", [0, 1, 0], {"group": "b"}),
                VectorRecord("u3", [0.9, 0.1, 0], {"group": "a"}),
            ]
        )

        fetched = await repo.fetch(ids=["u2", "u1"])
        hits = await repo.query([1, 0, 0], top_k=2)
        filtered = await repo.query([1, 0, 0], top_k=5, filters={"group": "a"})
        deleted = await repo.delete(["u2"])
        remaining = await repo.fetch()

        self.assertEqual([item.id for item in fetched], ["u2", "u1"])
        self.assertEqual(hits[0].id, "u1")
        self.assertEqual([hit.id for hit in filtered], ["u1", "u3"])
        self.assertEqual(deleted, 1)
        self.assertEqual({item.id for item in remaining}, {"u1", "u3"})

    async def test_chroma_supported_metrics_dot_and_l2(self) -> None:
        metric_cases = [
            (
                VectorMetric.DOT,
                [
                    VectorRecord("a", [1, 0], {"kind": "a"}),
                    VectorRecord("b", [2, 0], {"kind": "b"}),
                    VectorRecord("c", [0, 1], {"kind": "c"}),
                ],
                [1, 0],
                "b",
            ),
            (
                VectorMetric.L2,
                [
                    VectorRecord("x", [1, 0], {"kind": "x"}),
                    VectorRecord("y", [2, 0], {"kind": "y"}),
                    VectorRecord("z", [0, 1], {"kind": "z"}),
                ],
                [1, 0],
                "x",
            ),
        ]

        for metric, records, query_vector, expected_first in metric_cases:
            store = ChromaVectorStore(path=":memory:")
            repo = AsyncVectorRepository(
                store,
                f"chroma_async_metric_{metric.value}",
                dimension=2,
                metric=metric,
                auto_create=True,
                overwrite=True,
            )
            await repo.upsert(records)
            hits = await repo.query(query_vector, top_k=3)
            self.assertEqual(hits[0].id, expected_first)
            self.assertEqual(len(hits), 3)

    async def test_chroma_upsert_allows_none_payload(self) -> None:
        store = ChromaVectorStore(path=":memory:")
        repo = AsyncVectorRepository(
            store,
            "chroma_async_none_payload",
            dimension=2,
            metric=VectorMetric.COSINE,
            auto_create=True,
            overwrite=True,
        )
        await repo.upsert([VectorRecord("u1", [1, 0], None)])

        fetched = await repo.fetch(ids=["u1"])
        self.assertEqual(len(fetched), 1)
        self.assertIsNone(fetched[0].payload)

    async def test_chroma_delete_counts_only_existing_records(self) -> None:
        store = ChromaVectorStore(path=":memory:")
        repo = AsyncVectorRepository(
            store,
            "chroma_async_delete_count",
            dimension=2,
            metric=VectorMetric.COSINE,
            auto_create=True,
            overwrite=True,
        )
        await repo.upsert([VectorRecord("u1", [1, 0], {"kind": "x"})])

        deleted = await repo.delete(["u1", "missing"])
        self.assertEqual(deleted, 1)


@unittest.skipUnless(HAS_FAISS, "faiss/numpy is not installed")
class AsyncFaissVectorFlowTests(unittest.IsolatedAsyncioTestCase):
    async def test_faiss_memory_flow_upsert_query_fetch_delete(self) -> None:
        store = FaissVectorStore()
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
                VectorRecord("u1", [1, 0, 0], {"group": "a"}),
                VectorRecord("u2", [0, 1, 0], {"group": "b"}),
                VectorRecord("u3", [0.9, 0.1, 0], {"group": "a"}),
            ]
        )

        fetched = await repo.fetch(ids=["u2", "u1"])
        hits = await repo.query([1, 0, 0], top_k=2)
        deleted = await repo.delete(["u2"])
        remaining = await repo.fetch()

        self.assertEqual([item.id for item in fetched], ["u2", "u1"])
        self.assertEqual([hit.id for hit in hits], ["u1", "u3"])
        self.assertEqual(deleted, 1)
        self.assertEqual([item.id for item in remaining], ["u1", "u3"])

    async def test_faiss_supported_metrics_dot_and_l2(self) -> None:
        metric_cases = [
            (
                VectorMetric.DOT,
                [
                    VectorRecord("a", [1, 0]),
                    VectorRecord("b", [2, 0]),
                    VectorRecord("c", [0, 1]),
                ],
                [1, 0],
                ["b", "a", "c"],
            ),
            (
                VectorMetric.L2,
                [
                    VectorRecord("x", [1, 0]),
                    VectorRecord("y", [2, 0]),
                    VectorRecord("z", [0, 1]),
                ],
                [1, 0],
                ["x", "y", "z"],
            ),
        ]

        for metric, records, query_vector, expected_order in metric_cases:
            store = FaissVectorStore()
            repo = AsyncVectorRepository(
                store,
                f"faiss_async_metric_{metric.value}",
                dimension=2,
                metric=metric,
                auto_create=True,
                overwrite=True,
            )
            await repo.upsert(records)
            hits = await repo.query(query_vector, top_k=3)
            self.assertEqual([hit.id for hit in hits], expected_order)

    async def test_faiss_query_filters_raise_not_supported(self) -> None:
        store = FaissVectorStore()
        repo = AsyncVectorRepository(
            store,
            "faiss_async_filter_unsupported",
            dimension=2,
            metric=VectorMetric.COSINE,
            auto_create=True,
            overwrite=True,
        )
        await repo.upsert([VectorRecord("u1", [1, 0], {"group": "a"})])

        with self.assertRaisesRegex(NotImplementedError, "does not support payload filters"):
            await repo.query([1, 0], top_k=1, filters={"group": "a"})


class AsyncQdrantAdapterOptionalTests(unittest.TestCase):
    def test_qdrant_store_requires_dependency(self) -> None:
        real_import = __import__

        def fake_import(name, *args, **kwargs):
            if name == "qdrant_client":
                raise ImportError("simulated missing qdrant-client")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=fake_import):
            with self.assertRaises(ImportError):
                QdrantVectorStore(location=tempfile.mkdtemp(prefix="mini_orm_qdrant_"))


class AsyncChromaAdapterOptionalTests(unittest.TestCase):
    def test_chroma_store_requires_dependency(self) -> None:
        real_import = __import__

        def fake_import(name, *args, **kwargs):
            if name == "chromadb":
                raise ImportError("simulated missing chromadb")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=fake_import):
            with self.assertRaises(ImportError):
                ChromaVectorStore(path=tempfile.mkdtemp(prefix="mini_orm_chroma_"))


class AsyncFaissAdapterOptionalTests(unittest.TestCase):
    def test_faiss_store_requires_dependency(self) -> None:
        real_import = __import__

        def fake_import(name, *args, **kwargs):
            if name in {"faiss", "numpy"}:
                raise ImportError(f"simulated missing {name}")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=fake_import):
            with self.assertRaises(ImportError):
                FaissVectorStore()


if __name__ == "__main__":
    unittest.main()
