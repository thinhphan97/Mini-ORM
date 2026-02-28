from __future__ import annotations

import unittest
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from uuid import UUID

from mini_orm import (
    IdentityVectorPayloadCodec,
    InMemoryVectorStore,
    JsonVectorPayloadCodec,
    VectorIdPolicy,
    VectorMetric,
    VectorPayloadCodec,
    VectorRecord,
    VectorRepository,
)


class PayloadStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"


class InMemoryVectorStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = InMemoryVectorStore()
        self.store.create_collection("users", dimension=3, metric="cosine")

    def test_create_collection_validation(self) -> None:
        self.store.create_collection(
            "dot_enum",
            dimension=3,
            metric=VectorMetric.DOT,
        )

        with self.assertRaises(ValueError):
            self.store.create_collection("bad_dim", dimension=0)
        with self.assertRaises(ValueError):
            self.store.create_collection("bad_metric", dimension=3, metric="manhattan")
        with self.assertRaises(ValueError):
            self.store.create_collection("users", dimension=3)

        self.store.create_collection("users", dimension=3, overwrite=True)

    def test_upsert_fetch_query_delete_flow(self) -> None:
        self.store.upsert(
            "users",
            [
                VectorRecord("u1", [1, 0, 0], {"group": "a"}),
                VectorRecord("u2", [0, 1, 0], {"group": "b"}),
                VectorRecord("u3", [0.9, 0.1, 0], {"group": "a"}),
            ],
        )

        all_records = self.store.fetch("users")
        selected_records = self.store.fetch("users", ids=["u2", "u1", "missing"])
        hits = self.store.query("users", [1, 0, 0], top_k=2)
        filtered_hits = self.store.query(
            "users", [1, 0, 0], top_k=5, filters={"group": "a"}
        )
        deleted = self.store.delete("users", ["u2", "missing"])
        remaining = self.store.fetch("users")

        self.assertEqual(len(all_records), 3)
        self.assertEqual([item.id for item in selected_records], ["u2", "u1"])
        self.assertEqual([hit.id for hit in hits], ["u1", "u3"])
        self.assertEqual([hit.id for hit in filtered_hits], ["u1", "u3"])
        self.assertEqual(deleted, 1)
        self.assertEqual([item.id for item in remaining], ["u1", "u3"])

    def test_dimension_and_collection_validation(self) -> None:
        with self.assertRaises(ValueError):
            self.store.upsert("users", [VectorRecord("x", [1, 2], None)])
        with self.assertRaises(ValueError):
            self.store.query("users", [1, 2], top_k=3)
        with self.assertRaises(KeyError):
            self.store.fetch("missing_collection")

    def test_query_behaviors_with_metrics_and_topk(self) -> None:
        self.store.upsert(
            "users",
            [
                VectorRecord("u1", [1, 0, 0]),
                VectorRecord("u2", [0.5, 0.5, 0]),
                VectorRecord("u3", [0, 1, 0]),
            ],
        )
        self.assertEqual(self.store.query("users", [1, 0, 0], top_k=0), [])

        dot_store = InMemoryVectorStore()
        dot_store.create_collection("dot", dimension=2, metric="dot")
        dot_store.upsert(
            "dot",
            [
                VectorRecord("d1", [1, 0]),
                VectorRecord("d2", [0.8, 0.6]),
            ],
        )
        dot_hits = dot_store.query("dot", [1, 0], top_k=2)
        self.assertEqual([hit.id for hit in dot_hits], ["d1", "d2"])

        l2_store = InMemoryVectorStore()
        l2_store.create_collection("l2", dimension=2, metric="l2")
        l2_store.upsert(
            "l2",
            [
                VectorRecord("l1", [0, 0]),
                VectorRecord("l2", [2, 0]),
            ],
        )
        l2_hits = l2_store.query("l2", [1, 0], top_k=2)
        self.assertEqual([hit.id for hit in l2_hits], ["l1", "l2"])

    def test_all_supported_metrics_with_enum(self) -> None:
        for metric in (VectorMetric.COSINE, VectorMetric.DOT, VectorMetric.L2):
            store = InMemoryVectorStore()
            store.create_collection("metric_case", dimension=2, metric=metric)
            store.upsert(
                "metric_case",
                [
                    VectorRecord("a", [1, 0]),
                    VectorRecord("b", [2, 0]),
                    VectorRecord("c", [0, 1]),
                ],
            )
            hits = store.query("metric_case", [1, 0], top_k=3)
            self.assertEqual(len(hits), 3)
            if metric == VectorMetric.DOT:
                self.assertEqual([hit.id for hit in hits], ["b", "a", "c"])
            elif metric == VectorMetric.L2:
                self.assertEqual([hit.id for hit in hits], ["a", "b", "c"])
            else:
                self.assertEqual(hits[0].id, "a")


class VectorRepositoryTests(unittest.TestCase):
    def test_auto_create_and_manual_create(self) -> None:
        store = InMemoryVectorStore()
        repo = VectorRepository(store, "products", dimension=4, auto_create=True)

        repo.upsert([VectorRecord("p1", [1, 0, 0, 0], {"kind": "a"})])
        hits = repo.query([1, 0, 0, 0], top_k=1)
        self.assertEqual(hits[0].id, "p1")

        missing_store = InMemoryVectorStore()
        lazy_repo = VectorRepository(
            missing_store,
            "lazy",
            dimension=2,
            auto_create=False,
        )
        with self.assertRaises(KeyError):
            lazy_repo.upsert([VectorRecord("x", [1, 0])])

        lazy_repo.create_collection()
        lazy_repo.upsert([VectorRecord("x", [1, 0])])
        self.assertEqual(lazy_repo.fetch()[0].id, "x")

    def test_metric_enum_support(self) -> None:
        store = InMemoryVectorStore()
        repo = VectorRepository(
            store,
            "enum_metric",
            dimension=2,
            metric=VectorMetric.COSINE,
            auto_create=True,
        )
        repo.upsert([VectorRecord("e1", [1, 0])])
        self.assertEqual(repo.query([1, 0], top_k=1)[0].id, "e1")

    def test_overwrite_recreates_collection(self) -> None:
        store = InMemoryVectorStore()
        repo = VectorRepository(store, "events", dimension=2, auto_create=True)
        repo.upsert([VectorRecord("e1", [1, 1])])
        self.assertEqual(len(repo.fetch()), 1)

        recreated = VectorRepository(
            store,
            "events",
            dimension=2,
            auto_create=True,
            overwrite=True,
        )
        self.assertEqual(recreated.fetch(), [])

    def test_query_filters_raise_when_backend_unsupported(self) -> None:
        class NoFilterInMemoryStore(InMemoryVectorStore):
            supports_filters = False

        store = NoFilterInMemoryStore()
        repo = VectorRepository(store, "no_filters", dimension=2, auto_create=True)
        repo.upsert([VectorRecord("r1", [1, 0], {"group": "a"})])

        with self.assertRaises(NotImplementedError):
            repo.query([1, 0], top_k=1, filters={"group": "a"})

    def test_vector_repository_validates_dimension(self) -> None:
        store = InMemoryVectorStore()
        repo = VectorRepository(store, "dim", dimension=2, auto_create=True)
        with self.assertRaises(ValueError):
            repo.upsert([VectorRecord("x", [1, 0, 0])])
        with self.assertRaises(ValueError):
            repo.query([1, 0, 0], top_k=1)

    def test_vector_repository_validates_uuid_policy(self) -> None:
        class UUIDOnlyInMemoryStore(InMemoryVectorStore):
            id_policy = VectorIdPolicy.UUID

        store = UUIDOnlyInMemoryStore()
        repo = VectorRepository(store, "uuid_policy", dimension=2, auto_create=True)

        with self.assertRaisesRegex(ValueError, "requires UUID"):
            repo.upsert([VectorRecord("not-a-uuid", [1, 0])])
        with self.assertRaisesRegex(ValueError, "requires UUID"):
            repo.fetch(ids=["not-a-uuid"])
        with self.assertRaisesRegex(ValueError, "requires UUID"):
            repo.delete(["not-a-uuid"])

    def test_payload_codec_identity_keeps_behavior_unchanged(self) -> None:
        store = InMemoryVectorStore()
        repo = VectorRepository(
            store,
            "identity_codec",
            dimension=2,
            auto_create=True,
            payload_codec=IdentityVectorPayloadCodec(),
        )
        payload = {"group": "a", "meta": {"k": 1}}
        repo.upsert([VectorRecord("r1", [1, 0], payload)])

        loaded = repo.fetch(ids=["r1"])[0]
        self.assertEqual(loaded.payload, payload)

        raw = store.fetch("identity_codec", ids=["r1"])[0]
        self.assertEqual(raw.payload, payload)

    def test_payload_codec_json_roundtrip_and_filter_encoding(self) -> None:
        store = InMemoryVectorStore()
        codec = JsonVectorPayloadCodec()
        repo = VectorRepository(
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

        repo.upsert([VectorRecord("r1", [1, 0], payload)])

        raw = store.fetch("json_codec", ids=["r1"])[0]
        self.assertIsInstance(raw.payload["status"], str)
        self.assertTrue(raw.payload["status"].startswith(codec.prefix))
        self.assertIsInstance(raw.payload["meta"], str)
        self.assertTrue(raw.payload["meta"].startswith(codec.prefix))

        loaded = repo.fetch(ids=["r1"])[0]
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

        hits = repo.query([1, 0], top_k=1, filters={"status": PayloadStatus.ACTIVE})
        self.assertEqual([item.id for item in hits], ["r1"])
        self.assertIsInstance(hits[0].payload["status"], PayloadStatus)
        self.assertEqual(hits[0].payload["status"], PayloadStatus.ACTIVE)

        complex_hits = repo.query(
            [1, 0],
            top_k=1,
            filters={"meta": {"views": 10, "flags": ["a", "b"]}},
        )
        self.assertEqual([item.id for item in complex_hits], ["r1"])

    def test_payload_codec_json_escapes_prefixed_plain_string(self) -> None:
        codec = JsonVectorPayloadCodec()
        prefixed = f"{codec.prefix}already-prefixed-string"
        store = InMemoryVectorStore()
        repo = VectorRepository(
            store,
            "json_codec_escape",
            dimension=2,
            auto_create=True,
            payload_codec=codec,
        )
        repo.upsert([VectorRecord("r1", [1, 0], {"note": prefixed})])
        loaded = repo.fetch(ids=["r1"])[0]
        self.assertEqual(loaded.payload["note"], prefixed)

    def test_payload_codec_json_preserves_user_dict_with_codec_key(self) -> None:
        codec = JsonVectorPayloadCodec()
        user_dict = {"__miniorm_codec__": "date", "value": "2026-02-24"}
        store = InMemoryVectorStore()
        repo = VectorRepository(
            store,
            "json_codec_dict_collision",
            dimension=2,
            auto_create=True,
            payload_codec=codec,
        )
        repo.upsert([VectorRecord("r1", [1, 0], {"meta": user_dict})])

        raw = store.fetch("json_codec_dict_collision", ids=["r1"])[0]
        self.assertIsInstance(raw.payload["meta"], str)
        self.assertIn('"__miniorm_codec__":"dict"', raw.payload["meta"])

        loaded = repo.fetch(ids=["r1"])[0]
        self.assertEqual(loaded.payload["meta"], user_dict)

        hits = repo.query([1, 0], top_k=1, filters={"meta": user_dict})
        self.assertEqual([item.id for item in hits], ["r1"])

    def test_payload_codec_json_rejects_unsupported_value_type(self) -> None:
        class Unsupported:
            pass

        store = InMemoryVectorStore()
        repo = VectorRepository(
            store,
            "json_codec_invalid",
            dimension=2,
            auto_create=True,
            payload_codec=JsonVectorPayloadCodec(),
        )
        with self.assertRaisesRegex(TypeError, "cannot serialize payload value"):
            repo.upsert([VectorRecord("r1", [1, 0], {"bad": Unsupported()})])

    def test_payload_codec_json_enum_falls_back_when_type_unresolvable(self) -> None:
        codec = JsonVectorPayloadCodec()
        payload = {
            "status": (
                '__miniorm_json__:{"__miniorm_codec__":"enum",'
                '"class":"unknown.module:MissingEnum","value":"active"}'
            )
        }
        decoded = codec.deserialize(payload)
        self.assertEqual(decoded["status"], "active")

    def test_custom_payload_codec_is_used_for_payload_and_filters(self) -> None:
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
        repo = VectorRepository(
            store,
            "custom_codec",
            dimension=2,
            auto_create=True,
            payload_codec=codec,
        )
        repo.upsert([VectorRecord("r1", [1, 0], {"name": "alice"})])

        raw = store.fetch("custom_codec", ids=["r1"])[0]
        self.assertEqual(raw.payload["name"], "ALICE")

        loaded = repo.fetch(ids=["r1"])[0]
        self.assertEqual(loaded.payload["name"], "alice")

        hits = repo.query([1, 0], top_k=1, filters={"name": "alice"})
        self.assertEqual([item.id for item in hits], ["r1"])


if __name__ == "__main__":
    unittest.main()
