from __future__ import annotations

import importlib
import tempfile
import unittest
from unittest.mock import patch

from mini_orm import (
    ChromaVectorStore,
    FaissVectorStore,
    InMemoryVectorStore,
    QdrantVectorStore,
    VectorMetric,
    VectorRecord,
    VectorRepository,
)


def _module_available(name: str) -> bool:
    try:
        importlib.import_module(name)
    except Exception:
        return False
    return True


HAS_QDRANT = _module_available("qdrant_client")
HAS_CHROMA = _module_available("chromadb")
HAS_FAISS = _module_available("faiss") and _module_available("numpy")


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


@unittest.skipUnless(HAS_QDRANT, "qdrant_client is not installed")
class QdrantLocalVectorFlowTests(unittest.TestCase):
    def test_qdrant_memory_flow_upsert_query_fetch_delete(self) -> None:
        store = QdrantVectorStore(location=":memory:")
        repo = VectorRepository(
            store,
            "qdrant_mem_users",
            dimension=3,
            metric=VectorMetric.COSINE,
            auto_create=True,
            overwrite=True,
        )

        u1 = "11111111-1111-1111-1111-111111111111"
        u2 = "22222222-2222-2222-2222-222222222222"
        u3 = "33333333-3333-3333-3333-333333333333"
        repo.upsert(
            [
                VectorRecord(u1, [1, 0, 0], {"group": "a"}),
                VectorRecord(u2, [0, 1, 0], {"group": "b"}),
                VectorRecord(u3, [0.9, 0.1, 0], {"group": "a"}),
            ]
        )

        fetched = repo.fetch(ids=[u2, u1])
        hits = repo.query([1, 0, 0], top_k=2)
        filtered = repo.query([1, 0, 0], top_k=5, filters={"group": "a"})
        deleted = repo.delete([u2])
        remaining = repo.fetch()

        self.assertEqual([item.id for item in fetched], [u2, u1])
        self.assertEqual([hit.id for hit in hits], [u1, u3])
        self.assertEqual([hit.id for hit in filtered], [u1, u3])
        self.assertEqual(deleted, 1)
        self.assertEqual({item.id for item in remaining}, {u1, u3})

    def test_qdrant_path_flow_upsert_query_fetch_delete(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mini_orm_qdrant_path_") as db_path:
            store = QdrantVectorStore(location=db_path)
            repo = VectorRepository(
                store,
                "qdrant_path_users",
                dimension=3,
                metric=VectorMetric.COSINE,
                auto_create=True,
                overwrite=True,
            )

            u1 = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
            u2 = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
            repo.upsert(
                [
                    VectorRecord(u1, [1, 0, 0], {"kind": "x"}),
                    VectorRecord(u2, [0, 1, 0], {"kind": "y"}),
                ]
            )

            fetched = repo.fetch(ids=[u1])
            hits = repo.query([1, 0, 0], top_k=1)
            deleted = repo.delete([u2])
            remaining = repo.fetch()

            self.assertEqual([item.id for item in fetched], [u1])
            self.assertEqual([hit.id for hit in hits], [u1])
            self.assertEqual(deleted, 1)
            self.assertEqual([item.id for item in remaining], [u1])

    def test_qdrant_supported_metrics_dot_and_l2(self) -> None:
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
            repo = VectorRepository(
                store,
                f"qdrant_metric_{metric.value}",
                dimension=2,
                metric=metric,
                auto_create=True,
                overwrite=True,
            )
            repo.upsert(records)
            hits = repo.query(query_vector, top_k=3)
            self.assertEqual(hits[0].id, expected_first)
            self.assertEqual(len(hits), 3)

    def test_qdrant_delete_counts_only_existing_records(self) -> None:
        store = QdrantVectorStore(location=":memory:")
        repo = VectorRepository(
            store,
            "qdrant_delete_count",
            dimension=2,
            metric=VectorMetric.COSINE,
            auto_create=True,
            overwrite=True,
        )
        existing = "11111111-1111-1111-1111-111111111111"
        missing = "22222222-2222-2222-2222-222222222222"
        repo.upsert([VectorRecord(existing, [1, 0], {"kind": "x"})])

        deleted = repo.delete([existing, missing])
        self.assertEqual(deleted, 1)

    def test_qdrant_requires_uuid_ids(self) -> None:
        store = QdrantVectorStore(location=":memory:")
        repo = VectorRepository(
            store,
            "qdrant_uuid_policy",
            dimension=2,
            metric=VectorMetric.COSINE,
            auto_create=True,
            overwrite=True,
        )

        with self.assertRaisesRegex(ValueError, "requires UUID"):
            repo.upsert([VectorRecord("not-a-uuid", [1, 0], {"kind": "x"})])
        with self.assertRaisesRegex(ValueError, "requires UUID"):
            repo.fetch(ids=["not-a-uuid"])
        with self.assertRaisesRegex(ValueError, "requires UUID"):
            repo.delete(["not-a-uuid"])


@unittest.skipUnless(HAS_CHROMA, "chromadb is not installed")
class ChromaLocalVectorFlowTests(unittest.TestCase):
    def test_chroma_memory_flow_upsert_query_fetch_delete(self) -> None:
        store = ChromaVectorStore(path=":memory:")
        repo = VectorRepository(
            store,
            "chroma_users",
            dimension=3,
            metric=VectorMetric.COSINE,
            auto_create=True,
            overwrite=True,
        )

        repo.upsert(
            [
                VectorRecord("u1", [1, 0, 0], {"group": "a"}),
                VectorRecord("u2", [0, 1, 0], {"group": "b"}),
                VectorRecord("u3", [0.9, 0.1, 0], {"group": "a"}),
            ]
        )

        fetched = repo.fetch(ids=["u2", "u1"])
        hits = repo.query([1, 0, 0], top_k=2)
        filtered = repo.query([1, 0, 0], top_k=5, filters={"group": "a"})
        deleted = repo.delete(["u2"])
        remaining = repo.fetch()

        self.assertEqual([item.id for item in fetched], ["u2", "u1"])
        self.assertEqual(hits[0].id, "u1")
        self.assertEqual([hit.id for hit in filtered], ["u1", "u3"])
        self.assertEqual(deleted, 1)
        self.assertEqual({item.id for item in remaining}, {"u1", "u3"})

    def test_chroma_supported_metrics_dot_and_l2(self) -> None:
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
            repo = VectorRepository(
                store,
                f"chroma_metric_{metric.value}",
                dimension=2,
                metric=metric,
                auto_create=True,
                overwrite=True,
            )
            repo.upsert(records)
            hits = repo.query(query_vector, top_k=3)
            self.assertEqual(hits[0].id, expected_first)
            self.assertEqual(len(hits), 3)

    def test_chroma_upsert_allows_none_payload(self) -> None:
        store = ChromaVectorStore(path=":memory:")
        repo = VectorRepository(
            store,
            "chroma_none_payload",
            dimension=2,
            metric=VectorMetric.COSINE,
            auto_create=True,
            overwrite=True,
        )
        repo.upsert([VectorRecord("u1", [1, 0], None)])

        fetched = repo.fetch(ids=["u1"])
        self.assertEqual(len(fetched), 1)
        self.assertIsNone(fetched[0].payload)

    def test_chroma_delete_counts_only_existing_records(self) -> None:
        store = ChromaVectorStore(path=":memory:")
        repo = VectorRepository(
            store,
            "chroma_delete_count",
            dimension=2,
            metric=VectorMetric.COSINE,
            auto_create=True,
            overwrite=True,
        )
        repo.upsert([VectorRecord("u1", [1, 0], {"kind": "x"})])

        deleted = repo.delete(["u1", "missing"])
        self.assertEqual(deleted, 1)


@unittest.skipUnless(HAS_FAISS, "faiss/numpy is not installed")
class FaissLocalVectorFlowTests(unittest.TestCase):
    def test_faiss_memory_flow_upsert_query_fetch_delete(self) -> None:
        store = FaissVectorStore()
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
                VectorRecord("u1", [1, 0, 0], {"group": "a"}),
                VectorRecord("u2", [0, 1, 0], {"group": "b"}),
                VectorRecord("u3", [0.9, 0.1, 0], {"group": "a"}),
            ]
        )

        fetched = repo.fetch(ids=["u2", "u1"])
        hits = repo.query([1, 0, 0], top_k=2)
        deleted = repo.delete(["u2"])
        remaining = repo.fetch()

        self.assertEqual([item.id for item in fetched], ["u2", "u1"])
        self.assertEqual([hit.id for hit in hits], ["u1", "u3"])
        self.assertEqual(deleted, 1)
        self.assertEqual([item.id for item in remaining], ["u1", "u3"])

    def test_faiss_supported_metrics_dot_and_l2(self) -> None:
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
            repo = VectorRepository(
                store,
                f"faiss_metric_{metric.value}",
                dimension=2,
                metric=metric,
                auto_create=True,
                overwrite=True,
            )
            repo.upsert(records)
            hits = repo.query(query_vector, top_k=3)
            self.assertEqual([hit.id for hit in hits], expected_order)

    def test_faiss_query_filters_raise_not_supported(self) -> None:
        store = FaissVectorStore()
        repo = VectorRepository(
            store,
            "faiss_filter_unsupported",
            dimension=2,
            metric=VectorMetric.COSINE,
            auto_create=True,
            overwrite=True,
        )
        repo.upsert([VectorRecord("u1", [1, 0], {"group": "a"})])

        with self.assertRaisesRegex(NotImplementedError, "does not support payload filters"):
            repo.query([1, 0], top_k=1, filters={"group": "a"})


class QdrantAdapterOptionalTests(unittest.TestCase):
    def test_qdrant_store_requires_dependency(self) -> None:
        real_import = __import__

        def fake_import(name, *args, **kwargs):
            if name == "qdrant_client":
                raise ImportError("simulated missing qdrant-client")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=fake_import):
            with self.assertRaises(ImportError):
                QdrantVectorStore(location=tempfile.mkdtemp(prefix="mini_orm_qdrant_"))


class ChromaAdapterOptionalTests(unittest.TestCase):
    def test_chroma_store_requires_dependency(self) -> None:
        real_import = __import__

        def fake_import(name, *args, **kwargs):
            if name == "chromadb":
                raise ImportError("simulated missing chromadb")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=fake_import):
            with self.assertRaises(ImportError):
                ChromaVectorStore(path=tempfile.mkdtemp(prefix="mini_orm_chroma_"))


class FaissAdapterOptionalTests(unittest.TestCase):
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
