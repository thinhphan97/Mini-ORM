from __future__ import annotations

import importlib
import os
import tempfile
import unittest
from urllib.parse import urlparse
from unittest.mock import patch

from mini_orm import QdrantVectorStore, VectorMetric, VectorRecord, VectorRepository
from tests.vector_test_helpers import wait_for_service_or_skip


def _module_available(name: str) -> bool:
    try:
        importlib.import_module(name)
    except Exception:
        return False
    return True


HAS_QDRANT = _module_available("qdrant_client")
RUN_HOST_VECTOR_TESTS = os.getenv("MINI_ORM_VECTOR_HOST_TESTS", "").lower() in {
    "1",
    "true",
    "yes",
}


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


@unittest.skipUnless(HAS_QDRANT, "qdrant_client is not installed")
@unittest.skipUnless(
    RUN_HOST_VECTOR_TESTS,
    "Set MINI_ORM_VECTOR_HOST_TESTS=1 to run host-server vector integration tests.",
)
class QdrantHostVectorFlowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.qdrant_url = os.getenv("MINI_ORM_QDRANT_URL", "http://localhost:6333")
        parsed = urlparse(cls.qdrant_url)
        endpoint_host = parsed.hostname or "localhost"
        endpoint_port = parsed.port or (443 if parsed.scheme == "https" else 80)
        endpoint = f"{endpoint_host}:{endpoint_port}"

        cls.repo = wait_for_service_or_skip(
            service_name="Qdrant",
            endpoint=endpoint,
            initializer=lambda: VectorRepository(
                QdrantVectorStore(url=cls.qdrant_url, timeout=5.0),
                "qdrant_host_users",
                dimension=3,
                metric=VectorMetric.COSINE,
                auto_create=True,
                overwrite=True,
            ),
        )

    def test_qdrant_host_flow(self) -> None:
        u1 = "11111111-1111-1111-1111-111111111111"
        u2 = "22222222-2222-2222-2222-222222222222"
        u3 = "33333333-3333-3333-3333-333333333333"
        self.repo.upsert(
            [
                VectorRecord(u1, [1, 0, 0], {"group": "a"}),
                VectorRecord(u2, [0, 1, 0], {"group": "b"}),
                VectorRecord(u3, [0.9, 0.1, 0], {"group": "a"}),
            ]
        )

        hits = self.repo.query([1, 0, 0], top_k=2)
        filtered = self.repo.query([1, 0, 0], top_k=5, filters={"group": "a"})
        fetched = self.repo.fetch(ids=[u2, u1])

        self.assertEqual([item.id for item in fetched], [u2, u1])
        self.assertEqual([hit.id for hit in hits], [u1, u3])
        self.assertEqual([hit.id for hit in filtered], [u1, u3])


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


if __name__ == "__main__":
    unittest.main()
