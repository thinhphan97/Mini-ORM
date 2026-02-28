from __future__ import annotations

import importlib
import os
import tempfile
import unittest
from unittest.mock import patch

from mini_orm import ChromaVectorStore, VectorMetric, VectorRecord, VectorRepository
from tests.vector_test_helpers import wait_for_service_or_skip


def _module_available(name: str) -> bool:
    try:
        importlib.import_module(name)
    except Exception:
        return False
    return True


HAS_CHROMA = _module_available("chromadb")
RUN_HOST_VECTOR_TESTS = os.getenv("MINI_ORM_VECTOR_HOST_TESTS", "").lower() in {
    "1",
    "true",
    "yes",
}


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


@unittest.skipUnless(HAS_CHROMA, "chromadb is not installed")
@unittest.skipUnless(
    RUN_HOST_VECTOR_TESTS,
    "Set MINI_ORM_VECTOR_HOST_TESTS=1 to run host-server vector integration tests.",
)
class ChromaHostVectorFlowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.chroma_host = os.getenv("MINI_ORM_CHROMA_HOST", "localhost")
        cls.chroma_port = int(os.getenv("MINI_ORM_CHROMA_PORT", "8000"))
        cls.repo = wait_for_service_or_skip(
            service_name="Chroma",
            endpoint=f"{cls.chroma_host}:{cls.chroma_port}",
            initializer=lambda: VectorRepository(
                ChromaVectorStore(host=cls.chroma_host, port=cls.chroma_port),
                "chroma_host_users",
                dimension=3,
                metric=VectorMetric.COSINE,
                auto_create=True,
                overwrite=True,
            ),
        )

    def test_chroma_host_flow(self) -> None:
        self.repo.upsert(
            [
                VectorRecord("u1", [1, 0, 0], {"group": "a"}),
                VectorRecord("u2", [0, 1, 0], {"group": "b"}),
                VectorRecord("u3", [0.9, 0.1, 0], {"group": "a"}),
            ]
        )

        hits = self.repo.query([1, 0, 0], top_k=2)
        filtered = self.repo.query([1, 0, 0], top_k=5, filters={"group": "a"})
        fetched = self.repo.fetch(ids=["u2", "u1"])

        self.assertEqual([item.id for item in fetched], ["u2", "u1"])
        self.assertEqual(hits[0].id, "u1")
        self.assertEqual([hit.id for hit in filtered], ["u1", "u3"])


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


if __name__ == "__main__":
    unittest.main()
