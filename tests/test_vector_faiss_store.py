from __future__ import annotations

import importlib
import unittest
from unittest.mock import patch

from mini_orm import FaissVectorStore, VectorMetric, VectorRecord, VectorRepository


def _module_available(name: str) -> bool:
    try:
        importlib.import_module(name)
    except ImportError:
        return False
    return True


HAS_FAISS = _module_available("faiss") and _module_available("numpy")


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

        with self.assertRaisesRegex(
            NotImplementedError, "does not support payload filters"
        ):
            repo.query([1, 0], top_k=1, filters={"group": "a"})


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
