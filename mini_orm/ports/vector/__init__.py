"""Vector store adapter exports."""

from .in_memory import InMemoryVectorStore

try:  # pragma: no cover - depends on optional dependency
    from .chroma import ChromaVectorStore
except Exception:  # pragma: no cover - import side effect control
    ChromaVectorStore = None  # type: ignore[assignment]

try:  # pragma: no cover - depends on optional dependency
    from .faiss import FaissVectorStore
except Exception:  # pragma: no cover - import side effect control
    FaissVectorStore = None  # type: ignore[assignment]

try:  # pragma: no cover - depends on optional dependency
    from .qdrant import QdrantVectorStore
except Exception:  # pragma: no cover - import side effect control
    QdrantVectorStore = None  # type: ignore[assignment]

__all__ = [
    "InMemoryVectorStore",
    "QdrantVectorStore",
    "ChromaVectorStore",
    "FaissVectorStore",
]
