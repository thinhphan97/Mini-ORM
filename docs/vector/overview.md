# Vector Overview

Vector features are exposed through:

- `VectorRepository` in core.
- Vector store adapters in `mini_orm.ports.vector`.

Optional backends included in this project:

- `QdrantVectorStore`
- `ChromaVectorStore`
- `FaissVectorStore`

## In-memory adapter (default for local tests)

```python
from mini_orm import InMemoryVectorStore, VectorRecord, VectorRepository

store = InMemoryVectorStore()
repo = VectorRepository(store, "users", dimension=3, metric="cosine")

repo.upsert(
    [
        VectorRecord(id="u1", vector=[0.1, 0.2, 0.3], payload={"name": "alice"}),
        VectorRecord(id="u2", vector=[0.2, 0.1, 0.5], payload={"name": "bob"}),
    ]
)

results = repo.query([0.1, 0.2, 0.25], top_k=2)
```

Supported in-memory metrics:

- `cosine`
- `dot`
- `l2` (stored as negative distance score for ranking)

## Contract

Any backend implementing `VectorStorePort` can be plugged into `VectorRepository`.
