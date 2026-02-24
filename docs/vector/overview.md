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

## Payload codec (optional)

Use a payload codec when metadata contains rich types (for example Enum, nested JSON):

```python
from enum import Enum
from mini_orm import (
    InMemoryVectorStore,
    JsonVectorPayloadCodec,
    VectorRecord,
    VectorRepository,
)

class Status(str, Enum):
    ACTIVE = "active"

store = InMemoryVectorStore()
repo = VectorRepository(
    store,
    "users",
    dimension=2,
    payload_codec=JsonVectorPayloadCodec(),
)
repo.upsert([VectorRecord(id="u1", vector=[1, 0], payload={"status": Status.ACTIVE})])
hits = repo.query([1, 0], filters={"status": Status.ACTIVE})
```

Notes:
- Enum decode is best-effort: if enum class cannot be resolved at runtime,
  decoded value falls back to scalar (for example `"active"`).

Supported in-memory metrics:

- `cosine`
- `dot`
- `l2` (stored as negative distance score for ranking)

## Contract

Any backend implementing `VectorStorePort` can be plugged into `VectorRepository`.
