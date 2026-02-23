# Qdrant Adapter

`QdrantVectorStore` is an optional adapter for [Qdrant](https://qdrant.tech/).

## Install

```bash
pip install qdrant-client
```

## Usage

```python
from mini_orm import QdrantVectorStore, VectorRecord, VectorRepository

store = QdrantVectorStore(location=":memory:")
repo = VectorRepository(store, "items", dimension=4, metric="cosine")

repo.upsert(
    [
        VectorRecord(
            id="11111111-1111-1111-1111-111111111111",
            vector=[0.1, 0.2, 0.3, 0.4],
            payload={"type": "doc"},
        ),
    ]
)

top = repo.query([0.1, 0.2, 0.25, 0.4], top_k=5)
```

## Notes

- Qdrant requires UUID string IDs.
- `filters` are translated into exact-match payload conditions.
- For persistent local storage, use `QdrantVectorStore(location=\"./.qdrant\")`.
