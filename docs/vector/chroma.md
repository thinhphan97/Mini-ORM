# Chroma Adapter

`ChromaVectorStore` is an optional adapter for [Chroma](https://www.trychroma.com/).

## Install

```bash
pip install chromadb
```

## Usage

```python
from mini_orm import ChromaVectorStore, VectorRecord, VectorRepository

store = ChromaVectorStore(path="./.chroma")
repo = VectorRepository(store, "items", dimension=4, metric="cosine")

repo.upsert(
    [
        VectorRecord(id="1", vector=[0.1, 0.2, 0.3, 0.4], payload={"type": "doc"}),
    ]
)

top = repo.query([0.1, 0.2, 0.25, 0.4], top_k=5)
```

## Notes

- Supported metrics: `cosine`, `dot`, `l2`.
- `filters` are translated to Chroma `where` exact-match conditions.
