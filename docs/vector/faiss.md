# Faiss Adapter

`FaissVectorStore` is an optional adapter for [Faiss](https://github.com/facebookresearch/faiss).

## Install

```bash
pip install faiss-cpu numpy
```

## Usage

```python
from mini_orm import FaissVectorStore, VectorRecord, VectorRepository

store = FaissVectorStore()
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
- `filters` are ignored because Faiss query API has no standard payload filter.
