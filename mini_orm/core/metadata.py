"""Model metadata extraction used by repository SQL generation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Generic, List, Optional, Type, TypeVar

from .models import (
    DataclassModel,
    RelationSpec,
    auto_pk_field,
    model_fields,
    model_relations,
    pk_fields,
    table_name,
)

T = TypeVar("T", bound=DataclassModel)


@dataclass(frozen=True)
class ModelMetadata(Generic[T]):
    """Normalized model description used by repository operations."""

    model: Type[T]
    table: str
    pk: str
    auto_pk: Optional[str]
    columns: List[str]
    writable_columns: List[str]
    relations: Dict[str, RelationSpec]


def build_model_metadata(model: Type[T]) -> ModelMetadata[T]:
    """Build model metadata from dataclass annotations and field metadata.

    Args:
        model: Dataclass model type.

    Returns:
        Immutable metadata object used by `Repository`.

    Raises:
        ValueError: If model has zero or multiple primary key fields.
    """

    pks = pk_fields(model)
    if len(pks) != 1:
        raise ValueError("This mini-ORM supports exactly 1 PK field.")

    pk_name = pks[0].name
    auto_pk = auto_pk_field(model)
    auto_pk_name = auto_pk.name if auto_pk else None
    all_columns = [field.name for field in model_fields(model)]
    writable_columns = [name for name in all_columns if name != pk_name]

    return ModelMetadata(
        model=model,
        table=table_name(model),
        pk=pk_name,
        auto_pk=auto_pk_name,
        columns=all_columns,
        writable_columns=writable_columns,
        relations=model_relations(model),
    )
