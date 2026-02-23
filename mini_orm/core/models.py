"""Model utilities for dataclass validation and mapping."""

from __future__ import annotations

from dataclasses import Field
from dataclasses import asdict, fields, is_dataclass
from typing import Any, ClassVar, Dict, List, Optional, Protocol, Type, TypeVar

from .types import RowMapping


class DataclassModel(Protocol):
    """Protocol for supported dataclass model types."""

    __dataclass_fields__: ClassVar[dict[str, Any]]


T = TypeVar("T", bound=DataclassModel)


def require_dataclass_model(cls: Type[Any]) -> None:
    """Validate that a class is a dataclass model."""

    if not is_dataclass(cls):
        raise TypeError(f"{cls.__name__} must be a dataclass.")


def table_name(model_or_cls: Any) -> str:
    """Resolve table name from model class or instance.

    Uses `__table__` override when present, otherwise lowercased class name.
    """

    cls = model_or_cls if isinstance(model_or_cls, type) else type(model_or_cls)
    name = getattr(cls, "__table__", None)
    return name if isinstance(name, str) and name else cls.__name__.lower()


def model_fields(cls: Type[DataclassModel]) -> List[Field[Any]]:
    """Return dataclass fields for a model type."""

    require_dataclass_model(cls)
    return list(fields(cls))


def pk_fields(cls: Type[DataclassModel]) -> List[Field[Any]]:
    """Return primary key fields defined with `metadata={'pk': True}`."""

    pks = [f for f in model_fields(cls) if f.metadata.get("pk")]
    if not pks:
        raise ValueError(
            f"{cls.__name__} has no PK field. Use field(metadata={{'pk': True}})."
        )
    return pks


def auto_pk_field(cls: Type[DataclassModel]) -> Optional[Field[Any]]:
    """Return auto primary key field if model has exactly one auto PK."""

    pks = pk_fields(cls)
    if len(pks) == 1 and pks[0].metadata.get("auto"):
        return pks[0]
    return None


def to_dict(obj: DataclassModel) -> Dict[str, Any]:
    """Convert dataclass model instance to dictionary."""

    return asdict(obj)


def row_to_model(cls: Type[T], row: RowMapping) -> T:
    """Map one DB row mapping to a model instance."""

    return cls(**dict(row))  # type: ignore[arg-type]
