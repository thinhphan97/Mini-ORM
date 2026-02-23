"""Model utilities for dataclass validation and mapping."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import Field, dataclass
from dataclasses import asdict, fields, is_dataclass
from typing import Any, ClassVar, Dict, List, Optional, Protocol, Type, TypeVar

from .types import RowMapping


class DataclassModel(Protocol):
    """Protocol for supported dataclass model types."""

    __dataclass_fields__: ClassVar[dict[str, Any]]


@dataclass(frozen=True)
class RelationSpec:
    """Normalized relation definition attached to a model class."""

    name: str
    model: Type[DataclassModel]
    local_key: str
    remote_key: str
    many: bool = False


RelationInput = RelationSpec | Mapping[str, Any]


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


def model_relations(cls: Type[DataclassModel]) -> Dict[str, RelationSpec]:
    """Parse and validate model `__relations__` declarations."""

    require_dataclass_model(cls)
    raw_relations = getattr(cls, "__relations__", None)
    if raw_relations is None:
        return {}
    if not isinstance(raw_relations, Mapping):
        raise TypeError(
            f"{cls.__name__}.__relations__ must be a mapping of relation specs."
        )

    own_columns = {field.name for field in model_fields(cls)}
    parsed: Dict[str, RelationSpec] = {}
    for name, raw_spec in raw_relations.items():
        if not isinstance(name, str) or not name:
            raise TypeError("Relation name must be a non-empty string.")

        spec = _parse_relation_input(name, raw_spec)
        if spec.local_key not in own_columns:
            raise ValueError(
                f"Relation '{name}' on {cls.__name__} uses unknown local_key "
                f"{spec.local_key!r}."
            )

        target_columns = {field.name for field in model_fields(spec.model)}
        if spec.remote_key not in target_columns:
            raise ValueError(
                f"Relation '{name}' points to unknown remote_key "
                f"{spec.remote_key!r} on {spec.model.__name__}."
            )

        parsed[name] = spec

    return parsed


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


def _parse_relation_input(name: str, raw_spec: RelationInput) -> RelationSpec:
    if isinstance(raw_spec, RelationSpec):
        return RelationSpec(
            name=name,
            model=raw_spec.model,
            local_key=raw_spec.local_key,
            remote_key=raw_spec.remote_key,
            many=raw_spec.many,
        )

    if not isinstance(raw_spec, Mapping):
        raise TypeError(
            f"Relation {name!r} must be RelationSpec or mapping, got "
            f"{type(raw_spec).__name__}."
        )

    model = raw_spec.get("model")
    if not isinstance(model, type):
        raise TypeError(f"Relation {name!r} requires a dataclass model in 'model'.")
    require_dataclass_model(model)

    local_key = raw_spec.get("local_key") or raw_spec.get("foreign_key")
    if not isinstance(local_key, str) or not local_key:
        raise TypeError(
            f"Relation {name!r} requires non-empty 'local_key' (or 'foreign_key')."
        )

    remote_key = raw_spec.get("remote_key", "id")
    if not isinstance(remote_key, str) or not remote_key:
        raise TypeError(f"Relation {name!r} requires non-empty 'remote_key'.")

    relation_type = raw_spec.get("type")
    if relation_type is not None and relation_type not in {"has_many", "belongs_to"}:
        raise ValueError(
            f"Relation {name!r} has unsupported type {relation_type!r}. "
            "Use 'has_many' or 'belongs_to'."
        )

    many = bool(raw_spec.get("many", False))
    if relation_type == "has_many":
        many = True
    if relation_type == "belongs_to":
        many = False

    return RelationSpec(
        name=name,
        model=model,
        local_key=local_key,
        remote_key=remote_key,
        many=many,
    )
