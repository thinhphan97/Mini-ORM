"""Model utilities for dataclass validation and mapping."""

from __future__ import annotations

import sys
from collections.abc import Mapping, Sequence as SequenceABC
from dataclasses import Field, dataclass
from dataclasses import asdict, fields, is_dataclass
from enum import Enum
from typing import Any, ClassVar, Dict, List, Optional, Protocol, Type, TypeVar, cast

from .types import RowMapping


class DataclassModel(Protocol):
    """Protocol for supported dataclass model types."""

    __dataclass_fields__: ClassVar[dict[str, Any]]


class RelationType(str, Enum):
    """Supported model relation kinds."""

    HAS_MANY = "has_many"
    BELONGS_TO = "belongs_to"


@dataclass(frozen=True)
class RelationSpec:
    """Normalized relation definition attached to a model class."""

    name: str
    model: Type[DataclassModel]
    local_key: str
    remote_key: str
    relation_type: Optional[RelationType] = None
    many: bool = False

    def __post_init__(self) -> None:
        relation_type = self.relation_type
        if relation_type is None:
            relation_type = RelationType.HAS_MANY if self.many else RelationType.BELONGS_TO
        if relation_type not in (RelationType.HAS_MANY, RelationType.BELONGS_TO):
            raise ValueError(f"Unsupported relation_type {relation_type!r}.")
        object.__setattr__(self, "relation_type", relation_type)
        object.__setattr__(self, "many", relation_type is RelationType.HAS_MANY)


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
    """Parse/derive model relations and validate relation specs."""

    require_dataclass_model(cls)
    raw_relations = getattr(cls, "__relations__", None)
    if raw_relations is None:
        parsed = {}
    else:
        if not isinstance(raw_relations, Mapping):
            raise TypeError(
                f"{cls.__name__}.__relations__ must be a mapping of relation specs."
            )

        parsed = {}
        for name, raw_spec in raw_relations.items():
            if not isinstance(name, str) or not name:
                raise TypeError("Relation name must be a non-empty string.")
            parsed[name] = _parse_relation_input(name, raw_spec)

    _merge_missing_relations(parsed, _infer_belongs_to_relations(cls))
    _merge_missing_relations(parsed, _infer_has_many_relations(cls))
    _validate_relations(cls, parsed)
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
            relation_type=raw_spec.relation_type,
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

    relation_type = _parse_relation_type(raw_spec.get("type"), relation_name=name)

    many = bool(raw_spec.get("many", False))
    if relation_type is not None:
        many = relation_type is RelationType.HAS_MANY

    return RelationSpec(
        name=name,
        model=model,
        local_key=local_key,
        remote_key=remote_key,
        relation_type=relation_type,
        many=many,
    )


def _validate_relations(
    cls: Type[DataclassModel],
    relations: Mapping[str, RelationSpec],
) -> None:
    own_columns = {field.name for field in model_fields(cls)}
    for name, spec in relations.items():
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


def _merge_missing_relations(
    target: Dict[str, RelationSpec],
    inferred: Mapping[str, RelationSpec],
) -> None:
    existing_specs = {
        (spec.model, spec.local_key, spec.remote_key, spec.many)
        for spec in target.values()
    }
    for name, spec in inferred.items():
        spec_key = (spec.model, spec.local_key, spec.remote_key, spec.many)
        if name in target or spec_key in existing_specs:
            continue
        target[name] = spec
        existing_specs.add(spec_key)


def _infer_belongs_to_relations(cls: Type[DataclassModel]) -> Dict[str, RelationSpec]:
    inferred: Dict[str, RelationSpec] = {}
    for field in model_fields(cls):
        fk_meta = field.metadata.get("fk")
        if fk_meta is None:
            continue
        fk_ref = _parse_fk_model_reference(
            fk_meta,
            context=f"{cls.__name__}.{field.name}",
        )
        if fk_ref is None:
            continue

        target_model, target_key = fk_ref
        relation_name = _belongs_to_name(field, target_model)
        if relation_name in inferred:
            raise ValueError(
                f"Duplicate inferred relation name {relation_name!r} on {cls.__name__}. "
                "Use metadata={'relation': '...'} to disambiguate."
            )
        inferred[relation_name] = RelationSpec(
            name=relation_name,
            model=target_model,
            local_key=field.name,
            remote_key=target_key,
            relation_type=RelationType.BELONGS_TO,
        )

    return inferred


def _infer_has_many_relations(cls: Type[DataclassModel]) -> Dict[str, RelationSpec]:
    module = sys.modules.get(cls.__module__)
    if module is None:
        return {}

    inferred: Dict[str, RelationSpec] = {}
    for value in vars(module).values():
        if not isinstance(value, type):
            continue
        if value is cls or not is_dataclass(value):
            continue

        model = cast(Type[DataclassModel], value)
        for field in model_fields(model):
            fk_meta = field.metadata.get("fk")
            if fk_meta is None:
                continue
            fk_ref = _parse_fk_model_reference(
                fk_meta,
                context=f"{model.__name__}.{field.name}",
            )
            if fk_ref is None:
                continue

            target_model, target_key = fk_ref
            if target_model is not cls:
                continue

            relation_name = _has_many_name(field, model)
            if relation_name in inferred:
                raise ValueError(
                    f"Duplicate inferred relation name {relation_name!r} on {cls.__name__}. "
                    "Use metadata={'related_name': '...'} on FK fields to disambiguate."
                )
            inferred[relation_name] = RelationSpec(
                name=relation_name,
                model=model,
                local_key=target_key,
                remote_key=field.name,
                relation_type=RelationType.HAS_MANY,
            )

    return inferred


def _parse_relation_type(raw: Any, *, relation_name: str) -> Optional[RelationType]:
    if raw is None:
        return None
    if isinstance(raw, RelationType):
        return raw
    if isinstance(raw, str):
        try:
            return RelationType(raw)
        except ValueError as exc:  # pragma: no cover - explicit branch below
            raise ValueError(
                f"Relation {relation_name!r} has unsupported type {raw!r}. "
                "Use 'has_many' or 'belongs_to'."
            ) from exc
    raise ValueError(
        f"Relation {relation_name!r} has unsupported type {raw!r}. "
        "Use 'has_many' or 'belongs_to'."
    )


def _parse_fk_model_reference(
    raw_fk: Any,
    *,
    context: str,
) -> Optional[tuple[Type[DataclassModel], str]]:
    if isinstance(raw_fk, Mapping):
        model = raw_fk.get("model")
        if model is None:
            return None
        if not isinstance(model, type):
            raise TypeError(f"{context} metadata fk.model must be a dataclass model type.")
        require_dataclass_model(model)
        column = raw_fk.get("column", "id")
        if not isinstance(column, str) or not column:
            raise TypeError(f"{context} metadata fk.column must be a non-empty string.")
        return cast(Type[DataclassModel], model), column

    if isinstance(raw_fk, (str, bytes)) or not isinstance(raw_fk, SequenceABC):
        return None

    values = list(raw_fk)
    if len(values) != 2:
        return None

    model_or_table, column = values
    if not isinstance(model_or_table, type):
        return None
    require_dataclass_model(model_or_table)

    if not isinstance(column, str) or not column:
        raise TypeError(f"{context} metadata fk column must be a non-empty string.")
    return cast(Type[DataclassModel], model_or_table), column


def _belongs_to_name(field: Field[Any], target_model: Type[DataclassModel]) -> str:
    relation_name = field.metadata.get("relation")
    if relation_name is not None:
        if not isinstance(relation_name, str) or not relation_name:
            raise TypeError(
                f"{target_model.__name__} relation metadata 'relation' must be a non-empty string."
            )
        return relation_name

    if field.name.endswith("_id") and len(field.name) > 3:
        return field.name[:-3]
    return target_model.__name__.lower()


def _has_many_name(field: Field[Any], child_model: Type[DataclassModel]) -> str:
    relation_name = field.metadata.get("related_name")
    if relation_name is not None:
        if not isinstance(relation_name, str) or not relation_name:
            raise TypeError(
                f"{child_model.__name__}.{field.name} metadata 'related_name' "
                "must be a non-empty string."
            )
        return relation_name

    return _pluralize(child_model.__name__.lower())


def _pluralize(name: str) -> str:
    if name.endswith("y") and len(name) > 1 and name[-2] not in "aeiou":
        return f"{name[:-1]}ies"
    if name.endswith(("s", "x", "z", "ch", "sh")):
        return f"{name}es"
    return f"{name}s"
