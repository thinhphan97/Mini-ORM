"""Model field codec helpers for DB serialization/deserialization."""

from __future__ import annotations

import json
import types
from dataclasses import Field, fields, is_dataclass
from enum import Enum
from functools import lru_cache
from typing import Any, Type, Union, get_args, get_origin, get_type_hints


def serialize_model_value(
    cls: Type[Any],
    field_name: str,
    value: Any,
) -> Any:
    """Serialize one model field value for DB writes."""

    field = _model_field_map(cls).get(field_name)
    if field is None:
        return value
    annotation = _model_type_hints(cls).get(field_name, field.type)
    return _serialize_value(
        value,
        annotation=annotation,
        codec=_field_codec(field),
        field_name=field_name,
    )


def deserialize_model_value(
    cls: Type[Any],
    field_name: str,
    value: Any,
) -> Any:
    """Deserialize one DB value into model field type."""

    field = _model_field_map(cls).get(field_name)
    if field is None:
        return value
    annotation = _model_type_hints(cls).get(field_name, field.type)
    return _deserialize_value(
        value,
        annotation=annotation,
        codec=_field_codec(field),
        field_name=field_name,
    )


@lru_cache(maxsize=None)
def _model_field_map(cls: Type[Any]) -> dict[str, Field[Any]]:
    _require_dataclass_model(cls)
    return {field.name: field for field in fields(cls)}


@lru_cache(maxsize=None)
def _model_type_hints(cls: Type[Any]) -> dict[str, Any]:
    _require_dataclass_model(cls)
    try:
        return dict(get_type_hints(cls, include_extras=True))
    except Exception:
        return {}


def _serialize_value(
    value: Any,
    *,
    annotation: Any,
    codec: str | None,
    field_name: str,
) -> Any:
    if value is None:
        return None

    enum_type = _enum_type(annotation)
    if enum_type is not None or codec == "enum":
        return _serialize_enum(value, enum_type=enum_type, field_name=field_name)

    if _is_json_field(annotation, codec):
        return _serialize_json(value)

    return value


def _deserialize_value(
    value: Any,
    *,
    annotation: Any,
    codec: str | None,
    field_name: str,
) -> Any:
    if value is None:
        return None

    enum_type = _enum_type(annotation)
    if enum_type is not None or codec == "enum":
        return _deserialize_enum(value, enum_type=enum_type, field_name=field_name)

    if _is_json_field(annotation, codec):
        return _deserialize_json(value, field_name=field_name)

    return value


def _serialize_enum(
    value: Any,
    *,
    enum_type: type[Enum] | None,
    field_name: str,
) -> Any:
    if isinstance(value, Enum):
        return value.value
    if enum_type is None:
        raise ValueError(
            f"Field {field_name!r} uses enum codec but has no Enum annotation."
        )
    try:
        return enum_type(value).value
    except Exception as exc:
        if isinstance(value, str):
            try:
                return enum_type[value].value
            except Exception:
                pass
        raise ValueError(
            f"Invalid enum value {value!r} for field {field_name!r}."
        ) from exc


def _deserialize_enum(
    value: Any,
    *,
    enum_type: type[Enum] | None,
    field_name: str,
) -> Any:
    if enum_type is None:
        raise ValueError(
            f"Field {field_name!r} uses enum codec but has no Enum annotation."
        )
    if isinstance(value, enum_type):
        return value
    try:
        return enum_type(value)
    except Exception as exc:
        if isinstance(value, str):
            try:
                return enum_type[value]
            except Exception:
                pass
        raise ValueError(
            f"Cannot deserialize value {value!r} to enum {enum_type.__name__} "
            f"for field {field_name!r}."
        ) from exc


def _serialize_json(value: Any) -> Any:
    if isinstance(value, (str, bytes, bytearray, memoryview)):
        return value
    return json.dumps(value)


def _deserialize_json(value: Any, *, field_name: str) -> Any:
    if isinstance(value, (dict, list)):
        return value
    text: str | None
    if isinstance(value, str):
        text = value
    elif isinstance(value, (bytes, bytearray, memoryview)):
        text = bytes(value).decode("utf-8")
    else:
        return value

    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Cannot deserialize JSON for field {field_name!r}: {text!r}."
        ) from exc


def _field_codec(field: Field[Any]) -> str | None:
    codec = field.metadata.get("codec")
    if codec is None:
        return None
    if not isinstance(codec, str):
        raise TypeError(
            f"Field {field.name!r} metadata codec must be a string, got {type(codec).__name__}."
        )
    normalized = codec.strip().lower()
    if normalized in {"json", "enum"}:
        return normalized
    raise ValueError(
        f"Unsupported codec {codec!r} on field {field.name!r}. "
        "Supported codecs: 'json', 'enum'."
    )


def _enum_type(annotation: Any) -> type[Enum] | None:
    base = _unwrap_optional(annotation)
    if isinstance(base, type) and issubclass(base, Enum):
        return base
    return None


def _is_json_field(annotation: Any, codec: str | None) -> bool:
    if codec == "json":
        return True
    if codec == "enum":
        return False

    base = _unwrap_optional(annotation)
    if base in {dict, list}:
        return True

    origin = get_origin(base)
    return origin in {dict, list}


def _unwrap_optional(annotation: Any) -> Any:
    origin = get_origin(annotation)
    if origin is None:
        return annotation

    if origin not in {Union, types.UnionType}:
        return annotation

    all_args = get_args(annotation)
    args = [arg for arg in all_args if arg is not type(None)]
    if len(args) == 1 and len(all_args) == 2:
        return args[0]
    return annotation


def _require_dataclass_model(cls: Type[Any]) -> None:
    if not is_dataclass(cls):
        raise TypeError(f"{cls.__name__} must be a dataclass.")
