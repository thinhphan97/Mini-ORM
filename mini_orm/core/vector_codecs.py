"""Payload codec helpers for vector repository I/O."""

from __future__ import annotations

import base64
import json
import sys
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping, Protocol
from uuid import UUID

_TYPE_KEY = "__miniorm_codec__"


class VectorPayloadCodec(Protocol):
    """Codec interface for vector payload/filter serialization and deserialization."""

    def serialize(
        self,
        payload: Mapping[str, Any] | None,
    ) -> Mapping[str, Any] | None: ...

    def deserialize(
        self,
        payload: Mapping[str, Any] | None,
    ) -> Mapping[str, Any] | None: ...

    def serialize_filters(
        self,
        filters: Mapping[str, Any] | None,
    ) -> Mapping[str, Any] | None: ...


@dataclass(frozen=True)
class IdentityVectorPayloadCodec:
    """Default no-op payload codec."""

    def serialize(
        self,
        payload: Mapping[str, Any] | None,
    ) -> Mapping[str, Any] | None:
        if payload is None:
            return None
        return dict(payload)

    def deserialize(
        self,
        payload: Mapping[str, Any] | None,
    ) -> Mapping[str, Any] | None:
        if payload is None:
            return None
        return dict(payload)

    def serialize_filters(
        self,
        filters: Mapping[str, Any] | None,
    ) -> Mapping[str, Any] | None:
        if filters is None:
            return None
        return dict(filters)


@dataclass(frozen=True)
class JsonVectorPayloadCodec:
    """Codec that stores non-scalar payload values as tagged JSON strings.

    Scalar values (`None`, `bool`, `int`, `float`, and normal `str`) are kept as-is.
    Non-scalar values (for example `Enum`, `dict`, `list`, `datetime`, `Decimal`)
    are converted into prefixed JSON text and reconstructed on read.
    """

    prefix: str = "__miniorm_json__:"

    def serialize(
        self,
        payload: Mapping[str, Any] | None,
    ) -> Mapping[str, Any] | None:
        if payload is None:
            return None
        return {str(key): self._encode_value(value) for key, value in payload.items()}

    def deserialize(
        self,
        payload: Mapping[str, Any] | None,
    ) -> Mapping[str, Any] | None:
        if payload is None:
            return None
        return {str(key): self._decode_value(value) for key, value in payload.items()}

    def serialize_filters(
        self,
        filters: Mapping[str, Any] | None,
    ) -> Mapping[str, Any] | None:
        if filters is None:
            return None
        return {str(key): self._encode_value(value) for key, value in filters.items()}

    def _encode_value(self, value: Any) -> Any:
        if value is None or isinstance(value, (bool, int, float)):
            return value

        if isinstance(value, Enum):
            converted = self._to_jsonable(value)
            return self.prefix + json.dumps(converted, separators=(",", ":"), sort_keys=True)

        if isinstance(value, str):
            if value.startswith(self.prefix):
                escaped = {_TYPE_KEY: "str", "value": value}
                return self.prefix + json.dumps(escaped, separators=(",", ":"), sort_keys=True)
            return value

        converted = self._to_jsonable(value)
        return self.prefix + json.dumps(converted, separators=(",", ":"), sort_keys=True)

    def _decode_value(self, value: Any) -> Any:
        if not isinstance(value, str) or not value.startswith(self.prefix):
            return value

        raw = value[len(self.prefix) :]
        parsed = json.loads(raw)
        return self._from_jsonable(parsed)

    def _to_jsonable(self, value: Any) -> Any:
        if isinstance(value, Enum):
            return {
                _TYPE_KEY: "enum",
                "class": _enum_ref(type(value)),
                "value": self._to_jsonable(value.value),
            }
        if value is None or isinstance(value, (bool, int, float, str)):
            return value
        if isinstance(value, datetime):
            return {_TYPE_KEY: "datetime", "value": value.isoformat()}
        if isinstance(value, date):
            return {_TYPE_KEY: "date", "value": value.isoformat()}
        if isinstance(value, time):
            return {_TYPE_KEY: "time", "value": value.isoformat()}
        if isinstance(value, Decimal):
            return {_TYPE_KEY: "decimal", "value": str(value)}
        if isinstance(value, UUID):
            return {_TYPE_KEY: "uuid", "value": str(value)}
        if isinstance(value, (bytes, bytearray, memoryview)):
            encoded = base64.b64encode(bytes(value)).decode("ascii")
            return {_TYPE_KEY: "bytes", "value": encoded}
        if isinstance(value, tuple):
            return {
                _TYPE_KEY: "tuple",
                "items": [self._to_jsonable(item) for item in value],
            }
        if isinstance(value, (set, frozenset)):
            return {
                _TYPE_KEY: "set",
                "items": [self._to_jsonable(item) for item in value],
            }
        if isinstance(value, Mapping):
            return {str(key): self._to_jsonable(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._to_jsonable(item) for item in value]

        raise TypeError(
            "JsonVectorPayloadCodec cannot serialize payload value of type "
            f"{type(value).__name__}."
        )

    def _from_jsonable(self, value: Any) -> Any:
        if isinstance(value, list):
            return [self._from_jsonable(item) for item in value]

        if not isinstance(value, dict):
            return value

        codec_type = value.get(_TYPE_KEY)
        if codec_type == "str" and set(value) == {_TYPE_KEY, "value"}:
            return value["value"]
        if codec_type == "datetime" and set(value) == {_TYPE_KEY, "value"}:
            return datetime.fromisoformat(str(value["value"]))
        if codec_type == "date" and set(value) == {_TYPE_KEY, "value"}:
            return date.fromisoformat(str(value["value"]))
        if codec_type == "time" and set(value) == {_TYPE_KEY, "value"}:
            return time.fromisoformat(str(value["value"]))
        if codec_type == "decimal" and set(value) == {_TYPE_KEY, "value"}:
            return Decimal(str(value["value"]))
        if codec_type == "uuid" and set(value) == {_TYPE_KEY, "value"}:
            return UUID(str(value["value"]))
        if codec_type == "bytes" and set(value) == {_TYPE_KEY, "value"}:
            return base64.b64decode(str(value["value"]).encode("ascii"))
        if codec_type == "tuple" and set(value) == {_TYPE_KEY, "items"}:
            items = value.get("items", [])
            return tuple(self._from_jsonable(item) for item in items)
        if codec_type == "set" and set(value) == {_TYPE_KEY, "items"}:
            items = value.get("items", [])
            return {self._from_jsonable(item) for item in items}
        if codec_type == "enum" and set(value) == {_TYPE_KEY, "class", "value"}:
            restored_value = self._from_jsonable(value.get("value"))
            enum_cls = _resolve_enum_type(str(value.get("class", "")))
            if enum_cls is None:
                return restored_value
            try:
                return enum_cls(restored_value)
            except Exception:
                if isinstance(restored_value, str):
                    try:
                        return enum_cls[restored_value]
                    except Exception:
                        pass
                return restored_value

        return {str(key): self._from_jsonable(item) for key, item in value.items()}


def _enum_ref(enum_cls: type[Enum]) -> str:
    return f"{enum_cls.__module__}:{enum_cls.__qualname__}"


def _resolve_enum_type(ref: str) -> type[Enum] | None:
    if ":" not in ref:
        return None

    module_name, qualname = ref.split(":", 1)
    # Safety: do not import modules from payload-controlled content.
    module = sys.modules.get(module_name)
    if module is None:
        return None

    current: Any = module
    for part in qualname.split("."):
        current = getattr(current, part, None)
        if current is None:
            return None

    if isinstance(current, type) and issubclass(current, Enum):
        return current
    return None
