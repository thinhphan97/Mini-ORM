"""Dataclass validation mixin for pydantic-like basic input checks."""

from __future__ import annotations

import re
import types
from abc import ABC
from collections.abc import Mapping, Sequence
from dataclasses import fields, is_dataclass
from enum import Enum
from typing import Any, Literal, Union, get_args, get_origin, get_type_hints


class ValidationError(ValueError):
    """Raised when dataclass field validation fails."""


class ValidatedModel(ABC):
    """Base class for dataclasses that need runtime input validation.

    Usage:
    - Inherit this class and decorate child model with `@dataclass`.
    - Add field constraints in dataclass field metadata.
    - Optionally override `model_validate()` for model-level checks.
    """

    def __post_init__(self) -> None:
        if not is_dataclass(self):
            raise TypeError("ValidatedModel must be used with @dataclass models.")
        self._validate_fields()
        self.model_validate()

    def model_validate(self) -> None:
        """Hook for model-level custom validation after field checks."""

    def _validate_fields(self) -> None:
        hints = get_type_hints(type(self), include_extras=True)
        for field in fields(self):
            name = field.name
            value = getattr(self, name)
            annotation = hints.get(name, Any)
            metadata = dict(field.metadata)
            _validate_type(name, value, annotation)
            _validate_constraints(name, value, metadata)


def _validate_type(name: str, value: Any, annotation: Any) -> None:
    if annotation is Any:
        return
    origin = get_origin(annotation)
    args = get_args(annotation)

    if origin is Literal:
        allowed = tuple(args)
        if value not in allowed:
            raise ValidationError(f"Field '{name}' must be one of {allowed!r}.")
        return

    if origin in (Union, types.UnionType):
        for option in args:
            try:
                _validate_type(name, value, option)
                return
            except ValidationError:
                continue
        raise ValidationError(
            f"Field '{name}' expects {_annotation_name(annotation)}, got {type(value).__name__}."
        )

    if origin in (list, set, frozenset, Sequence):
        if origin is list and not isinstance(value, list):
            raise ValidationError(f"Field '{name}' must be a list.")
        if origin is set and not isinstance(value, set):
            raise ValidationError(f"Field '{name}' must be a set.")
        if origin is frozenset and not isinstance(value, frozenset):
            raise ValidationError(f"Field '{name}' must be a frozenset.")
        if origin is Sequence and (
            isinstance(value, (str, bytes)) or not isinstance(value, Sequence)
        ):
            raise ValidationError(f"Field '{name}' must be a sequence.")

        if not args:
            return
        item_type = args[0]
        for index, item in enumerate(value):
            try:
                _validate_type(f"{name}[{index}]", item, item_type)
            except ValidationError as exc:
                raise ValidationError(str(exc)) from exc
        return

    if origin is tuple:
        if not isinstance(value, tuple):
            raise ValidationError(f"Field '{name}' must be a tuple.")
        if not args:
            return
        if len(args) == 2 and args[1] is Ellipsis:
            for index, item in enumerate(value):
                _validate_type(f"{name}[{index}]", item, args[0])
            return
        if len(value) != len(args):
            raise ValidationError(
                f"Field '{name}' expects tuple length {len(args)}, got {len(value)}."
            )
        for index, (item, item_type) in enumerate(zip(value, args, strict=True)):
            _validate_type(f"{name}[{index}]", item, item_type)
        return

    if origin in (dict, Mapping):
        if not isinstance(value, Mapping):
            raise ValidationError(f"Field '{name}' must be a mapping.")
        if len(args) != 2:
            return
        key_type, value_type = args
        for key, item in value.items():
            _validate_type(f"{name}.<key>", key, key_type)
            _validate_type(f"{name}[{key!r}]", item, value_type)
        return

    if isinstance(annotation, type):
        if annotation is type(None):
            if value is None:
                return
            raise ValidationError(
                f"Field '{name}' expects None, got {type(value).__name__}."
            )
        if value is None:
            raise ValidationError(
                f"Field '{name}' cannot be None (expected {annotation.__name__})."
            )
        if annotation is float and isinstance(value, bool):
            raise ValidationError(f"Field '{name}' expects float, got bool.")
        if annotation is float and isinstance(value, int):
            return
        if annotation is int and isinstance(value, bool):
            raise ValidationError(f"Field '{name}' expects int, got bool.")
        if issubclass(annotation, Enum):
            if not isinstance(value, annotation):
                raise ValidationError(
                    f"Field '{name}' expects {annotation.__name__}, got {type(value).__name__}."
                )
            return
        if not isinstance(value, annotation):
            raise ValidationError(
                f"Field '{name}' expects {annotation.__name__}, got {type(value).__name__}."
            )
        return


def _validate_constraints(name: str, value: Any, metadata: dict[str, Any]) -> None:
    required = bool(metadata.get("required", False))
    if value is None:
        if required:
            raise ValidationError(f"Field '{name}' is required and cannot be None.")
        return

    if metadata.get("non_empty") and isinstance(value, str) and not value.strip():
        raise ValidationError(f"Field '{name}' must be non-empty.")

    if "choices" in metadata and value not in set(metadata["choices"]):
        raise ValidationError(f"Field '{name}' must be one of {metadata['choices']!r}.")

    if isinstance(value, str) and "pattern" in metadata:
        if re.fullmatch(str(metadata["pattern"]), value) is None:
            raise ValidationError(
                f"Field '{name}' must match regex pattern {metadata['pattern']!r}."
            )

    if hasattr(value, "__len__"):
        if "min_len" in metadata and len(value) < int(metadata["min_len"]):
            raise ValidationError(
                f"Field '{name}' length must be >= {int(metadata['min_len'])}."
            )
        if "max_len" in metadata and len(value) > int(metadata["max_len"]):
            raise ValidationError(
                f"Field '{name}' length must be <= {int(metadata['max_len'])}."
            )

    for key, op in (("gt", ">"), ("ge", ">="), ("lt", "<"), ("le", "<=")):
        if key not in metadata:
            continue
        bound = metadata[key]
        ok = (
            (key == "gt" and value > bound)
            or (key == "ge" and value >= bound)
            or (key == "lt" and value < bound)
            or (key == "le" and value <= bound)
        )
        if not ok:
            raise ValidationError(f"Field '{name}' must satisfy {op} {bound!r}.")

    if "min" in metadata and value < metadata["min"]:
        raise ValidationError(f"Field '{name}' must be >= {metadata['min']!r}.")
    if "max" in metadata and value > metadata["max"]:
        raise ValidationError(f"Field '{name}' must be <= {metadata['max']!r}.")

    validator = metadata.get("validator")
    if validator is None:
        return
    if not callable(validator):
        raise TypeError(
            f"Field '{name}' metadata 'validator' must be callable, got {type(validator).__name__}."
        )

    try:
        result = validator(value)
    except Exception as exc:  # noqa: BLE001
        raise ValidationError(
            f"Field '{name}' custom validator raised {type(exc).__name__}: {exc}"
        ) from exc
    if result is False:
        raise ValidationError(f"Field '{name}' failed custom validator.")


def _annotation_name(annotation: Any) -> str:
    origin = get_origin(annotation)
    if origin is None:
        if isinstance(annotation, type):
            return annotation.__name__
        return str(annotation)
    args = ", ".join(_annotation_name(arg) for arg in get_args(annotation))
    return f"{origin.__name__}[{args}]"
