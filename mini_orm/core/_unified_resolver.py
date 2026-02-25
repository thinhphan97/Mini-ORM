"""Shared helpers for resolving model class from unified repository inputs."""

from __future__ import annotations

from typing import Sequence, Type, TypeVar, cast

from .models import DataclassModel, require_dataclass_model

T = TypeVar("T", bound=DataclassModel)


def resolve_model_and_obj(
    model_or_object: Type[T] | T,
    obj: T | None = None,
) -> tuple[Type[T], T]:
    """Resolve `(model, object)` from explicit or inferred unified call style."""

    if obj is None:
        if isinstance(model_or_object, type):
            raise TypeError("Object instance is required when passing a model class.")
        inferred_model = type(model_or_object)
        require_dataclass_model(inferred_model)
        return cast(Type[T], inferred_model), cast(T, model_or_object)

    if not isinstance(model_or_object, type):
        raise TypeError(
            "First argument must be a model class when second argument is provided."
        )
    require_dataclass_model(model_or_object)
    if not isinstance(obj, model_or_object):
        raise TypeError(
            f"Object type {type(obj).__name__} does not match model "
            f"{model_or_object.__name__}."
        )
    return model_or_object, obj


def resolve_model_and_objects(
    model_or_list: Type[T] | Sequence[T],
    objects: Sequence[T] | None = None,
) -> tuple[Type[T], Sequence[T]]:
    """Resolve `(model, objects)` from explicit or inferred unified call style."""

    if objects is None:
        if isinstance(model_or_list, type):
            raise ValueError(
                "When passing a model class, you must also pass objects: "
                "insert_many(Model, objects)."
            )
        inferred_objects = cast(Sequence[T], model_or_list)
        if not inferred_objects:
            raise ValueError(
                "Cannot infer model from an empty objects sequence. "
                "Pass model explicitly: insert_many(Model, objects)."
            )
        inferred_model = type(inferred_objects[0])
        require_dataclass_model(inferred_model)
        for item in inferred_objects:
            if not isinstance(item, inferred_model):
                raise TypeError("All objects must share the same model class.")
        return cast(Type[T], inferred_model), inferred_objects

    if not isinstance(model_or_list, type):
        raise TypeError(
            "First argument must be a model class when second argument is provided."
        )
    require_dataclass_model(model_or_list)
    for item in objects:
        if not isinstance(item, model_or_list):
            raise TypeError("All objects must match the provided model class.")
    return model_or_list, objects
