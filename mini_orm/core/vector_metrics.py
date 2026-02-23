"""Shared vector metric definitions and normalization helpers."""

from __future__ import annotations

from enum import Enum
from typing import Iterable, Mapping


class VectorMetric(str, Enum):
    """Supported normalized vector metric values."""

    COSINE = "cosine"
    DOT = "dot"
    L2 = "l2"


VectorMetricInput = str | VectorMetric


def normalize_vector_metric(
    metric: VectorMetricInput,
    *,
    supported: Iterable[VectorMetric] | None = None,
    aliases: Mapping[str, VectorMetric] | None = None,
) -> VectorMetric:
    """Normalize user metric input into a `VectorMetric` value."""

    alias_map = {key.lower(): value for key, value in (aliases or {}).items()}

    if isinstance(metric, VectorMetric):
        normalized = metric
    elif isinstance(metric, str):
        key = metric.strip().lower()
        if key in VectorMetric._value2member_map_:
            normalized = VectorMetric(key)
        elif key in alias_map:
            normalized = alias_map[key]
        else:
            allowed = sorted(
                set(VectorMetric._value2member_map_.keys()) | set(alias_map.keys())
            )
            raise ValueError(
                f"Unsupported metric: {metric}. Supported: {allowed}"
            )
    else:
        raise ValueError(f"Unsupported metric type: {type(metric).__name__}")

    if supported is not None:
        supported_set = set(supported)
        if normalized not in supported_set:
            allowed = sorted(item.value for item in supported_set)
            raise ValueError(
                f"Unsupported metric: {normalized.value}. Supported: {allowed}"
            )

    return normalized
