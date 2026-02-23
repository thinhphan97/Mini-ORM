"""Index SQL helpers used by schema generation."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Type

from .contracts import DialectPort
from .models import DataclassModel, model_fields


@dataclass(frozen=True)
class IndexSpec:
    """Represents one index definition."""

    columns: tuple[str, ...]
    unique: bool = False
    name: str | None = None


IndexInput = str | Sequence[str] | Mapping[str, Any] | IndexSpec


def collect_index_specs(cls: Type[DataclassModel]) -> list[IndexSpec]:
    """Collect index specs from field metadata and model `__indexes__`."""

    specs: list[IndexSpec] = []

    for field in model_fields(cls):
        has_index = bool(field.metadata.get("index"))
        unique = bool(field.metadata.get("unique_index"))
        if not has_index and not unique:
            continue
        specs.append(
            IndexSpec(
                columns=(field.name,),
                unique=unique,
                name=field.metadata.get("index_name"),
            )
        )

    raw_indexes = getattr(cls, "__indexes__", ())
    for raw in raw_indexes:
        specs.append(parse_index_input(raw))

    return dedupe_index_specs(specs)


def model_column_names(cls: Type[DataclassModel]) -> set[str]:
    return {field.name for field in model_fields(cls)}


def build_index_sql(
    table: str,
    spec: IndexSpec,
    dialect: DialectPort,
    available_columns: set[str],
    *,
    if_not_exists: bool = False,
) -> str:
    """Build one `CREATE INDEX` SQL statement from one spec."""

    validate_index_columns(spec.columns, available_columns)

    index_name = spec.name or default_index_name(table, spec.columns, spec.unique)
    index_sql = dialect.q(index_name)
    table_sql = dialect.q(table)
    columns_sql = ", ".join(dialect.q(column) for column in spec.columns)

    prefix = "CREATE UNIQUE INDEX" if spec.unique else "CREATE INDEX"
    if if_not_exists and supports_index_if_not_exists(dialect):
        prefix += " IF NOT EXISTS"
    return f"{prefix} {index_sql} ON {table_sql} ({columns_sql});"


def parse_index_input(raw: IndexInput) -> IndexSpec:
    if isinstance(raw, IndexSpec):
        if not raw.columns:
            raise ValueError("IndexSpec.columns must not be empty.")
        return raw

    if isinstance(raw, str):
        return IndexSpec(columns=(raw,))

    if isinstance(raw, Mapping):
        columns_raw = raw.get("columns")
        unique = bool(raw.get("unique", False))
        name = raw.get("name")
        columns = normalize_columns(columns_raw)
        return IndexSpec(columns=columns, unique=unique, name=name)

    if isinstance(raw, Sequence):
        columns = normalize_columns(raw)
        return IndexSpec(columns=columns)

    raise TypeError(f"Unsupported index definition type: {type(raw)}")


def normalize_columns(raw: Any) -> tuple[str, ...]:
    if isinstance(raw, str):
        return (raw,)

    if not isinstance(raw, Sequence):
        raise TypeError("Index columns must be a string or sequence of strings.")

    columns = tuple(raw)
    if not columns:
        raise ValueError("Index columns must not be empty.")
    if not all(isinstance(col, str) and col for col in columns):
        raise TypeError("All index column names must be non-empty strings.")
    return columns


def dedupe_index_specs(specs: Sequence[IndexSpec]) -> list[IndexSpec]:
    deduped: list[IndexSpec] = []
    seen: set[tuple[tuple[str, ...], bool, str | None]] = set()

    for spec in specs:
        key = (spec.columns, spec.unique, spec.name)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(spec)

    return deduped


def validate_index_columns(columns: Sequence[str], available_columns: set[str]) -> None:
    missing = [column for column in columns if column not in available_columns]
    if missing:
        raise ValueError(f"Index column(s) not found in model: {', '.join(missing)}")


def default_index_name(table: str, columns: Sequence[str], unique: bool) -> str:
    prefix = "uidx" if unique else "idx"
    raw_name = f"{prefix}_{table}_{'_'.join(columns)}"
    safe = "".join(char if char.isalnum() or char == "_" else "_" for char in raw_name)
    return safe


def supports_index_if_not_exists(dialect: DialectPort) -> bool:
    name = getattr(dialect, "name", "").lower()
    return name in {"sqlite", "postgres"}
