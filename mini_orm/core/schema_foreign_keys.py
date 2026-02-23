"""Foreign-key metadata parsing helpers for SQL schema generation."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from .models import require_dataclass_model, table_name


def parse_fk_reference(raw: Any) -> tuple[str, str]:
    """Parse `field.metadata['fk']` into `(table, column)`."""

    if isinstance(raw, str):
        parts = raw.split(".", maxsplit=1)
        if len(parts) != 2 or not parts[0] or not parts[1]:
            raise ValueError(
                "fk string must have format 'table.column', e.g. 'user.id'."
            )
        return parts[0], parts[1]

    if isinstance(raw, Mapping):
        table = _resolve_fk_table(raw.get("model"), raw.get("table"))
        column = raw.get("column", "id")
        if not isinstance(column, str) or not column:
            raise TypeError("fk mapping 'column' must be a non-empty string.")
        return table, column

    if isinstance(raw, Sequence):
        values = tuple(raw)
        if len(values) != 2:
            raise ValueError("fk sequence must have exactly 2 items: (table/model, column).")
        table = _resolve_fk_table(values[0], None)
        column = values[1]
        if not isinstance(column, str) or not column:
            raise TypeError("fk sequence column must be a non-empty string.")
        return table, column

    raise TypeError(
        "Unsupported fk format. Use 'table.column', (ModelOrTable, 'column') "
        "or {'model': Model, 'column': 'id'}."
    )


def _resolve_fk_table(model_or_table: Any, table_fallback: Any) -> str:
    if isinstance(model_or_table, str) and model_or_table:
        return model_or_table

    if isinstance(model_or_table, type):
        require_dataclass_model(model_or_table)
        return table_name(model_or_table)

    if isinstance(table_fallback, str) and table_fallback:
        return table_fallback

    raise TypeError("fk reference requires a table name string or dataclass model.")
