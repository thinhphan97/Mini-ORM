"""Column SQL helpers used by schema generation."""

from __future__ import annotations

from dataclasses import MISSING
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, get_args, get_origin

from .contracts import DialectPort
from .schema_foreign_keys import parse_fk_reference


def column_sql(field: Any, dialect: DialectPort) -> str:
    """Build one column definition SQL fragment."""

    if field.metadata.get("pk") and field.metadata.get("auto"):
        return dialect.auto_pk_sql(field.name)

    column_name = dialect.q(field.name)

    sql_parts = [column_name, resolve_sql_type(field.type)]
    sql_parts.append("NULL" if is_nullable(field) else "NOT NULL")

    if field.metadata.get("pk"):
        sql_parts.append("PRIMARY KEY")
    if "fk" in field.metadata:
        ref_table, ref_column = parse_fk_reference(field.metadata["fk"])
        sql_parts.append(f"REFERENCES {dialect.q(ref_table)} ({dialect.q(ref_column)})")

    return " ".join(sql_parts)


def resolve_sql_type(annotation: Any) -> str:
    """Map Python annotation to SQL scalar type."""

    if isinstance(annotation, str):
        lowered = annotation.lower()
        if "bool" in lowered:
            return "BOOLEAN"
        if "datetime" in lowered:
            return "TIMESTAMP"
        if "date" in lowered:
            return "DATE"
        if "time" in lowered:
            return "TIME"
        if "decimal" in lowered:
            return "NUMERIC"
        if "bytes" in lowered:
            return "BLOB"
        if "int" in lowered:
            return "INTEGER"
        if "float" in lowered:
            return "REAL"
        return "TEXT"

    base_type = _unwrap_optional(annotation)

    if base_type is bool:
        return "BOOLEAN"
    if base_type is datetime:
        return "TIMESTAMP"
    if base_type is date:
        return "DATE"
    if base_type is time:
        return "TIME"
    if base_type is Decimal:
        return "NUMERIC"
    if base_type in {bytes, bytearray, memoryview}:
        return "BLOB"
    if base_type is int:
        return "INTEGER"
    if base_type is float:
        return "REAL"
    return "TEXT"


def is_nullable(field: Any) -> bool:
    """Infer whether SQL column should allow NULL values."""

    if field.default is None:
        return True

    if field.default is not MISSING:
        return False

    if isinstance(field.type, str):
        lowered = field.type.lower()
        return (
            lowered.startswith("optional[")
            or "| none" in lowered
            or "none |" in lowered
            or "typing.optional[" in lowered
        )

    origin = get_origin(field.type)
    if origin is None:
        return False

    return any(arg is type(None) for arg in get_args(field.type))


def _unwrap_optional(annotation: Any) -> Any:
    """Extract wrapped type from `Optional[T]` style annotations."""

    origin = get_origin(annotation)
    if origin is None:
        return annotation

    args = [arg for arg in get_args(annotation) if arg is not type(None)]
    if len(args) == 1:
        return args[0]
    return annotation
