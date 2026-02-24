"""Schema helpers for deriving and applying table/index SQL from models."""

from __future__ import annotations

from typing import Type

from .contracts import AsyncDatabasePort, DatabasePort, DialectPort
from .models import DataclassModel, model_fields, require_dataclass_model, table_name
from .schema_columns import column_sql
from .schema_indexes import (
    IndexSpec,
    build_index_sql,
    collect_index_specs,
    model_column_names,
)


def create_table_sql(
    cls: Type[DataclassModel],
    dialect: DialectPort,
    *,
    if_not_exists: bool = False,
) -> str:
    """Build `CREATE TABLE` statement for a dataclass model."""

    require_dataclass_model(cls)

    table_sql = dialect.q(table_name(cls))
    column_definitions = [column_sql(field, dialect) for field in model_fields(cls)]
    prefix = "CREATE TABLE IF NOT EXISTS" if if_not_exists else "CREATE TABLE"
    return f"{prefix} {table_sql} (\n  " + ",\n  ".join(column_definitions) + "\n);"


def create_index_sql(
    cls: Type[DataclassModel],
    dialect: DialectPort,
    column: str,
    *,
    unique: bool = False,
    name: str | None = None,
    if_not_exists: bool = False,
) -> str:
    """Build one single-column index SQL statement."""

    require_dataclass_model(cls)
    spec = IndexSpec(columns=(column,), unique=unique, name=name)
    return build_index_sql(
        table_name(cls),
        spec,
        dialect,
        model_column_names(cls),
        if_not_exists=if_not_exists,
    )


def create_indexes_sql(
    cls: Type[DataclassModel],
    dialect: DialectPort,
    *,
    if_not_exists: bool = False,
) -> list[str]:
    """Build index SQL statements from model field metadata and `__indexes__`."""

    require_dataclass_model(cls)
    table = table_name(cls)
    columns = model_column_names(cls)
    specs = collect_index_specs(cls)
    return [
        build_index_sql(
            table,
            spec,
            dialect,
            columns,
            if_not_exists=if_not_exists,
        )
        for spec in specs
    ]


def create_schema_sql(
    cls: Type[DataclassModel],
    dialect: DialectPort,
    *,
    if_not_exists: bool = False,
) -> list[str]:
    """Build full schema SQL list (table first, then indexes)."""

    return [
        create_table_sql(cls, dialect, if_not_exists=if_not_exists),
        *create_indexes_sql(cls, dialect, if_not_exists=if_not_exists),
    ]


def apply_schema(
    db: DatabasePort,
    cls: Type[DataclassModel],
    *,
    if_not_exists: bool = False,
) -> list[str]:
    """Create table and all configured indexes for a model on a database."""

    statements = create_schema_sql(cls, db.dialect, if_not_exists=if_not_exists)
    with db.transaction():
        for sql in statements:
            db.execute(sql)
    return statements


async def apply_schema_async(
    db: AsyncDatabasePort,
    cls: Type[DataclassModel],
    *,
    if_not_exists: bool = False,
) -> list[str]:
    """Async variant of `apply_schema` with identical SQL generation."""

    statements = create_schema_sql(cls, db.dialect, if_not_exists=if_not_exists)
    async with db.transaction():
        for sql in statements:
            await db.execute(sql)
    return statements
