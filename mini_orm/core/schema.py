"""Schema helpers for deriving and applying table/index SQL from models."""

from __future__ import annotations

from dataclasses import MISSING, dataclass
from typing import Any, Mapping, Sequence, Type, get_args, get_origin

from .contracts import DatabasePort, DialectPort
from .models import DataclassModel, model_fields, require_dataclass_model, table_name


@dataclass(frozen=True)
class IndexSpec:
    """Represents one index definition.

    Attributes:
        columns: Ordered column names included in the index.
        unique: Whether this is a unique index.
        name: Optional explicit index name.
    """

    columns: tuple[str, ...]
    unique: bool = False
    name: str | None = None


IndexInput = str | Sequence[str] | Mapping[str, Any] | IndexSpec


def create_table_sql(cls: Type[DataclassModel], dialect: DialectPort) -> str:
    """Build `CREATE TABLE` statement for a dataclass model.

    Args:
        cls: Dataclass model type.
        dialect: SQL dialect used for identifier quoting.

    Returns:
        `CREATE TABLE` SQL statement.
    """

    require_dataclass_model(cls)

    table_sql = dialect.q(table_name(cls))
    column_definitions = [_column_sql(field, dialect) for field in model_fields(cls)]
    return f"CREATE TABLE {table_sql} (\n  " + ",\n  ".join(column_definitions) + "\n);"


def create_index_sql(
    cls: Type[DataclassModel],
    dialect: DialectPort,
    column: str,
    *,
    unique: bool = False,
    name: str | None = None,
) -> str:
    """Build one single-column index SQL statement.

    Args:
        cls: Dataclass model type.
        dialect: SQL dialect used for identifier quoting.
        column: Column to index.
        unique: Whether to create a unique index.
        name: Optional explicit index name.

    Returns:
        `CREATE INDEX` SQL statement.

    Raises:
        ValueError: If column does not exist in model fields.
    """

    require_dataclass_model(cls)
    spec = IndexSpec(columns=(column,), unique=unique, name=name)
    return _build_index_sql(table_name(cls), spec, dialect, _model_column_names(cls))


def create_indexes_sql(cls: Type[DataclassModel], dialect: DialectPort) -> list[str]:
    """Build index SQL statements from model field metadata and `__indexes__`.

    Supported field metadata keys:
    - `index=True`: create non-unique single-column index
    - `unique_index=True`: create unique single-column index
    - `index_name="..."`: override generated name

    Supported `__indexes__` item formats:
    - `"column"`
    - `("col1", "col2")`
    - `{ "columns": ("col1", "col2"), "unique": True, "name": "..." }`

    Args:
        cls: Dataclass model type.
        dialect: SQL dialect used for identifier quoting.

    Returns:
        List of `CREATE INDEX` SQL statements.
    """

    require_dataclass_model(cls)
    table = table_name(cls)
    column_names = _model_column_names(cls)

    specs = _collect_index_specs(cls)
    return [_build_index_sql(table, spec, dialect, column_names) for spec in specs]


def create_schema_sql(cls: Type[DataclassModel], dialect: DialectPort) -> list[str]:
    """Build full schema SQL list (table first, then indexes)."""

    return [create_table_sql(cls, dialect), *create_indexes_sql(cls, dialect)]


def apply_schema(db: DatabasePort, cls: Type[DataclassModel]) -> list[str]:
    """Create table and all configured indexes for a model on a database.

    This is a convenience API to avoid manual loops:
    `db.execute(create_table_sql(...))` followed by per-index execution.

    Args:
        db: Database adapter implementing `DatabasePort`.
        cls: Dataclass model type.

    Returns:
        The list of executed SQL statements (table first, then indexes).
    """

    statements = create_schema_sql(cls, db.dialect)
    with db.transaction():
        for sql in statements:
            db.execute(sql)
    return statements


def _collect_index_specs(cls: Type[DataclassModel]) -> list[IndexSpec]:
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
        specs.append(_parse_index_input(raw))

    return _dedupe_index_specs(specs)


def _parse_index_input(raw: IndexInput) -> IndexSpec:
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

        columns = _normalize_columns(columns_raw)
        return IndexSpec(columns=columns, unique=unique, name=name)

    if isinstance(raw, Sequence):
        columns = _normalize_columns(raw)
        return IndexSpec(columns=columns)

    raise TypeError(f"Unsupported index definition type: {type(raw)}")


def _normalize_columns(raw: Any) -> tuple[str, ...]:
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


def _dedupe_index_specs(specs: Sequence[IndexSpec]) -> list[IndexSpec]:
    deduped: list[IndexSpec] = []
    seen: set[tuple[tuple[str, ...], bool, str | None]] = set()

    for spec in specs:
        key = (spec.columns, spec.unique, spec.name)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(spec)

    return deduped


def _model_column_names(cls: Type[DataclassModel]) -> set[str]:
    return {field.name for field in model_fields(cls)}


def _build_index_sql(
    table: str,
    spec: IndexSpec,
    dialect: DialectPort,
    available_columns: set[str],
) -> str:
    _validate_index_columns(spec.columns, available_columns)

    index_name = spec.name or _default_index_name(table, spec.columns, spec.unique)
    index_sql = dialect.q(index_name)
    table_sql = dialect.q(table)
    columns_sql = ", ".join(dialect.q(column) for column in spec.columns)

    prefix = "CREATE UNIQUE INDEX" if spec.unique else "CREATE INDEX"
    return f"{prefix} {index_sql} ON {table_sql} ({columns_sql});"


def _validate_index_columns(columns: Sequence[str], available_columns: set[str]) -> None:
    missing = [column for column in columns if column not in available_columns]
    if missing:
        raise ValueError(f"Index column(s) not found in model: {', '.join(missing)}")


def _default_index_name(table: str, columns: Sequence[str], unique: bool) -> str:
    prefix = "uidx" if unique else "idx"
    raw_name = f"{prefix}_{table}_{'_'.join(columns)}"
    safe = "".join(char if char.isalnum() or char == "_" else "_" for char in raw_name)
    return safe


def _column_sql(field: Any, dialect: DialectPort) -> str:
    """Build one column definition SQL fragment."""

    column_name = dialect.q(field.name)

    if field.metadata.get("pk") and field.metadata.get("auto"):
        return f"{column_name} INTEGER PRIMARY KEY"

    sql_parts = [column_name, _resolve_sql_type(field.type)]
    sql_parts.append("NULL" if _is_nullable(field) else "NOT NULL")

    if field.metadata.get("pk"):
        sql_parts.append("PRIMARY KEY")

    return " ".join(sql_parts)


def _resolve_sql_type(annotation: Any) -> str:
    """Map Python annotation to SQL scalar type."""

    if isinstance(annotation, str):
        lowered = annotation.lower()
        if "int" in lowered:
            return "INTEGER"
        if "float" in lowered:
            return "REAL"
        return "TEXT"

    base_type = _unwrap_optional(annotation)

    if base_type is int:
        return "INTEGER"
    if base_type is float:
        return "REAL"
    return "TEXT"


def _unwrap_optional(annotation: Any) -> Any:
    """Extract wrapped type from `Optional[T]` style annotations."""

    origin = get_origin(annotation)
    if origin is None:
        return annotation

    args = [arg for arg in get_args(annotation) if arg is not type(None)]
    if len(args) == 1:
        return args[0]
    return annotation


def _is_nullable(field: Any) -> bool:
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
