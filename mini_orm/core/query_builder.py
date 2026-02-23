"""SQL fragment builders for filtering, sorting, and paging.

This module centralizes SQL string compilation from query inputs. It keeps
`Repository` focused on orchestration while making SQL generation reusable and
easy to extend.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional, Sequence, Tuple

from .conditions import Condition, OrderBy
from .contracts import DialectPort
from .types import NamedParams, PositionalParams, QueryParams


WhereInput = Optional[Sequence[Condition] | Condition]


@dataclass(frozen=True)
class CompiledFragment:
    """Represents a compiled SQL fragment with its bound parameters."""

    sql: str
    params: QueryParams


class _ParamNameGenerator:
    """Generates safe, unique parameter names for named SQL styles."""

    def __init__(self) -> None:
        self._counter = 0

    def next(self, base: str) -> str:
        """Return a deterministic parameter name based on a column hint."""

        self._counter += 1
        safe = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in base)
        return f"{safe}_{self._counter}"


def compile_where(where: WhereInput, dialect: DialectPort) -> CompiledFragment:
    """Compile one or many conditions into a SQL `WHERE` fragment.

    Multiple conditions are combined using `AND`.

    Args:
        where: A single condition, a list of conditions, or `None`.
        dialect: SQL dialect used for identifier quoting and placeholders.

    Returns:
        A compiled SQL fragment and parameters. Empty fragment if no condition.
    """

    if where is None:
        return CompiledFragment("", None)

    conditions = [where] if isinstance(where, Condition) else list(where)
    if not conditions:
        return CompiledFragment("", None)

    generator = _ParamNameGenerator()
    clauses: List[str] = []
    params: QueryParams = {} if dialect.paramstyle == "named" else []

    for item in conditions:
        clause, fragment_params = _compile_condition(item, dialect, generator)
        clauses.append(clause)
        _merge_params(params, fragment_params)

    return CompiledFragment(f" WHERE {' AND '.join(clauses)}", params)


def compile_order_by(
    order_by: Optional[Sequence[OrderBy]], dialect: DialectPort
) -> str:
    """Compile `ORDER BY` clause from ordering inputs.

    Args:
        order_by: Ordering expressions or `None`.
        dialect: SQL dialect used for identifier quoting.

    Returns:
        SQL `ORDER BY` fragment or an empty string.
    """

    if not order_by:
        return ""

    ordered_cols = ", ".join(
        f"{dialect.q(item.col)} {'DESC' if item.desc else 'ASC'}" for item in order_by
    )
    return f" ORDER BY {ordered_cols}"


def append_limit_offset(
    sql: str,
    params: QueryParams,
    *,
    limit: Optional[int],
    offset: Optional[int],
    dialect: DialectPort,
) -> Tuple[str, QueryParams]:
    """Append pagination clauses and merge parameters.

    Args:
        sql: Base SQL string.
        params: Existing parameters from previous fragment compilation.
        limit: Optional row limit.
        offset: Optional row offset.
        dialect: SQL dialect used for placeholder style.

    Returns:
        Updated SQL and merged parameters.
    """

    if dialect.paramstyle == "named":
        named_params: NamedParams = {}
        if isinstance(params, dict):
            named_params.update(params)
        if limit is not None:
            named_params["__limit"] = limit
            sql += " LIMIT :__limit"
        if offset is not None:
            named_params["__offset"] = offset
            sql += " OFFSET :__offset"
        return sql, named_params if named_params else None

    positional_params: PositionalParams = []
    if isinstance(params, list):
        positional_params.extend(params)
    if limit is not None:
        sql += f" LIMIT {dialect.placeholder('limit')}"
        positional_params.append(limit)
    if offset is not None:
        sql += f" OFFSET {dialect.placeholder('offset')}"
        positional_params.append(offset)
    return sql, positional_params if positional_params else None


def _compile_condition(
    condition: Condition,
    dialect: DialectPort,
    generator: _ParamNameGenerator,
) -> Tuple[str, QueryParams]:
    """Compile one condition into SQL and parameters."""

    col_sql = dialect.q(condition.col)

    if condition.is_unary:
        return f"{col_sql} {condition.op}", _empty_params(dialect)

    if condition.op == "IN":
        values = list(condition.values or [])
        if not values:
            return "1=0", _empty_params(dialect)

        keys = [generator.next(condition.col) for _ in values]
        if dialect.paramstyle == "named":
            placeholders = ", ".join(f":{key}" for key in keys)
            return (
                f"{col_sql} IN ({placeholders})",
                {key: value for key, value in zip(keys, values)},
            )

        placeholders = ", ".join(dialect.placeholder(key) for key in keys)
        return f"{col_sql} IN ({placeholders})", list(values)

    key = generator.next(condition.col)
    if dialect.paramstyle == "named":
        return f"{col_sql} {condition.op} :{key}", {key: condition.value}

    return (
        f"{col_sql} {condition.op} {dialect.placeholder(key)}",
        [condition.value],
    )


def _empty_params(dialect: DialectPort) -> QueryParams:
    """Return empty parameters matching dialect param style."""

    return {} if dialect.paramstyle == "named" else []


def _merge_params(target: QueryParams, source: QueryParams) -> None:
    """Merge parameter collections in place."""

    if isinstance(target, dict) and isinstance(source, dict):
        target.update(source)
    elif isinstance(target, list) and isinstance(source, list):
        target.extend(source)
