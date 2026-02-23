"""Query condition primitives for repository filtering and sorting."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Sequence


@dataclass(frozen=True)
class Condition:
    """Represents one SQL condition expression.

    Attributes:
        col: Raw column name.
        op: SQL operator (for example `=`, `IN`, `IS NULL`).
        value: Scalar value for binary operators.
        values: Sequence value for `IN`.
        is_unary: Whether the operator is unary (`IS NULL`, `IS NOT NULL`).
    """

    col: str
    op: str
    value: Any = None
    values: Optional[Sequence[Any]] = None
    is_unary: bool = False


class C:
    """Fluent condition factory methods."""

    @staticmethod
    def eq(col: str, val: Any) -> Condition:
        """Build `col = value` condition."""

        return Condition(col=col, op="=", value=val)

    @staticmethod
    def ne(col: str, val: Any) -> Condition:
        """Build `col <> value` condition."""

        return Condition(col=col, op="<>", value=val)

    @staticmethod
    def lt(col: str, val: Any) -> Condition:
        """Build `col < value` condition."""

        return Condition(col=col, op="<", value=val)

    @staticmethod
    def le(col: str, val: Any) -> Condition:
        """Build `col <= value` condition."""

        return Condition(col=col, op="<=", value=val)

    @staticmethod
    def gt(col: str, val: Any) -> Condition:
        """Build `col > value` condition."""

        return Condition(col=col, op=">", value=val)

    @staticmethod
    def ge(col: str, val: Any) -> Condition:
        """Build `col >= value` condition."""

        return Condition(col=col, op=">=", value=val)

    @staticmethod
    def like(col: str, pattern: str) -> Condition:
        """Build `col LIKE pattern` condition."""

        return Condition(col=col, op="LIKE", value=pattern)

    @staticmethod
    def is_null(col: str) -> Condition:
        """Build `col IS NULL` condition."""

        return Condition(col=col, op="IS NULL", is_unary=True)

    @staticmethod
    def is_not_null(col: str) -> Condition:
        """Build `col IS NOT NULL` condition."""

        return Condition(col=col, op="IS NOT NULL", is_unary=True)

    @staticmethod
    def in_(col: str, values: Sequence[Any]) -> Condition:
        """Build `col IN (...)` condition."""

        return Condition(col=col, op="IN", values=list(values))


@dataclass(frozen=True)
class OrderBy:
    """Represents one ordering expression."""

    col: str
    desc: bool = False
