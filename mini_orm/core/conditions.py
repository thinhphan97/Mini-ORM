"""Query condition primitives for repository filtering and sorting."""

from __future__ import annotations

from collections.abc import Sequence as SequenceABC
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


@dataclass(frozen=True)
class ConditionGroup:
    """Represents a grouped logical expression (`AND`/`OR`)."""

    operator: str
    items: tuple["WhereExpression", ...]


@dataclass(frozen=True)
class NotCondition:
    """Represents a negated expression."""

    item: "WhereExpression"


WhereExpression = Condition | ConditionGroup | NotCondition


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

    @staticmethod
    def and_(*items: WhereExpression | Sequence[WhereExpression]) -> ConditionGroup:
        """Build a grouped `AND` expression."""

        normalized = C._normalize_group_items(items)
        return ConditionGroup(operator="AND", items=normalized)

    @staticmethod
    def or_(*items: WhereExpression | Sequence[WhereExpression]) -> ConditionGroup:
        """Build a grouped `OR` expression."""

        normalized = C._normalize_group_items(items)
        return ConditionGroup(operator="OR", items=normalized)

    @staticmethod
    def not_(item: WhereExpression) -> NotCondition:
        """Build a negated expression (`NOT (...)`)."""

        C._ensure_expr(item)
        return NotCondition(item=item)

    @staticmethod
    def _normalize_group_items(
        items: Sequence[WhereExpression | Sequence[WhereExpression]],
    ) -> tuple[WhereExpression, ...]:
        normalized_input: Sequence[WhereExpression | Sequence[WhereExpression]]
        if (
            len(items) == 1
            and isinstance(items[0], SequenceABC)
            and not isinstance(
                items[0], (str, bytes, Condition, ConditionGroup, NotCondition)
            )
        ):
            normalized_input = items[0]
        else:
            normalized_input = items

        normalized: list[WhereExpression] = []
        for item in normalized_input:
            C._ensure_expr(item)
            normalized.append(item)

        if not normalized:
            raise ValueError("Grouped condition must contain at least one expression.")
        return tuple(normalized)

    @staticmethod
    def _ensure_expr(item: Any) -> None:
        if not isinstance(item, (Condition, ConditionGroup, NotCondition)):
            raise TypeError(
                "Expression must be Condition, ConditionGroup, or NotCondition."
            )


@dataclass(frozen=True)
class OrderBy:
    """Represents one ordering expression."""

    col: str
    desc: bool = False
