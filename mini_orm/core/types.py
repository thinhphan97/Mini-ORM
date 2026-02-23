"""Shared core type aliases used across contracts, repository, and ports."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Union

NamedParams = Dict[str, Any]
PositionalParams = List[Any]
QueryParams = Union[NamedParams, PositionalParams, None]

RowMapping = Mapping[str, Any]
Rows = List[RowMapping]
MaybeRow = Optional[RowMapping]
