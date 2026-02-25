"""Vector store capability and id policy definitions."""

from __future__ import annotations

from enum import Enum


class VectorIdPolicy(str, Enum):
    """Identifier policy used by a vector backend."""

    ANY = "any"
    UUID = "uuid"
