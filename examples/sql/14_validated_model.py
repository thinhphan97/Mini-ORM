"""Validated dataclass model example (pydantic-like basic checks)."""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path

# Allow running this script directly from repository root.
PROJECT_ROOT = next(
    (parent for parent in Path(__file__).resolve().parents if (parent / "mini_orm").exists()),
    None,
)
if PROJECT_ROOT and str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from mini_orm import ValidatedModel, ValidationError


@dataclass
class CreateUserInput(ValidatedModel):
    email: str = field(
        default="",
        metadata={
            "non_empty": True,
            "pattern": r"[^@]+@[^@]+\.[^@]+",
        },
    )
    age: int = field(default=0, metadata={"ge": 0, "le": 130})


def main() -> None:
    ok = CreateUserInput(email="alice@example.com", age=25)
    print("Valid input:", ok)

    try:
        CreateUserInput(email="bad-email", age=-2)
    except ValidationError as exc:
        print("Validation error:", exc)


if __name__ == "__main__":
    main()
