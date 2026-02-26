from __future__ import annotations

import unittest
from dataclasses import dataclass, field
from typing import Literal, Optional

from mini_orm import ValidatedModel, ValidationError


@dataclass
class ValidUser(ValidatedModel):
    email: str = field(
        default="",
        metadata={
            "non_empty": True,
            "pattern": r"[^@]+@[^@]+\.[^@]+",
            "min_len": 5,
        },
    )
    age: int = field(default=0, metadata={"ge": 0, "le": 130})
    tags: list[str] = field(default_factory=list, metadata={"max_len": 3})


@dataclass
class RequiredNullableField(ValidatedModel):
    code: Optional[str] = field(default=None, metadata={"required": True})


@dataclass
class CustomValidatorField(ValidatedModel):
    sku: str = field(default="", metadata={"validator": lambda value: value.startswith("SKU-")})


@dataclass
class DateRange(ValidatedModel):
    start_day: int = 0
    end_day: int = 0

    def model_validate(self) -> None:
        if self.end_day <= self.start_day:
            raise ValidationError("end_day must be greater than start_day.")


@dataclass
class LiteralMixedField(ValidatedModel):
    value: Literal[1, "one"] = 1


@dataclass
class FloatField(ValidatedModel):
    amount: float = 0.0


class ValidatedModelTests(unittest.TestCase):
    def test_valid_data_passes(self) -> None:
        user = ValidUser(email="alice@example.com", age=20, tags=["vip"])
        self.assertEqual(user.email, "alice@example.com")
        self.assertEqual(user.age, 20)
        self.assertEqual(user.tags, ["vip"])

    def test_invalid_type_raises(self) -> None:
        with self.assertRaises(ValidationError):
            ValidUser(email="alice@example.com", age="20")  # type: ignore[arg-type]

    def test_pattern_constraint_raises(self) -> None:
        with self.assertRaises(ValidationError):
            ValidUser(email="not-email", age=20)

    def test_list_item_type_raises(self) -> None:
        with self.assertRaises(ValidationError):
            ValidUser(email="alice@example.com", age=20, tags=["ok", 1])  # type: ignore[list-item]

    def test_required_nullable_field_raises_on_none(self) -> None:
        with self.assertRaises(ValidationError):
            RequiredNullableField()

    def test_custom_field_validator_raises(self) -> None:
        with self.assertRaises(ValidationError):
            CustomValidatorField(sku="BAD-001")

    def test_model_level_hook_raises(self) -> None:
        with self.assertRaises(ValidationError):
            DateRange(start_day=3, end_day=3)

    def test_literal_mixed_type_error(self) -> None:
        with self.assertRaises(ValidationError):
            LiteralMixedField(value=2)

    def test_bool_rejected_for_float(self) -> None:
        with self.assertRaises(ValidationError):
            FloatField(amount=True)  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
