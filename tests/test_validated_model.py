from __future__ import annotations

import unittest
from dataclasses import dataclass, field
from enum import Enum
from typing import Literal, Optional, Sequence, Union

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


@dataclass
class DictField(ValidatedModel):
    metadata: dict[str, int] = field(default_factory=dict)


@dataclass
class SetField(ValidatedModel):
    tags: set[str] = field(default_factory=set)


@dataclass
class FrozenSetField(ValidatedModel):
    immutable_tags: frozenset[str] = field(default_factory=frozenset)


@dataclass
class TupleEllipsisField(ValidatedModel):
    coords: tuple[int, ...] = field(default_factory=tuple)


@dataclass
class TupleFixedField(ValidatedModel):
    pair: tuple[str, int] = ("", 0)


@dataclass
class OptionalWithDefault(ValidatedModel):
    name: Optional[str] = None


@dataclass
class UnionField(ValidatedModel):
    value: Union[int, str] = 0


@dataclass
class NestedListField(ValidatedModel):
    matrix: list[list[int]] = field(default_factory=list)


@dataclass
class ChoicesField(ValidatedModel):
    status: str = field(default="draft", metadata={"choices": ["draft", "published", "archived"]})


@dataclass
class UnhashableChoicesField(ValidatedModel):
    payload: list[int] = field(
        default_factory=lambda: [1, 2],
        metadata={"choices": [[1, 2], [3, 4]]},
    )


@dataclass
class MinMaxField(ValidatedModel):
    score: int = field(default=0, metadata={"min": 0, "max": 100})


@dataclass
class NonEmptyWhitespace(ValidatedModel):
    text: str = field(default="", metadata={"non_empty": True})


@dataclass
class BoundaryConstraints(ValidatedModel):
    min_len_zero: str = field(default="", metadata={"min_len": 0})
    max_len_zero: list[str] = field(default_factory=list, metadata={"max_len": 0})
    gt_value: int = field(default=1, metadata={"gt": 0})
    lt_value: int = field(default=99, metadata={"lt": 100})


@dataclass
class ValidatorNoneReturn(ValidatedModel):
    code: str = field(default="", metadata={"validator": lambda x: None})


@dataclass
class ValidatorRaisesError(ValidatedModel):
    code: str = field(default="", metadata={"validator": lambda x: int("bad")})


class Status(str, Enum):
    DRAFT = "draft"
    PUBLISHED = "published"


@dataclass
class EnumField(ValidatedModel):
    status: Status = Status.DRAFT


@dataclass
class SequenceField(ValidatedModel):
    items: Sequence[int] = field(default_factory=list)


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
            LiteralMixedField(value=True)  # type: ignore[arg-type]

        accepted = LiteralMixedField(value=1)
        self.assertEqual(accepted.value, 1)

    def test_bool_rejected_for_float(self) -> None:
        with self.assertRaises(ValidationError):
            FloatField(amount=True)  # type: ignore[arg-type]

    def test_dict_valid_key_value_types(self) -> None:
        valid = DictField(metadata={"count": 10, "total": 100})
        self.assertEqual(valid.metadata, {"count": 10, "total": 100})

    def test_dict_invalid_key_type_raises(self) -> None:
        with self.assertRaises(ValidationError):
            DictField(metadata={1: 10})  # type: ignore[dict-item]

    def test_dict_invalid_value_type_raises(self) -> None:
        with self.assertRaises(ValidationError):
            DictField(metadata={"key": "not-an-int"})  # type: ignore[dict-item]

    def test_set_valid_items(self) -> None:
        valid = SetField(tags={"tag1", "tag2"})
        self.assertEqual(valid.tags, {"tag1", "tag2"})

    def test_set_invalid_item_type_raises(self) -> None:
        with self.assertRaises(ValidationError):
            SetField(tags={"tag1", 123})  # type: ignore[arg-type]

    def test_frozenset_valid_items(self) -> None:
        valid = FrozenSetField(immutable_tags=frozenset(["tag1", "tag2"]))
        self.assertEqual(valid.immutable_tags, frozenset(["tag1", "tag2"]))

    def test_frozenset_invalid_item_type_raises(self) -> None:
        with self.assertRaises(ValidationError):
            FrozenSetField(immutable_tags=frozenset(["tag1", 123]))  # type: ignore[arg-type]

    def test_tuple_ellipsis_valid(self) -> None:
        valid = TupleEllipsisField(coords=(1, 2, 3))
        self.assertEqual(valid.coords, (1, 2, 3))

    def test_tuple_ellipsis_invalid_item_type_raises(self) -> None:
        with self.assertRaises(ValidationError):
            TupleEllipsisField(coords=(1, 2, "three"))  # type: ignore[arg-type]

    def test_tuple_fixed_valid(self) -> None:
        valid = TupleFixedField(pair=("name", 42))
        self.assertEqual(valid.pair, ("name", 42))

    def test_tuple_fixed_wrong_length_raises(self) -> None:
        with self.assertRaises(ValidationError):
            TupleFixedField(pair=("name", 42, "extra"))  # type: ignore[arg-type]

    def test_tuple_fixed_invalid_item_type_raises(self) -> None:
        with self.assertRaises(ValidationError):
            TupleFixedField(pair=(123, 42))  # type: ignore[arg-type]

    def test_optional_none_allowed(self) -> None:
        valid = OptionalWithDefault(name=None)
        self.assertIsNone(valid.name)

    def test_optional_value_allowed(self) -> None:
        valid = OptionalWithDefault(name="Alice")
        self.assertEqual(valid.name, "Alice")

    def test_union_first_type_valid(self) -> None:
        valid = UnionField(value=42)
        self.assertEqual(valid.value, 42)

    def test_union_second_type_valid(self) -> None:
        valid = UnionField(value="text")
        self.assertEqual(valid.value, "text")

    def test_union_invalid_type_raises(self) -> None:
        with self.assertRaises(ValidationError):
            UnionField(value=[1, 2, 3])  # type: ignore[arg-type]

    def test_nested_list_valid(self) -> None:
        valid = NestedListField(matrix=[[1, 2], [3, 4]])
        self.assertEqual(valid.matrix, [[1, 2], [3, 4]])

    def test_nested_list_invalid_inner_type_raises(self) -> None:
        with self.assertRaises(ValidationError):
            NestedListField(matrix=[[1, 2], ["a", "b"]])  # type: ignore[list-item]

    def test_choices_valid_value(self) -> None:
        valid = ChoicesField(status="published")
        self.assertEqual(valid.status, "published")

    def test_choices_invalid_value_raises(self) -> None:
        with self.assertRaises(ValidationError):
            ChoicesField(status="invalid")

    def test_choices_with_unhashable_item_valid(self) -> None:
        valid = UnhashableChoicesField(payload=[1, 2])
        self.assertEqual(valid.payload, [1, 2])

    def test_choices_with_unhashable_item_invalid_raises(self) -> None:
        with self.assertRaises(ValidationError):
            UnhashableChoicesField(payload=[9, 9])

    def test_min_max_constraint_valid(self) -> None:
        valid = MinMaxField(score=50)
        self.assertEqual(valid.score, 50)

    def test_min_constraint_violation_raises(self) -> None:
        with self.assertRaises(ValidationError):
            MinMaxField(score=-1)

    def test_max_constraint_violation_raises(self) -> None:
        with self.assertRaises(ValidationError):
            MinMaxField(score=101)

    def test_non_empty_whitespace_only_raises(self) -> None:
        with self.assertRaises(ValidationError):
            NonEmptyWhitespace(text="   ")

    def test_non_empty_valid_text(self) -> None:
        valid = NonEmptyWhitespace(text="hello")
        self.assertEqual(valid.text, "hello")

    def test_boundary_min_len_zero_allows_empty(self) -> None:
        valid = BoundaryConstraints(min_len_zero="")
        self.assertEqual(valid.min_len_zero, "")

    def test_boundary_max_len_zero_requires_empty(self) -> None:
        valid = BoundaryConstraints(max_len_zero=[])
        self.assertEqual(valid.max_len_zero, [])

    def test_boundary_max_len_zero_rejects_nonempty(self) -> None:
        with self.assertRaises(ValidationError):
            BoundaryConstraints(max_len_zero=["item"])

    def test_boundary_gt_constraint(self) -> None:
        valid = BoundaryConstraints(gt_value=1)
        self.assertEqual(valid.gt_value, 1)

    def test_boundary_gt_constraint_violation_raises(self) -> None:
        with self.assertRaises(ValidationError):
            BoundaryConstraints(gt_value=0)

    def test_boundary_lt_constraint(self) -> None:
        valid = BoundaryConstraints(lt_value=99)
        self.assertEqual(valid.lt_value, 99)

    def test_boundary_lt_constraint_violation_raises(self) -> None:
        with self.assertRaises(ValidationError):
            BoundaryConstraints(lt_value=100)

    def test_validator_none_return_treated_as_valid(self) -> None:
        valid = ValidatorNoneReturn(code="any")
        self.assertEqual(valid.code, "any")

    def test_validator_raises_exception_wrapped_in_validation_error(self) -> None:
        with self.assertRaises(ValidationError):
            ValidatorRaisesError(code="test")

    def test_enum_valid_value(self) -> None:
        valid = EnumField(status=Status.PUBLISHED)
        self.assertEqual(valid.status, Status.PUBLISHED)

    def test_enum_invalid_type_raises(self) -> None:
        with self.assertRaises(ValidationError):
            EnumField(status="published")  # type: ignore[arg-type]

    def test_int_accepts_int_not_bool(self) -> None:
        user = ValidUser(email="alice@example.com", age=25, tags=[])
        self.assertEqual(user.age, 25)

    def test_int_rejects_bool(self) -> None:
        with self.assertRaises(ValidationError):
            ValidUser(email="alice@example.com", age=True, tags=[])  # type: ignore[arg-type]

    def test_float_accepts_int(self) -> None:
        valid = FloatField(amount=42)
        self.assertEqual(valid.amount, 42)

    def test_ge_boundary_exact_value(self) -> None:
        valid = ValidUser(email="alice@example.com", age=0, tags=[])
        self.assertEqual(valid.age, 0)

    def test_le_boundary_exact_value(self) -> None:
        valid = ValidUser(email="alice@example.com", age=130, tags=[])
        self.assertEqual(valid.age, 130)

    def test_max_len_boundary_exact(self) -> None:
        valid = ValidUser(email="alice@example.com", age=20, tags=["a", "b", "c"])
        self.assertEqual(len(valid.tags), 3)

    def test_max_len_boundary_violation(self) -> None:
        with self.assertRaises(ValidationError):
            ValidUser(email="alice@example.com", age=20, tags=["a", "b", "c", "d"])

    def test_min_len_boundary_exact(self) -> None:
        valid = ValidUser(email="ab@c.d", age=20, tags=[])
        self.assertEqual(len(valid.email), 6)

    def test_min_len_boundary_violation(self) -> None:
        with self.assertRaises(ValidationError):
            ValidUser(email="a@b.", age=20, tags=[])

    def test_pattern_full_match_required(self) -> None:
        with self.assertRaises(ValidationError):
            ValidUser(email="alice@example", age=20, tags=[])

    def test_sequence_accepts_tuple(self) -> None:
        valid = SequenceField(items=(1, 2, 3))
        self.assertEqual(valid.items, (1, 2, 3))

    def test_sequence_rejects_string(self) -> None:
        with self.assertRaises(ValidationError):
            SequenceField(items="abc")  # type: ignore[arg-type]

    def test_sequence_rejects_bytes(self) -> None:
        with self.assertRaises(ValidationError):
            SequenceField(items=b"abc")  # type: ignore[arg-type]

    def test_sequence_invalid_item_type_raises(self) -> None:
        with self.assertRaises(ValidationError):
            SequenceField(items=[1, "bad"])  # type: ignore[list-item]

    def test_error_message_contains_field_name(self) -> None:
        with self.assertRaises(ValidationError) as ctx:
            ValidUser(email="bad", age=20)
        self.assertIn("email", str(ctx.exception))

    def test_error_message_contains_constraint_info(self) -> None:
        with self.assertRaises(ValidationError) as ctx:
            ValidUser(email="alice@example.com", age=-1)
        self.assertIn(">=", str(ctx.exception))
        self.assertIn("0", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
