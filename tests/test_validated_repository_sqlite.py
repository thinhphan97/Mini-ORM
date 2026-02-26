from __future__ import annotations

import sqlite3
import unittest
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from mini_orm import (
    C,
    Database,
    Repository,
    SQLiteDialect,
    UnifiedRepository,
    ValidationError,
    ValidatedModel,
)


@dataclass
class SignupUser(ValidatedModel):
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = field(
        default="",
        metadata={
            "unique_index": True,
            "non_empty": True,
            "pattern": r"[^@]+@[^@]+\.[^@]+",
        },
    )
    display_name: str = field(default="", metadata={"non_empty": True, "min_len": 2})
    age: int = field(default=0, metadata={"ge": 13, "le": 120})


class AccountStatus(str, Enum):
    ACTIVE = "active"
    SUSPENDED = "suspended"
    DELETED = "deleted"


@dataclass
class Account(ValidatedModel):
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    username: str = field(
        default="",
        metadata={
            "non_empty": True,
            "min_len": 3,
            "max_len": 20,
            "pattern": r"[a-zA-Z0-9_]+",
        },
    )
    status: AccountStatus = AccountStatus.ACTIVE
    credits: int = field(default=0, metadata={"ge": 0})


class ValidatedRepositorySQLiteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.db = Database(self.conn, SQLiteDialect())
        self.repo = Repository[SignupUser](self.db, SignupUser, auto_schema=True)

    def tearDown(self) -> None:
        self.conn.close()

    def test_insert_valid_object_succeeds(self) -> None:
        inserted = self.repo.insert(
            SignupUser(
                email="alice@example.com",
                display_name="Alice",
                age=24,
            )
        )

        self.assertIsNotNone(inserted.id)
        loaded = self.repo.get(inserted.id)
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.email, "alice@example.com")
        self.assertEqual(loaded.display_name, "Alice")
        self.assertEqual(loaded.age, 24)

    def test_get_or_create_respects_validation_and_does_not_insert_invalid(self) -> None:
        with self.assertRaises(ValidationError):
            self.repo.get_or_create(
                lookup={"email": "bad-email"},
                defaults={"display_name": "Alice", "age": 20},
            )
        self.assertEqual(self.repo.count(), 0)

    def test_get_or_create_valid_payload_creates_then_reuses(self) -> None:
        first, first_created = self.repo.get_or_create(
            lookup={"email": "alice@example.com"},
            defaults={"display_name": "Alice", "age": 24},
        )
        second, second_created = self.repo.get_or_create(
            lookup={"email": "alice@example.com"},
            defaults={"display_name": "Ignored", "age": 99},
        )

        self.assertTrue(first_created)
        self.assertFalse(second_created)
        self.assertIsNotNone(first.id)
        self.assertEqual(second.id, first.id)
        self.assertEqual(self.repo.count(), 1)

    def test_unified_repository_object_only_insert_with_validated_model(self) -> None:
        unified = UnifiedRepository(self.db, auto_schema=True)
        inserted = unified.insert(
            SignupUser(
                email="hub@example.com",
                display_name="Hub User",
                age=30,
            )
        )
        self.assertIsNotNone(inserted.id)
        self.assertEqual(unified.count(SignupUser), 1)

        with self.assertRaises(ValidationError):
            unified.insert(
                SignupUser(
                    email="bad-email",
                    display_name="Hub User",
                    age=30,
                )
            )
        self.assertEqual(unified.count(SignupUser), 1)

    def test_insert_invalid_pattern_rejected_before_db(self) -> None:
        with self.assertRaises(ValidationError):
            self.repo.insert(
                SignupUser(
                    email="not-an-email",
                    display_name="Test",
                    age=20,
                )
            )
        self.assertEqual(self.repo.count(), 0)

    def test_insert_invalid_age_below_minimum_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            self.repo.insert(
                SignupUser(
                    email="test@example.com",
                    display_name="Test",
                    age=12,
                )
            )
        self.assertEqual(self.repo.count(), 0)

    def test_insert_invalid_age_above_maximum_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            self.repo.insert(
                SignupUser(
                    email="test@example.com",
                    display_name="Test",
                    age=121,
                )
            )
        self.assertEqual(self.repo.count(), 0)

    def test_insert_invalid_display_name_too_short_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            self.repo.insert(
                SignupUser(
                    email="test@example.com",
                    display_name="A",
                    age=20,
                )
            )
        self.assertEqual(self.repo.count(), 0)

    def test_insert_many_validates_all_items(self) -> None:
        users = [
            SignupUser(email="user1@example.com", display_name="User One", age=20),
            SignupUser(email="user2@example.com", display_name="User Two", age=25),
        ]
        inserted = self.repo.insert_many(users)
        self.assertEqual(len(inserted), 2)
        self.assertEqual(self.repo.count(), 2)

    def test_invalid_object_construction_blocks_insert_many_input(self) -> None:
        with self.assertRaises(ValidationError):
            users = [
                SignupUser(email="user1@example.com", display_name="User One", age=20),
                SignupUser(email="bad-email", display_name="User Two", age=25),
            ]
            _ = users
        self.assertEqual(self.repo.count(), 0)

    def test_list_returns_validated_models(self) -> None:
        self.repo.insert(
            SignupUser(email="alice@example.com", display_name="Alice", age=24)
        )
        results = self.repo.list()
        self.assertEqual(len(results), 1)
        self.assertIsInstance(results[0], SignupUser)

    def test_list_with_where_condition(self) -> None:
        self.repo.insert(
            SignupUser(email="alice@example.com", display_name="Alice", age=24)
        )
        self.repo.insert(
            SignupUser(email="bob@example.com", display_name="Bob", age=30)
        )
        results = self.repo.list(where=C.eq("display_name", "Alice"))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].email, "alice@example.com")

    def test_update_validates_modified_object(self) -> None:
        inserted = self.repo.insert(
            SignupUser(email="alice@example.com", display_name="Alice", age=24)
        )
        inserted.age = 25
        rows_affected = self.repo.update(inserted)
        self.assertEqual(rows_affected, 1)

        loaded = self.repo.get(inserted.id)
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.age, 25)

    def test_invalid_object_construction_blocks_update_input(self) -> None:
        inserted = self.repo.insert(
            SignupUser(email="alice@example.com", display_name="Alice", age=24)
        )
        with self.assertRaises(ValidationError):
            invalid_user = SignupUser(
                id=inserted.id, email="alice@example.com", display_name="Alice", age=200
            )
            _ = invalid_user
        loaded = self.repo.get(inserted.id)
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.age, 24)

    def test_count_after_validation_rejection(self) -> None:
        with self.assertRaises(ValidationError):
            self.repo.insert(
                SignupUser(email="bad-email", display_name="Test", age=20)
            )
        self.assertEqual(self.repo.count(), 0)

    def test_exists_with_validated_model(self) -> None:
        self.repo.insert(
            SignupUser(email="alice@example.com", display_name="Alice", age=24)
        )
        exists = self.repo.exists(where=C.eq("email", "alice@example.com"))
        self.assertTrue(exists)

        not_exists = self.repo.exists(where=C.eq("email", "nobody@example.com"))
        self.assertFalse(not_exists)


class ValidatedRepositoryEnumTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.db = Database(self.conn, SQLiteDialect())
        self.repo = Repository[Account](self.db, Account, auto_schema=True)

    def tearDown(self) -> None:
        self.conn.close()

    def test_insert_with_enum_field(self) -> None:
        account = self.repo.insert(
            Account(username="alice123", status=AccountStatus.ACTIVE, credits=100)
        )
        self.assertEqual(account.status, AccountStatus.ACTIVE)

    def test_enum_serialized_and_deserialized_correctly(self) -> None:
        inserted = self.repo.insert(
            Account(username="bob456", status=AccountStatus.SUSPENDED, credits=50)
        )
        loaded = self.repo.get(inserted.id)
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.status, AccountStatus.SUSPENDED)
        self.assertIsInstance(loaded.status, AccountStatus)

    def test_list_with_enum_filter(self) -> None:
        self.repo.insert(
            Account(username="alice123", status=AccountStatus.ACTIVE, credits=100)
        )
        self.repo.insert(
            Account(username="bob456", status=AccountStatus.SUSPENDED, credits=50)
        )
        active = self.repo.list(where=C.eq("status", AccountStatus.ACTIVE))
        self.assertEqual(len(active), 1)
        self.assertEqual(active[0].username, "alice123")

    def test_pattern_validation_on_username(self) -> None:
        with self.assertRaises(ValidationError):
            self.repo.insert(
                Account(username="alice@invalid", status=AccountStatus.ACTIVE, credits=0)
            )

    def test_username_min_length_validation(self) -> None:
        with self.assertRaises(ValidationError):
            self.repo.insert(
                Account(username="ab", status=AccountStatus.ACTIVE, credits=0)
            )

    def test_username_max_length_validation(self) -> None:
        with self.assertRaises(ValidationError):
            self.repo.insert(
                Account(username="a" * 21, status=AccountStatus.ACTIVE, credits=0)
            )

    def test_credits_ge_constraint(self) -> None:
        with self.assertRaises(ValidationError):
            self.repo.insert(
                Account(username="alice123", status=AccountStatus.ACTIVE, credits=-1)
            )

    def test_update_where_updates_fields(self) -> None:
        self.repo.insert(
            Account(username="alice123", status=AccountStatus.ACTIVE, credits=100)
        )
        updated_count = self.repo.update_where(
            {"credits": 150}, where=C.eq("username", "alice123")
        )
        self.assertEqual(updated_count, 1)

        loaded = self.repo.list(where=C.eq("username", "alice123"))[0]
        self.assertEqual(loaded.credits, 150)

    def test_delete_where_with_validated_model(self) -> None:
        self.repo.insert(
            Account(username="alice123", status=AccountStatus.ACTIVE, credits=100)
        )
        deleted_count = self.repo.delete_where(where=C.eq("username", "alice123"))
        self.assertEqual(deleted_count, 1)
        self.assertEqual(self.repo.count(), 0)


if __name__ == "__main__":
    unittest.main()
