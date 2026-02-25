from __future__ import annotations

import sqlite3
import unittest
from dataclasses import dataclass, field
from typing import Optional

from mini_orm import (
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
        if loaded is None:
            self.fail("Expected inserted user to be retrievable.")
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


if __name__ == "__main__":
    unittest.main()
