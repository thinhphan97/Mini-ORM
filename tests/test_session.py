from __future__ import annotations

import sqlite3
import unittest
from dataclasses import dataclass, field
from typing import Optional

from mini_orm import (
    AsyncDatabase,
    AsyncSession,
    Database,
    SQLiteDialect,
    Session,
    apply_schema,
    apply_schema_async,
)


@dataclass
class UserRow:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = ""


class SessionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.db = Database(self.conn, SQLiteDialect())
        apply_schema(self.db, UserRow)

    def tearDown(self) -> None:
        self.conn.close()

    def test_begin_commits_on_success(self) -> None:
        session = Session(self.db)
        with session.begin():
            inserted = session.insert(UserRow(email="alice@example.com"))
            self.assertIsNotNone(inserted.id)

        self.assertEqual(session.count(UserRow), 1)

    def test_begin_rolls_back_on_error(self) -> None:
        session = Session(self.db)

        with self.assertRaises(RuntimeError):
            with session.begin():
                session.insert(UserRow(email="alice@example.com"))
                raise RuntimeError("boom")

        self.assertEqual(session.count(UserRow), 0)

    def test_with_session_context_manages_transaction(self) -> None:
        with Session(self.db) as session:
            session.insert(UserRow(email="context@example.com"))
            self.assertIs(session.repo(UserRow), session.repo(UserRow))

        count = self.db.fetchone('SELECT COUNT(*) AS "count" FROM "userrow";')
        self.assertIsNotNone(count)
        if count is None:
            self.fail("Expected row count mapping.")
        self.assertEqual(count["count"], 1)


class AsyncSessionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.db = AsyncDatabase(self.conn, SQLiteDialect())
        await apply_schema_async(self.db, UserRow)

    async def asyncTearDown(self) -> None:
        self.conn.close()

    async def test_begin_commits_on_success(self) -> None:
        session = AsyncSession(self.db)
        async with session.begin():
            inserted = await session.insert(UserRow(email="alice@example.com"))
            self.assertIsNotNone(inserted.id)

        self.assertEqual(await session.count(UserRow), 1)

    async def test_begin_rolls_back_on_error(self) -> None:
        session = AsyncSession(self.db)

        with self.assertRaises(RuntimeError):
            async with session.begin():
                await session.insert(UserRow(email="alice@example.com"))
                raise RuntimeError("boom")

        self.assertEqual(await session.count(UserRow), 0)

    async def test_async_with_session_context_manages_transaction(self) -> None:
        async with AsyncSession(self.db) as session:
            await session.insert(UserRow(email="context@example.com"))
            self.assertIs(session.repo(UserRow), session.repo(UserRow))

        count = await self.db.fetchone('SELECT COUNT(*) AS "count" FROM "userrow";')
        self.assertIsNotNone(count)
        if count is None:
            self.fail("Expected row count mapping.")
        self.assertEqual(count["count"], 1)
