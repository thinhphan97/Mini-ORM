from __future__ import annotations

import sqlite3
import unittest
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from mini_orm import C, Database, OrderBy, Repository, SQLiteDialect, UnifiedRepository, apply_schema
from mini_orm.ports.db_api.dialects import Dialect


@dataclass
class UserRow:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = field(default="", metadata={"unique_index": True})
    age: Optional[int] = None


@dataclass
class AuthorRow:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    name: str = ""


@dataclass
class PostRow:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    author_id: Optional[int] = field(default=None, metadata={"fk": (AuthorRow, "id")})
    title: str = ""


AuthorRow.__relations__ = {
    "posts": {
        "model": PostRow,
        "local_key": "id",
        "remote_key": "author_id",
        "type": "has_many",
    }
}

PostRow.__relations__ = {
    "author": {
        "model": AuthorRow,
        "local_key": "author_id",
        "remote_key": "id",
        "type": "belongs_to",
    }
}


@dataclass
class AutoAuthor:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    name: str = ""


@dataclass
class AutoPost:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    author_id: Optional[int] = field(
        default=None,
        metadata={
            "fk": (AutoAuthor, "id"),
            "relation": "author",
            "related_name": "posts",
        },
    )
    title: str = ""


@dataclass
class OnlyPkRow:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})


@dataclass
class MultiPkRow:
    id1: int = field(default=0, metadata={"pk": True})
    id2: int = field(default=0, metadata={"pk": True})


@dataclass
class NonUniqueLookupRow:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = ""


class TicketStatus(str, Enum):
    OPEN = "open"
    CLOSED = "closed"


@dataclass
class TicketRow:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    status: TicketStatus = TicketStatus.OPEN
    payload: dict[str, Any] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)


@dataclass
class AutoSchemaUserV1:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = ""


AutoSchemaUserV1.__table__ = "autoschema_user"


@dataclass
class AutoSchemaUserV2:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: str = ""
    age: Optional[int] = None


AutoSchemaUserV2.__table__ = "autoschema_user"


@dataclass
class AutoSchemaUserIncompatible:
    id: Optional[int] = field(default=None, metadata={"pk": True, "auto": True})
    email: int = 0


AutoSchemaUserIncompatible.__table__ = "autoschema_user"


class PlainModel:
    pass


class _NoReturningDialect(Dialect):
    paramstyle = "named"
    supports_returning = False
    quote_char = '"'


class _NoReturningDb:
    def __init__(self) -> None:
        self.dialect = _NoReturningDialect()
        self.executed: list[tuple[str, object]] = []

    class _Cursor:
        rowcount = 1
        lastrowid = 77

    def execute(self, sql, params=None):  # noqa: ANN001,ANN201
        self.executed.append((sql, params))
        return self._Cursor()

    def fetchone(self, sql, params=None):  # noqa: ANN001,ANN201
        return None

    def fetchall(self, sql, params=None):  # noqa: ANN001,ANN201
        return []

    def transaction(self):  # pragma: no cover
        raise NotImplementedError


class RepositorySQLiteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.db = Database(self.conn, SQLiteDialect())
        apply_schema(self.db, UserRow)
        self.repo = Repository[UserRow](self.db, UserRow)

    def tearDown(self) -> None:
        self.conn.close()

    def _seed_users(self) -> list[UserRow]:
        users = [
            UserRow(email="alice@example.com", age=25),
            UserRow(email="bob@example.com", age=30),
            UserRow(email="charlie@sample.com", age=35),
            UserRow(email="david@example.com", age=None),
        ]
        with self.db.transaction():
            for user in users:
                self.repo.insert(user)
        return users

    def _relation_repositories(
        self,
    ) -> tuple[sqlite3.Connection, Repository[AuthorRow], Repository[PostRow]]:
        conn = sqlite3.connect(":memory:")
        db = Database(conn, SQLiteDialect())
        apply_schema(db, AuthorRow)
        apply_schema(db, PostRow)
        return conn, Repository[AuthorRow](db, AuthorRow), Repository[PostRow](db, PostRow)

    def test_unified_repository_reuses_cached_repositories(self) -> None:
        unified = UnifiedRepository(self.db)

        user_repo_1 = unified.repo(UserRow)
        user_repo_2 = unified.repo(UserRow)
        author_repo = unified.repo(AuthorRow)

        self.assertIs(user_repo_1, user_repo_2)
        self.assertIsNot(user_repo_1, author_repo)

    def test_unified_repository_crud_and_relations(self) -> None:
        unified = UnifiedRepository(self.db)
        inserted = unified.insert(UserRow, UserRow(email="hub@example.com", age=20))
        self.assertIsNotNone(inserted.id)
        self.assertEqual(unified.count(UserRow), 1)
        self.assertTrue(unified.exists(UserRow, where=C.eq("email", "hub@example.com")))

        loaded = unified.get(UserRow, inserted.id)
        self.assertIsNotNone(loaded)
        if loaded is None:
            self.fail("Expected inserted row to exist.")

        loaded.age = 21
        self.assertEqual(unified.update(UserRow, loaded), 1)
        self.assertEqual(unified.delete(UserRow, loaded), 1)
        self.assertEqual(unified.count(UserRow), 0)

        conn = sqlite3.connect(":memory:")
        self.addCleanup(conn.close)
        db = Database(conn, SQLiteDialect())
        apply_schema(db, AuthorRow)
        apply_schema(db, PostRow)
        relation_unified = UnifiedRepository(db)

        author = relation_unified.create(
            AuthorRow,
            AuthorRow(name="alice"),
            relations={"posts": [PostRow(title="p1"), PostRow(title="p2")]},
        )
        self.assertIsNotNone(author.id)
        if author.id is None:
            self.fail("Expected author to get auto PK.")

        author_with_posts = relation_unified.get_related(
            AuthorRow,
            author.id,
            include=["posts"],
        )
        self.assertIsNotNone(author_with_posts)
        if author_with_posts is None:
            self.fail("Expected related result for existing author.")
        self.assertEqual(len(author_with_posts.relations["posts"]), 2)

        posts_with_author = relation_unified.list_related(
            PostRow,
            include=["author"],
            order_by=[OrderBy("id")],
        )
        self.assertEqual(len(posts_with_author), 2)
        self.assertTrue(all(item.relations["author"] is not None for item in posts_with_author))

    def test_unified_repository_accepts_object_only_for_mutations(self) -> None:
        unified = UnifiedRepository(self.db)
        inserted = unified.insert(UserRow(email="object@example.com", age=20))
        self.assertIsNotNone(inserted.id)

        loaded = unified.get(UserRow, inserted.id)
        self.assertIsNotNone(loaded)
        if loaded is None:
            self.fail("Expected inserted row to exist.")

        loaded.age = 21
        self.assertEqual(unified.update(loaded), 1)
        self.assertEqual(unified.delete(loaded), 1)

        inserted_many = unified.insert_many(
            [
                UserRow(email="m1@example.com"),
                UserRow(email="m2@example.com"),
            ]
        )
        self.assertEqual(len(inserted_many), 2)

    def test_unified_repository_auto_schema_modes(self) -> None:
        conn = sqlite3.connect(":memory:")
        self.addCleanup(conn.close)
        db = Database(conn, SQLiteDialect())
        unified = UnifiedRepository(db, auto_schema=True)
        unified.insert(AutoSchemaUserV1, AutoSchemaUserV1(email="a@example.com"))

        # Additive model change should auto-sync.
        unified_v2 = UnifiedRepository(db, auto_schema=True)
        unified_v2.insert(
            AutoSchemaUserV2,
            AutoSchemaUserV2(email="b@example.com", age=20),
        )
        self.assertEqual(unified_v2.count(AutoSchemaUserV2), 2)

        with self.assertRaises(ValueError):
            UnifiedRepository(
                db,
                auto_schema=True,
                schema_conflict="raise",
            ).count(AutoSchemaUserIncompatible)

        recreated = UnifiedRepository(
            db,
            auto_schema=True,
            schema_conflict="recreate",
        )
        self.assertEqual(recreated.count(AutoSchemaUserIncompatible), 0)
        recreated_row = recreated.insert(
            AutoSchemaUserIncompatible,
            AutoSchemaUserIncompatible(email=123),
        )
        self.assertIsNotNone(recreated_row.id)
        if recreated_row.id is None:
            self.fail("Expected inserted row id after recreate.")
        recreated_loaded = recreated.get(AutoSchemaUserIncompatible, recreated_row.id)
        self.assertIsNotNone(recreated_loaded)
        if recreated_loaded is None:
            self.fail("Expected recreated schema row to be readable.")
        self.assertEqual(recreated_loaded.email, 123)

    def test_unified_repository_auto_schema_relations_create_child_tables(self) -> None:
        conn = sqlite3.connect(":memory:")
        self.addCleanup(conn.close)
        db = Database(conn, SQLiteDialect())
        unified = UnifiedRepository(db, auto_schema=True)

        author = unified.create(
            AuthorRow,
            AuthorRow(name="auto"),
            relations={"posts": [PostRow(title="p1"), PostRow(title="p2")]},
        )
        self.assertIsNotNone(author.id)
        self.assertEqual(unified.count(PostRow), 2)

    def test_unified_repository_require_registration(self) -> None:
        conn = sqlite3.connect(":memory:")
        self.addCleanup(conn.close)
        db = Database(conn, SQLiteDialect())
        unified = UnifiedRepository(
            db,
            auto_schema=True,
            require_registration=True,
        )

        with self.assertRaisesRegex(ValueError, "not registered"):
            unified.insert(UserRow, UserRow(email="u@example.com"))

        unified.register(UserRow)
        inserted = unified.insert(UserRow, UserRow(email="u@example.com"))
        self.assertIsNotNone(inserted.id)
        self.assertEqual(unified.count(UserRow), 1)

        unified_no_ensure = UnifiedRepository(
            db,
            auto_schema=True,
            require_registration=True,
        )
        unified_no_ensure.register(UserRow, ensure=False)
        inserted2 = unified_no_ensure.insert(UserRow, UserRow(email="u2@example.com"))
        self.assertIsNotNone(inserted2.id)

    def test_unified_repository_auto_registers_on_first_action(self) -> None:
        conn = sqlite3.connect(":memory:")
        self.addCleanup(conn.close)
        db = Database(conn, SQLiteDialect())
        unified = UnifiedRepository(db, auto_schema=True)
        self.assertFalse(unified.repo(UserRow).is_registered())
        inserted = unified.insert(UserRow, UserRow(email="u@example.com"))
        self.assertIsNotNone(inserted.id)
        self.assertEqual(unified.count(UserRow), 1)
        self.assertTrue(unified.repo(UserRow).is_registered())

    def test_unified_repository_relations_require_registered_child_model(self) -> None:
        conn = sqlite3.connect(":memory:")
        self.addCleanup(conn.close)
        db = Database(conn, SQLiteDialect())
        unified = UnifiedRepository(
            db,
            auto_schema=True,
            require_registration=True,
        )
        unified.register(AuthorRow)

        with self.assertRaisesRegex(ValueError, "not registered"):
            unified.create(
                AuthorRow,
                AuthorRow(name="alice"),
                relations={"posts": [PostRow(title="p1")]},
            )
        self.assertEqual(unified.count(AuthorRow), 0)

        unified.register(PostRow)
        author = unified.create(
            AuthorRow,
            AuthorRow(name="alice"),
            relations={"posts": [PostRow(title="p1"), PostRow(title="p2")]},
        )
        self.assertIsNotNone(author.id)
        self.assertEqual(unified.count(PostRow), 2)

    def test_insert_assigns_auto_pk(self) -> None:
        user = UserRow(email="new@example.com", age=20)
        inserted = self.repo.insert(user)
        self.assertIsNotNone(inserted.id)
        self.assertEqual(inserted.email, "new@example.com")

    def test_repository_auto_schema_creates_and_updates_schema(self) -> None:
        conn = sqlite3.connect(":memory:")
        self.addCleanup(conn.close)
        db = Database(conn, SQLiteDialect())

        repo_v1 = Repository(db, AutoSchemaUserV1, auto_schema=True)
        repo_v1.insert(AutoSchemaUserV1(email="a@example.com"))

        # Same model config should be a no-op.
        Repository(db, AutoSchemaUserV1, auto_schema=True)

        # Additive change: new nullable column should be added automatically.
        repo_v2 = Repository(db, AutoSchemaUserV2, auto_schema=True)
        inserted = repo_v2.insert(AutoSchemaUserV2(email="b@example.com", age=20))
        self.assertIsNotNone(inserted.id)
        self.assertEqual(repo_v2.count(), 2)

    def test_repository_auto_schema_conflict_modes(self) -> None:
        conn = sqlite3.connect(":memory:")
        self.addCleanup(conn.close)
        db = Database(conn, SQLiteDialect())

        Repository(db, AutoSchemaUserV1, auto_schema=True).insert(
            AutoSchemaUserV1(email="a@example.com")
        )

        with self.assertRaises(ValueError):
            Repository(
                db,
                AutoSchemaUserIncompatible,
                auto_schema=True,
                schema_conflict="raise",
            ).count()

        recreated = Repository(
            db,
            AutoSchemaUserIncompatible,
            auto_schema=True,
            schema_conflict="recreate",
        )
        self.assertEqual(recreated.count(), 0)

    def test_repository_require_registration(self) -> None:
        conn = sqlite3.connect(":memory:")
        self.addCleanup(conn.close)
        db = Database(conn, SQLiteDialect())
        repo = Repository(
            db,
            UserRow,
            auto_schema=True,
            require_registration=True,
        )

        with self.assertRaisesRegex(ValueError, "not registered"):
            repo.insert(UserRow(email="x@example.com"))

        repo.register()
        inserted = repo.insert(UserRow(email="x@example.com"))
        self.assertIsNotNone(inserted.id)
        self.assertEqual(repo.count(), 1)

        repo2 = Repository(
            db,
            UserRow,
            auto_schema=True,
            require_registration=True,
        )
        repo2.register(ensure=False)
        inserted2 = repo2.insert(UserRow(email="x2@example.com"))
        self.assertIsNotNone(inserted2.id)

    def test_repository_auto_registers_on_first_action(self) -> None:
        conn = sqlite3.connect(":memory:")
        self.addCleanup(conn.close)
        db = Database(conn, SQLiteDialect())
        repo = Repository(db, UserRow, auto_schema=True)
        self.assertFalse(repo.is_registered())
        inserted = repo.insert(UserRow(email="x@example.com"))
        self.assertIsNotNone(inserted.id)
        self.assertEqual(repo.count(), 1)
        self.assertTrue(repo.is_registered())

    def test_insert_uses_lastrowid_when_returning_is_not_supported(self) -> None:
        fake_db = _NoReturningDb()
        repo = Repository[UserRow](fake_db, UserRow)
        user = UserRow(email="fallback@example.com", age=10)
        repo.insert(user)
        self.assertEqual(user.id, 77)
        self.assertTrue(fake_db.executed)

    def test_get_and_get_missing(self) -> None:
        inserted = self.repo.insert(UserRow(email="lookup@example.com", age=50))
        found = self.repo.get(inserted.id)
        missing = self.repo.get(9999)

        self.assertEqual(found.email, "lookup@example.com")
        self.assertIsNone(missing)

    def test_update_and_delete(self) -> None:
        inserted = self.repo.insert(UserRow(email="mutate@example.com", age=10))
        inserted.age = 11
        update_count = self.repo.update(inserted)
        updated = self.repo.get(inserted.id)

        delete_count = self.repo.delete(inserted)
        missing = self.repo.get(inserted.id)

        self.assertEqual(update_count, 1)
        self.assertEqual(updated.age, 11)
        self.assertEqual(delete_count, 1)
        self.assertIsNone(missing)

    def test_update_and_delete_require_pk(self) -> None:
        missing_pk = UserRow(email="x", age=1)
        with self.assertRaises(ValueError):
            self.repo.update(missing_pk)
        with self.assertRaises(ValueError):
            self.repo.delete(missing_pk)

    def test_list_with_condition_order_limit_offset(self) -> None:
        self._seed_users()
        rows = self.repo.list(
            where=[C.like("email", "%@example.com"), C.is_not_null("age")],
            order_by=[OrderBy("age", desc=True)],
            limit=1,
            offset=1,
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].email, "alice@example.com")

    def test_list_with_in_and_is_null(self) -> None:
        users = self._seed_users()
        selected = self.repo.list(
            where=C.in_("id", [users[0].id, users[1].id]),
            order_by=[OrderBy("id")],
        )
        null_age = self.repo.list(where=C.is_null("age"))

        self.assertEqual([row.email for row in selected], ["alice@example.com", "bob@example.com"])
        self.assertEqual(len(null_age), 1)
        self.assertEqual(null_age[0].email, "david@example.com")

    def test_list_with_grouped_conditions(self) -> None:
        self._seed_users()
        rows = self.repo.list(
            where=C.or_(
                C.eq("email", "alice@example.com"),
                C.eq("email", "bob@example.com"),
            ),
            order_by=[OrderBy("id")],
        )
        self.assertEqual([row.email for row in rows], ["alice@example.com", "bob@example.com"])

    def test_list_rejects_invalid_limit_offset(self) -> None:
        with self.assertRaises(ValueError):
            self.repo.list(limit=0)
        with self.assertRaises(ValueError):
            self.repo.list(offset=-1)

    def test_count_and_exists(self) -> None:
        self._seed_users()
        self.assertEqual(self.repo.count(), 4)
        self.assertEqual(self.repo.count(where=C.like("email", "%@example.com")), 3)
        self.assertTrue(self.repo.exists(where=C.eq("email", "alice@example.com")))
        self.assertFalse(self.repo.exists(where=C.eq("email", "nobody@example.com")))

    def test_enum_and_json_codec_serialize_and_deserialize(self) -> None:
        conn = sqlite3.connect(":memory:")
        self.addCleanup(conn.close)
        db = Database(conn, SQLiteDialect())
        apply_schema(db, TicketRow)
        repo = Repository[TicketRow](db, TicketRow)

        inserted = repo.insert(
            TicketRow(
                status=TicketStatus.CLOSED,
                payload={"priority": 2, "tags": ["bug"]},
                tags=["bug", "urgent"],
            )
        )
        self.assertIsNotNone(inserted.id)

        loaded = repo.get(inserted.id)
        self.assertEqual(loaded.status, TicketStatus.CLOSED)
        self.assertEqual(loaded.payload, {"priority": 2, "tags": ["bug"]})
        self.assertEqual(loaded.tags, ["bug", "urgent"])

        filtered = repo.list(where=C.eq("status", TicketStatus.CLOSED))
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0].id, inserted.id)

        updated = repo.update_where(
            {"payload": {"priority": 1}},
            where=C.eq("status", TicketStatus.CLOSED),
        )
        self.assertEqual(updated, 1)

        refreshed = repo.get(inserted.id)
        self.assertEqual(refreshed.payload, {"priority": 1})
        self.assertEqual(refreshed.tags, ["bug", "urgent"])

    def test_insert_many(self) -> None:
        inserted = self.repo.insert_many(
            [
                UserRow(email="m1@example.com", age=10),
                UserRow(email="m2@example.com", age=20),
            ]
        )
        self.assertEqual(len(inserted), 2)
        self.assertEqual(self.repo.count(), 2)
        self.assertTrue(all(item.id is not None for item in inserted))

    def test_update_where(self) -> None:
        self._seed_users()
        updated = self.repo.update_where(
            {"age": 40},
            where=C.or_(
                C.eq("email", "alice@example.com"),
                C.eq("email", "bob@example.com"),
            ),
        )
        rows = self.repo.list(
            where=C.in_("email", ["alice@example.com", "bob@example.com"]),
            order_by=[OrderBy("id")],
        )
        self.assertEqual(updated, 2)
        self.assertEqual([row.age for row in rows], [40, 40])

    def test_update_where_validations(self) -> None:
        self._seed_users()
        with self.assertRaises(ValueError):
            self.repo.update_where({}, where=C.eq("id", 1))
        with self.assertRaises(ValueError):
            self.repo.update_where({"age": 20}, where=None)
        with self.assertRaises(ValueError):
            self.repo.update_where({"unknown": 1}, where=C.eq("id", 1))
        with self.assertRaises(ValueError):
            self.repo.update_where({"id": 123}, where=C.eq("id", 1))

    def test_delete_where(self) -> None:
        self._seed_users()
        deleted = self.repo.delete_where(
            where=C.like("email", "%@example.com"),
        )
        self.assertEqual(deleted, 3)
        self.assertEqual(self.repo.count(), 1)

    def test_delete_where_requires_where(self) -> None:
        with self.assertRaises(ValueError):
            self.repo.delete_where(where=None)

    def test_get_or_create(self) -> None:
        first, created_first = self.repo.get_or_create(
            lookup={"email": "new@example.com"},
            defaults={"age": 33},
        )
        second, created_second = self.repo.get_or_create(
            lookup={"email": "new@example.com"},
            defaults={"age": 99},
        )
        self.assertTrue(created_first)
        self.assertFalse(created_second)
        self.assertEqual(first.email, "new@example.com")
        self.assertEqual(first.age, 33)
        self.assertEqual(second.id, first.id)

    def test_get_or_create_insert_first_conflict_path(self) -> None:
        existing = self.repo.insert(UserRow(email="conflict@example.com", age=20))

        insert_call_count = 0
        original_insert = self.repo.insert

        def tracked_insert(obj: UserRow) -> UserRow:
            nonlocal insert_call_count
            insert_call_count += 1
            return original_insert(obj)

        self.repo.insert = tracked_insert  # type: ignore[method-assign]
        row, created = self.repo.get_or_create(
            lookup={"email": "conflict@example.com"},
            defaults={"age": 99},
        )

        self.assertFalse(created)
        self.assertEqual(insert_call_count, 1)
        self.assertEqual(row.id, existing.id)

    def test_get_or_create_requires_lookup(self) -> None:
        with self.assertRaises(ValueError):
            self.repo.get_or_create(lookup={})

    def test_get_or_create_requires_unique_lookup_constraint(self) -> None:
        conn = sqlite3.connect(":memory:")
        db = Database(conn, SQLiteDialect())
        apply_schema(db, NonUniqueLookupRow)
        repo = Repository[NonUniqueLookupRow](db, NonUniqueLookupRow)

        with self.assertRaises(ValueError):
            repo.get_or_create(lookup={"email": "x@example.com"})
        conn.close()

    def test_get_or_create_reraises_non_integrity_errors(self) -> None:
        def failing_insert(obj: UserRow) -> UserRow:  # noqa: ARG001
            raise RuntimeError("boom")

        self.repo.insert = failing_insert  # type: ignore[method-assign]
        with self.assertRaises(RuntimeError):
            self.repo.get_or_create(lookup={"email": "boom@example.com"})

    def test_get_or_create_reraises_integrity_when_row_still_missing(self) -> None:
        def failing_insert(obj: UserRow) -> UserRow:  # noqa: ARG001
            raise sqlite3.IntegrityError("forced conflict")

        self.repo.insert = failing_insert  # type: ignore[method-assign]
        self.repo.list = lambda *args, **kwargs: []  # type: ignore[method-assign]
        with self.assertRaises(sqlite3.IntegrityError):
            self.repo.get_or_create(lookup={"email": "missing@example.com"})

    def test_repository_requires_dataclass_and_single_pk(self) -> None:
        with self.assertRaises(TypeError):
            Repository(self.db, PlainModel)
        with self.assertRaises(ValueError):
            Repository(self.db, MultiPkRow)

    def test_insert_with_only_auto_pk_model_uses_default_values(self) -> None:
        conn = sqlite3.connect(":memory:")
        db = Database(conn, SQLiteDialect())
        apply_schema(db, OnlyPkRow)
        repo = Repository[OnlyPkRow](db, OnlyPkRow)

        obj = repo.insert(OnlyPkRow())
        self.assertIsNotNone(obj.id)
        conn.close()

    def test_update_with_only_auto_pk_model_raises_clear_error(self) -> None:
        conn = sqlite3.connect(":memory:")
        db = Database(conn, SQLiteDialect())
        apply_schema(db, OnlyPkRow)
        repo = Repository[OnlyPkRow](db, OnlyPkRow)

        db.execute('INSERT INTO "onlypkrow" DEFAULT VALUES;')
        obj = repo.get(1)
        self.assertIsNotNone(obj)
        with self.assertRaises(ValueError):
            repo.update(obj)
        conn.close()

    def test_create_with_has_many_relations(self) -> None:
        conn = sqlite3.connect(":memory:")
        db = Database(conn, SQLiteDialect())
        apply_schema(db, AuthorRow)
        apply_schema(db, PostRow)
        author_repo = Repository[AuthorRow](db, AuthorRow)
        post_repo = Repository[PostRow](db, PostRow)

        author = AuthorRow(name="Alice")
        posts = [PostRow(title="Post A"), PostRow(title="Post B")]
        author_repo.create(author, relations={"posts": posts})

        self.assertIsNotNone(author.id)
        self.assertEqual(post_repo.count(), 2)
        loaded_posts = post_repo.list(order_by=[OrderBy("id")])
        self.assertTrue(all(post.author_id == author.id for post in loaded_posts))
        conn.close()

    def test_create_with_belongs_to_relation(self) -> None:
        conn = sqlite3.connect(":memory:")
        db = Database(conn, SQLiteDialect())
        apply_schema(db, AuthorRow)
        apply_schema(db, PostRow)
        author_repo = Repository[AuthorRow](db, AuthorRow)
        post_repo = Repository[PostRow](db, PostRow)

        post = PostRow(title="Nested")
        post_repo.create(post, relations={"author": AuthorRow(name="Nested Author")})

        self.assertIsNotNone(post.id)
        self.assertIsNotNone(post.author_id)
        self.assertEqual(author_repo.count(), 1)
        conn.close()

    def test_get_related_and_list_related(self) -> None:
        conn = sqlite3.connect(":memory:")
        db = Database(conn, SQLiteDialect())
        apply_schema(db, AuthorRow)
        apply_schema(db, PostRow)
        author_repo = Repository[AuthorRow](db, AuthorRow)
        post_repo = Repository[PostRow](db, PostRow)

        author = author_repo.create(
            AuthorRow(name="Reader"),
            relations={"posts": [PostRow(title="T1"), PostRow(title="T2")]},
        )

        author_with_posts = author_repo.get_related(author.id, include=["posts"])
        self.assertIsNotNone(author_with_posts)
        self.assertEqual(len(author_with_posts.relations["posts"]), 2)

        posts_with_author = post_repo.list_related(include=["author"], order_by=[OrderBy("id")])
        self.assertEqual(len(posts_with_author), 2)
        self.assertTrue(all(item.relations["author"] is not None for item in posts_with_author))
        self.assertTrue(all(item.relations["author"].name == "Reader" for item in posts_with_author))
        conn.close()

    def test_get_related_returns_none_for_missing_row(self) -> None:
        conn, author_repo, _ = self._relation_repositories()
        self.assertIsNone(author_repo.get_related(9999, include=["posts"]))
        conn.close()

    def test_create_with_unknown_relation_raises(self) -> None:
        conn, author_repo, post_repo = self._relation_repositories()
        with self.assertRaises(ValueError):
            author_repo.create(AuthorRow(name="A"), relations={"missing_relation": []})

        self.assertEqual(author_repo.count(), 0)
        self.assertEqual(post_repo.count(), 0)
        conn.close()

    def test_create_has_many_requires_sequence_and_rolls_back(self) -> None:
        conn, author_repo, post_repo = self._relation_repositories()
        with self.assertRaises(TypeError):
            author_repo.create(
                AuthorRow(name="A"),
                relations={"posts": PostRow(title="must-be-sequence")},  # type: ignore[arg-type]
            )

        self.assertEqual(author_repo.count(), 0)
        self.assertEqual(post_repo.count(), 0)
        conn.close()

    def test_create_has_many_rejects_invalid_child_type_and_rolls_back(self) -> None:
        conn, author_repo, post_repo = self._relation_repositories()
        with self.assertRaises(TypeError):
            author_repo.create(
                AuthorRow(name="A"),
                relations={
                    "posts": [
                        PostRow(title="valid"),
                        AuthorRow(name="invalid-child"),  # type: ignore[list-item]
                    ]
                },
            )

        self.assertEqual(author_repo.count(), 0)
        self.assertEqual(post_repo.count(), 0)
        conn.close()

    def test_create_belongs_to_requires_model_type(self) -> None:
        conn, author_repo, post_repo = self._relation_repositories()
        with self.assertRaises(TypeError):
            post_repo.create(PostRow(title="invalid"), relations={"author": "not-a-model"})  # type: ignore[arg-type]

        self.assertEqual(author_repo.count(), 0)
        self.assertEqual(post_repo.count(), 0)
        conn.close()

    def test_get_related_for_belongs_to_returns_none_when_fk_is_null(self) -> None:
        conn, _, post_repo = self._relation_repositories()
        post = post_repo.insert(PostRow(title="Orphan", author_id=None))
        result = post_repo.get_related(post.id, include=["author"])

        self.assertIsNotNone(result)
        self.assertIsNone(result.relations["author"])
        conn.close()

    def test_list_related_validates_include_items(self) -> None:
        conn, _, post_repo = self._relation_repositories()
        with self.assertRaises(TypeError):
            post_repo.list_related(include=["author", ""])
        with self.assertRaises(TypeError):
            post_repo.list_related(include=[123])  # type: ignore[list-item]
        conn.close()

    def test_list_related_validates_unknown_relation_name(self) -> None:
        conn, _, post_repo = self._relation_repositories()
        with self.assertRaises(ValueError):
            post_repo.list_related(include=["missing"])
        conn.close()

    def test_list_related_deduplicates_include_names(self) -> None:
        conn, _, post_repo = self._relation_repositories()
        post_repo.create(PostRow(title="One"), relations={"author": AuthorRow(name="Single")})

        rows = post_repo.list_related(include=["author", "author"])
        self.assertEqual(len(rows), 1)
        self.assertEqual(list(rows[0].relations.keys()), ["author"])
        conn.close()

    def test_relations_can_be_inferred_from_fk_metadata(self) -> None:
        conn = sqlite3.connect(":memory:")
        db = Database(conn, SQLiteDialect())
        apply_schema(db, AutoAuthor)
        apply_schema(db, AutoPost)
        author_repo = Repository[AutoAuthor](db, AutoAuthor)
        post_repo = Repository[AutoPost](db, AutoPost)

        author = author_repo.create(
            AutoAuthor(name="Inferred"),
            relations={"posts": [AutoPost(title="T1"), AutoPost(title="T2")]},
        )
        post_with_author = post_repo.create(
            AutoPost(title="Nested"),
            relations={"author": AutoAuthor(name="Nested Author")},
        )

        self.assertIsNotNone(author.id)
        self.assertIsNotNone(post_with_author.author_id)
        self.assertEqual(post_repo.count(), 3)
        self.assertEqual(author_repo.count(), 2)

        author_with_posts = author_repo.get_related(author.id, include=["posts"])
        self.assertIsNotNone(author_with_posts)
        self.assertEqual(len(author_with_posts.relations["posts"]), 2)

        rows = post_repo.list_related(include=["author"], order_by=[OrderBy("id")])
        self.assertEqual(len(rows), 3)
        self.assertTrue(all(item.relations["author"] is not None for item in rows))
        conn.close()


if __name__ == "__main__":
    unittest.main()
