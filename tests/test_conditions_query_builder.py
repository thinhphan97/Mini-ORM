from __future__ import annotations

import random
import re
import sqlite3
import unittest
from collections.abc import Sequence
from typing import Any

from mini_orm.core.conditions import C, Condition, ConditionGroup, NotCondition, OrderBy
from mini_orm.core.query_builder import (
    append_limit_offset,
    compile_order_by,
    compile_where,
)
from mini_orm.ports.db_api.dialects import PostgresDialect, SQLiteDialect


class ConditionsTests(unittest.TestCase):
    def test_condition_factory_methods(self) -> None:
        samples = [
            ("eq", C.eq("age", 18), "=", False),
            ("ne", C.ne("age", 18), "<>", False),
            ("lt", C.lt("age", 18), "<", False),
            ("le", C.le("age", 18), "<=", False),
            ("gt", C.gt("age", 18), ">", False),
            ("ge", C.ge("age", 18), ">=", False),
            ("like", C.like("email", "%@x.com"), "LIKE", False),
            ("is_null", C.is_null("deleted_at"), "IS NULL", True),
            ("is_not_null", C.is_not_null("email"), "IS NOT NULL", True),
            ("in_", C.in_("id", [1, 2]), "IN", False),
        ]

        for name, condition, op, unary in samples:
            with self.subTest(name=name):
                self.assertIsInstance(condition, Condition)
                self.assertEqual(condition.op, op)
                self.assertEqual(condition.is_unary, unary)

    def test_group_factory_methods(self) -> None:
        group_and = C.and_(C.eq("age", 18), C.eq("email", "a@x.com"))
        group_or = C.or_(C.eq("age", 18), C.eq("age", 21))
        negated = C.not_(C.eq("deleted", True))

        self.assertIsInstance(group_and, ConditionGroup)
        self.assertEqual(group_and.operator, "AND")
        self.assertEqual(len(group_and.items), 2)

        self.assertIsInstance(group_or, ConditionGroup)
        self.assertEqual(group_or.operator, "OR")
        self.assertEqual(len(group_or.items), 2)

        self.assertIsInstance(negated, NotCondition)

    def test_group_factory_validation(self) -> None:
        with self.assertRaises(ValueError):
            C.and_()
        with self.assertRaises(TypeError):
            C.or_(123)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            C.not_(123)  # type: ignore[arg-type]

    def test_order_by_defaults(self) -> None:
        order = OrderBy("id")
        self.assertEqual(order.col, "id")
        self.assertFalse(order.desc)


class QueryBuilderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.named = SQLiteDialect()
        self.positional = PostgresDialect()
        self._rows = [
            {
                "id": 1,
                "age": 18,
                "role": "admin",
                "email": "admin@example.com",
                "active": 1,
                "deleted_at": None,
            },
            {
                "id": 2,
                "age": 22,
                "role": "owner",
                "email": "owner@example.com",
                "active": 1,
                "deleted_at": None,
            },
            {
                "id": 3,
                "age": 30,
                "role": "user",
                "email": "alice@sample.com",
                "active": 1,
                "deleted_at": None,
            },
            {
                "id": 4,
                "age": 41,
                "role": "user",
                "email": "bob@example.com",
                "active": 0,
                "deleted_at": "2025-01-01",
            },
            {
                "id": 5,
                "age": None,
                "role": "auditor",
                "email": "auditor@example.com",
                "active": 1,
                "deleted_at": None,
            },
        ]

    def test_compile_where_with_none_and_empty_list(self) -> None:
        none_fragment = compile_where(None, self.named)
        empty_fragment = compile_where([], self.named)
        self.assertEqual(none_fragment.sql, "")
        self.assertIsNone(none_fragment.params)
        self.assertEqual(empty_fragment.sql, "")
        self.assertIsNone(empty_fragment.params)

    def test_compile_where_binary_named(self) -> None:
        fragment = compile_where(C.eq("age", 30), self.named)
        self.assertEqual(fragment.sql, ' WHERE "age" = :age_1')
        self.assertEqual(fragment.params, {"age_1": 30})

    def test_compile_where_unary_and_binary_named(self) -> None:
        fragment = compile_where([C.eq("age", 30), C.is_not_null("email")], self.named)
        self.assertEqual(
            fragment.sql,
            ' WHERE "age" = :age_1 AND "email" IS NOT NULL',
        )
        self.assertEqual(fragment.params, {"age_1": 30})

    def test_compile_where_in_named_and_empty(self) -> None:
        filled = compile_where(C.in_("id", [1, 2]), self.named)
        empty = compile_where(C.in_("id", []), self.named)

        self.assertEqual(filled.sql, ' WHERE "id" IN (:id_1, :id_2)')
        self.assertEqual(filled.params, {"id_1": 1, "id_2": 2})
        self.assertEqual(empty.sql, " WHERE 1=0")
        self.assertEqual(empty.params, {})

    def test_compile_where_positional(self) -> None:
        binary = compile_where(C.eq("age", 30), self.positional)
        in_fragment = compile_where(C.in_("id", [1, 2]), self.positional)
        self.assertEqual(binary.sql, ' WHERE "age" = %s')
        self.assertEqual(binary.params, [30])
        self.assertEqual(in_fragment.sql, ' WHERE "id" IN (%s, %s)')
        self.assertEqual(in_fragment.params, [1, 2])

    def test_compile_where_grouped_or_not(self) -> None:
        grouped = compile_where(
            C.or_(C.eq("email", "a@example.com"), C.eq("email", "b@example.com")),
            self.named,
        )
        negated = compile_where(C.not_(C.eq("deleted", True)), self.named)

        self.assertEqual(
            grouped.sql,
            ' WHERE (("email" = :email_1) OR ("email" = :email_2))',
        )
        self.assertEqual(
            grouped.params,
            {"email_1": "a@example.com", "email_2": "b@example.com"},
        )
        self.assertEqual(negated.sql, ' WHERE NOT ("deleted" = :deleted_1)')
        self.assertEqual(negated.params, {"deleted_1": True})

    def test_compile_where_sequence_with_group(self) -> None:
        fragment = compile_where(
            [
                C.eq("active", True),
                C.or_(C.eq("role", "admin"), C.eq("role", "owner")),
            ],
            self.named,
        )
        self.assertEqual(
            fragment.sql,
            ' WHERE "active" = :active_1 AND (("role" = :role_2) OR ("role" = :role_3))',
        )
        self.assertEqual(
            fragment.params,
            {"active_1": True, "role_2": "admin", "role_3": "owner"},
        )

    def test_compile_order_by(self) -> None:
        sql = compile_order_by([OrderBy("age", desc=True), OrderBy("id")], self.named)
        self.assertEqual(sql, ' ORDER BY "age" DESC, "id" ASC')
        self.assertEqual(compile_order_by(None, self.named), "")

    def test_append_limit_offset_named(self) -> None:
        sql, params = append_limit_offset(
            'SELECT * FROM "user"',
            {"age_1": 30},
            limit=10,
            offset=5,
            dialect=self.named,
        )
        self.assertEqual(sql, 'SELECT * FROM "user" LIMIT :__limit OFFSET :__offset')
        self.assertEqual(params, {"age_1": 30, "__limit": 10, "__offset": 5})

    def test_append_limit_offset_positional(self) -> None:
        sql, params = append_limit_offset(
            'SELECT * FROM "user"',
            [30],
            limit=10,
            offset=5,
            dialect=self.positional,
        )
        self.assertEqual(sql, 'SELECT * FROM "user" LIMIT %s OFFSET %s')
        self.assertEqual(params, [30, 10, 5])

    def test_append_limit_offset_without_params(self) -> None:
        sql, params = append_limit_offset(
            'SELECT * FROM "user"',
            None,
            limit=None,
            offset=None,
            dialect=self.named,
        )
        self.assertEqual(sql, 'SELECT * FROM "user"')
        self.assertIsNone(params)

    def test_append_limit_offset_rejects_invalid_values(self) -> None:
        with self.assertRaises(ValueError):
            append_limit_offset(
                'SELECT * FROM "user"',
                None,
                limit=0,
                offset=None,
                dialect=self.named,
            )
        with self.assertRaises(ValueError):
            append_limit_offset(
                'SELECT * FROM "user"',
                None,
                limit=1,
                offset=-1,
                dialect=self.named,
            )

    def test_property_random_where_compilation_named_vs_positional(self) -> None:
        rng = random.Random(20260223)
        for _ in range(250):
            expression = self._random_expression(rng, depth=3)
            if rng.random() < 0.35:
                where_input: Any = [expression, self._random_expression(rng, depth=2)]
            else:
                where_input = expression

            named_fragment = compile_where(where_input, self.named)
            positional_fragment = compile_where(where_input, self.positional)

            self.assertTrue(named_fragment.sql.startswith(" WHERE "))
            self.assertTrue(positional_fragment.sql.startswith(" WHERE "))

            canonical_named = self._canonical_sql(named_fragment.sql)
            canonical_positional = self._canonical_sql(positional_fragment.sql)
            self.assertEqual(canonical_named, canonical_positional)

            named_placeholder_count = len(re.findall(r":[A-Za-z_]\w*", named_fragment.sql))
            positional_placeholder_count = positional_fragment.sql.count("%s")

            if isinstance(named_fragment.params, dict):
                self.assertEqual(named_placeholder_count, len(named_fragment.params))
            else:
                self.assertEqual(named_placeholder_count, 0)

            if isinstance(positional_fragment.params, list):
                self.assertEqual(positional_placeholder_count, len(positional_fragment.params))
            else:
                self.assertEqual(positional_placeholder_count, 0)

            named_result = self._run_named_query(named_fragment.sql, named_fragment.params)
            positional_result = self._run_positional_query(
                positional_fragment.sql, positional_fragment.params
            )
            self.assertEqual(named_result, positional_result)

    def test_property_random_append_limit_offset_consistency(self) -> None:
        rng = random.Random(20260224)
        for _ in range(200):
            base_sql = 'SELECT * FROM "user"'
            limit = rng.choice([None, 1, 2, 5, 10])
            offset = rng.choice([None, 0, 1, 3])

            named_params: dict[str, Any] | None = (
                {"age_1": rng.choice([18, 22, 30])} if rng.random() < 0.5 else None
            )
            positional_params: list[Any] | None = (
                [rng.choice([18, 22, 30])] if named_params is not None else None
            )

            named_sql, named_out = append_limit_offset(
                base_sql,
                named_params,
                limit=limit,
                offset=offset,
                dialect=self.named,
            )
            positional_sql, positional_out = append_limit_offset(
                base_sql,
                positional_params,
                limit=limit,
                offset=offset,
                dialect=self.positional,
            )

            expected_named_limit = limit is not None
            expected_named_offset = offset is not None
            self.assertEqual(" LIMIT :__limit" in named_sql, expected_named_limit)
            self.assertEqual(" OFFSET :__offset" in named_sql, expected_named_offset)

            expected_positional_limit = limit is not None
            expected_positional_offset = offset is not None
            self.assertEqual(" LIMIT %s" in positional_sql, expected_positional_limit)
            self.assertEqual(" OFFSET %s" in positional_sql, expected_positional_offset)

            if isinstance(named_out, dict):
                if named_params:
                    self.assertIn("age_1", named_out)
                    self.assertEqual(named_out["age_1"], named_params["age_1"])
                self.assertEqual("__limit" in named_out, expected_named_limit)
                self.assertEqual("__offset" in named_out, expected_named_offset)
            else:
                self.assertIsNone(named_out)

            if isinstance(positional_out, list):
                expected_len = (1 if positional_params else 0) + int(
                    expected_positional_limit
                ) + int(expected_positional_offset)
                self.assertEqual(len(positional_out), expected_len)
                if positional_params:
                    self.assertEqual(positional_out[0], positional_params[0])
                    if expected_positional_limit:
                        self.assertEqual(positional_out[1], limit)
                    if expected_positional_offset:
                        self.assertEqual(positional_out[-1], offset)
            else:
                self.assertIsNone(positional_out)

    def _canonical_sql(self, sql: str) -> str:
        normalized_named = re.sub(r":[A-Za-z_]\w*", "?", sql)
        normalized_positional = normalized_named.replace("%s", "?")
        return re.sub(r"\s+", " ", normalized_positional).strip()

    def _run_named_query(
        self, where_sql: str, params: dict[str, Any] | list[Any] | None
    ) -> list[int]:
        conn = sqlite3.connect(":memory:")
        self._seed_query_builder_db(conn)
        rows = conn.execute(
            'SELECT "id" FROM "items"' + where_sql + ' ORDER BY "id";',
            params or {},
        ).fetchall()
        conn.close()
        return [row[0] for row in rows]

    def _run_positional_query(
        self, where_sql: str, params: dict[str, Any] | list[Any] | None
    ) -> list[int]:
        conn = sqlite3.connect(":memory:")
        self._seed_query_builder_db(conn)
        sqlite_sql = where_sql.replace("%s", "?")
        rows = conn.execute(
            'SELECT "id" FROM "items"' + sqlite_sql + ' ORDER BY "id";',
            params or [],
        ).fetchall()
        conn.close()
        return [row[0] for row in rows]

    def _seed_query_builder_db(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE "items" (
                "id" INTEGER PRIMARY KEY,
                "age" INTEGER NULL,
                "role" TEXT NOT NULL,
                "email" TEXT NOT NULL,
                "active" INTEGER NOT NULL,
                "deleted_at" TEXT NULL
            );
            """
        )
        conn.executemany(
            """
            INSERT INTO "items" ("id", "age", "role", "email", "active", "deleted_at")
            VALUES (:id, :age, :role, :email, :active, :deleted_at);
            """,
            self._rows,
        )

    def _random_expression(
        self, rng: random.Random, *, depth: int
    ) -> Condition | ConditionGroup | NotCondition:
        if depth <= 0 or rng.random() < 0.55:
            return self._random_condition(rng)

        mode = rng.choice(["group_and", "group_or", "not"])
        if mode == "not":
            return C.not_(self._random_expression(rng, depth=depth - 1))

        item_count = rng.randint(2, 3)
        items = [self._random_expression(rng, depth=depth - 1) for _ in range(item_count)]
        return C.and_(items) if mode == "group_and" else C.or_(items)

    def _random_condition(self, rng: random.Random) -> Condition:
        condition_kind = rng.choice(
            [
                "eq",
                "ne",
                "lt",
                "le",
                "gt",
                "ge",
                "like",
                "is_null",
                "is_not_null",
                "in",
            ]
        )

        if condition_kind in {"lt", "le", "gt", "ge"}:
            col = rng.choice(["id", "age", "active"])
            value = rng.choice([0, 1, 2, 3, 4, 5, 10, 18, 22, 30, 41])
            return {
                "lt": C.lt,
                "le": C.le,
                "gt": C.gt,
                "ge": C.ge,
            }[condition_kind](col, value)

        if condition_kind == "like":
            col = rng.choice(["email", "role"])
            if col == "email":
                pattern = rng.choice(["%@example.com", "%@sample.com", "admin%", "%x%"])
            else:
                pattern = rng.choice(["a%", "u%", "%er", "%own%"])
            return C.like(col, pattern)

        if condition_kind in {"is_null", "is_not_null"}:
            col = rng.choice(["age", "deleted_at"])
            return C.is_null(col) if condition_kind == "is_null" else C.is_not_null(col)

        if condition_kind == "in":
            col = rng.choice(["id", "age", "role", "active"])
            values = self._random_in_values(rng, col)
            return C.in_(col, values)

        col = rng.choice(["id", "age", "role", "email", "active"])
        value = self._random_scalar_value(rng, col)
        return {"eq": C.eq, "ne": C.ne}[condition_kind](col, value)

    def _random_in_values(self, rng: random.Random, col: str) -> Sequence[Any]:
        if rng.random() < 0.15:
            return []

        size = rng.randint(1, 4)
        if col in {"id", "age", "active"}:
            pool = [0, 1, 2, 3, 4, 5, 10, 18, 22, 30, 41]
        elif col == "role":
            pool = ["admin", "owner", "user", "auditor", "guest"]
        else:
            pool = ["admin@example.com", "owner@example.com", "alice@sample.com", "x@y.com"]
        return [rng.choice(pool) for _ in range(size)]

    def _random_scalar_value(self, rng: random.Random, col: str) -> Any:
        if col in {"id", "age", "active"}:
            return rng.choice([0, 1, 2, 3, 4, 5, 10, 18, 22, 30, 41])
        if col == "role":
            return rng.choice(["admin", "owner", "user", "auditor", "guest"])
        return rng.choice(
            [
                "admin@example.com",
                "owner@example.com",
                "alice@sample.com",
                "auditor@example.com",
                "nobody@example.com",
            ]
        )


if __name__ == "__main__":
    unittest.main()
