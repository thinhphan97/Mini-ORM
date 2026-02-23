from __future__ import annotations

import unittest

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


if __name__ == "__main__":
    unittest.main()
