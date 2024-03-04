import unittest
import sqlparse

from sqlparse.sql import Token

from sql_to_ast.builder.from_clause_builder import get_from_clause
from sql_to_ast.models import join


class TestFromClauseBuilder(unittest.TestCase):
    def get_tokens(self, sql: str) -> Token:
        return sqlparse.parse(sql)[0].tokens

    def get_from(self, sql: str):
        tokens = self.get_tokens(sql)

        return get_from_clause(tokens)

    def test_get_from(self):
        sql = "FROM table1"
        from_ = self.get_from(sql)

        self.assertEqual(from_.table.name, "table1")
        self.assertEqual(from_.joins, [])

        sql = "FROM table1 AS t"
        from_ = self.get_from(sql)

        self.assertEqual(from_.table.name, "table1")
        self.assertEqual(from_.table.alias, "t")

        sql = "FROM table1 AS t JOIN table2 AS t2 ON t.id = t2.id"
        from_ = self.get_from(sql)

        self.assertEqual(from_.table.name, "table1")
        self.assertEqual(from_.table.alias, "t")
        self.assertEqual(len(from_.joins), 1)
        self.assertEqual(from_.joins[0].table.name, "table2")
        self.assertEqual(from_.joins[0].table.alias, "t2")
        self.assertEqual(from_.joins[0].condition.left.name, "id")
        self.assertEqual(from_.joins[0].condition.left.parent, "t")
        self.assertEqual(from_.joins[0].condition.right.name, "id")
        self.assertEqual(from_.joins[0].condition.right.parent, "t2")
        self.assertEqual(
            from_.joins[0].condition.operator, join.JoinConditionOperator.EQUAL)
        self.assertEqual(from_.joins[0].type, join.JoinType.INNER)

        sql = "FROM table1 AS t JOIN table2 AS t2 ON t.id = t2.id JOIN table3 AS t3 ON t2.id = t3.id"
        from_ = self.get_from(sql)

        self.assertEqual(from_.table.name, "table1")
        self.assertEqual(from_.table.alias, "t")
        self.assertEqual(len(from_.joins), 2)
        self.assertEqual(from_.joins[0].table.name, "table2")
        self.assertEqual(from_.joins[0].table.alias, "t2")
        self.assertEqual(from_.joins[0].condition.left.name, "id")
        self.assertEqual(from_.joins[0].condition.left.parent, "t")
        self.assertEqual(from_.joins[0].condition.right.name, "id")
        self.assertEqual(from_.joins[0].condition.right.parent, "t2")
        self.assertEqual(
            from_.joins[0].condition.operator, join.JoinConditionOperator.EQUAL)
        self.assertEqual(from_.joins[0].type, join.JoinType.INNER)

        self.assertEqual(from_.joins[1].table.name, "table3")
        self.assertEqual(from_.joins[1].table.alias, "t3")
        self.assertEqual(from_.joins[1].condition.left.name, "id")
        self.assertEqual(from_.joins[1].condition.left.parent, "t2")
        self.assertEqual(from_.joins[1].condition.right.name, "id")
        self.assertEqual(from_.joins[1].condition.right.parent, "t3")
        self.assertEqual(
            from_.joins[1].condition.operator, join.JoinConditionOperator.EQUAL)
        self.assertEqual(from_.joins[1].type, join.JoinType.INNER)


if __name__ == '__main__':
    unittest.main()
