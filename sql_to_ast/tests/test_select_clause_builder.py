import unittest
import sqlparse

from sqlparse.sql import Token

from builder.select_clause_builder import get_select_clause

from models import field, function, wildcard


class TestSelectClauseBuilder(unittest.TestCase):
    def get_tokens(self, sql: str) -> Token:
        return sqlparse.parse(sql)[0].tokens

    def get_select(self, sql: str):
        tokens = self.get_tokens(sql)

        return get_select_clause(tokens)

    # TODO: when having SomeFunction(arg) + ALIAS, it doesn't work :/
    def test_values(self):
        sql = "SELECT AVG(x.t), y"
        select = self.get_select(sql)

        self.assertEqual(select.is_distinct, False)
        self.assertEqual(len(select.fields), 2)
        self.assertIsInstance(select.fields[0], function.Function)
        self.assertIsInstance(select.fields[1], field.Field)

        self.assertEqual(select.fields[0].type, function.FunctionType.AVG)
        self.assertEqual(select.fields[0].field.name, "t")
        self.assertEqual(select.fields[0].field.parent, "x")

        self.assertEqual(select.fields[1].name, "y")

        sql = "SELECT a, b.c AS x, c.d"
        select = self.get_select(sql)

        self.assertEqual(select.is_distinct, False)
        self.assertEqual(len(select.fields), 3)
        self.assertIsInstance(select.fields[0], field.Field)
        self.assertIsInstance(select.fields[1], field.Field)
        self.assertIsInstance(select.fields[2], field.Field)

        self.assertEqual(select.fields[0].name, "a")
        self.assertEqual(select.fields[1].name, "c")
        self.assertEqual(select.fields[1].parent, "b")
        self.assertEqual(select.fields[1].alias, "x")

        sql = "SELECT *"
        select = self.get_select(sql)
        self.assertEqual(select.is_distinct, False)

        self.assertEqual(len(select.fields), 1)
        self.assertIsInstance(select.fields[0], wildcard.Wildcard)

    def test_distinct(self):
        sql = "SELECT DISTINCT a"
        select = self.get_select(sql)

        self.assertEqual(select.is_distinct, True)
        self.assertEqual(len(select.fields), 1)
        self.assertIsInstance(select.fields[0], field.Field)
        self.assertEqual(select.fields[0].name, "a")


if __name__ == '__main__':
    unittest.main()
