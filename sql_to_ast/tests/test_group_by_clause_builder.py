import unittest
import sqlparse

from sqlparse.sql import Token

from sql_to_ast.builder.group_by_clause_builder import get_group_by_clause
from sql_to_ast.models import join


class TestGroupByClauseBuilder(unittest.TestCase):
    def get_tokens(self, sql: str) -> Token:
        return sqlparse.parse(sql)[0].tokens

    def get_group_by(self, sql: str):
        tokens = self.get_tokens(sql)

        return get_group_by_clause(tokens)

    def test(self):
        sql = "GROUP BY column1"
        group_by = self.get_group_by(sql)

        self.assertEqual(len(group_by.fields), 1)
        self.assertEqual(group_by.fields[0].name, "column1")
        self.assertEqual(group_by.fields[0].parent, None)

        sql = "GROUP BY a.column1"
        group_by = self.get_group_by(sql)

        self.assertEqual(len(group_by.fields), 1)
        self.assertEqual(group_by.fields[0].name, "column1")
        self.assertEqual(group_by.fields[0].parent, "a")

        sql = "GROUP BY a.column1, b.column2"
        group_by = self.get_group_by(sql)

        self.assertEqual(len(group_by.fields), 2)
        self.assertEqual(group_by.fields[0].name, "column1")
        self.assertEqual(group_by.fields[0].parent, "a")
        self.assertEqual(group_by.fields[1].name, "column2")
        self.assertEqual(group_by.fields[1].parent, "b")


if __name__ == '__main__':
    unittest.main()
