import unittest
from ast_to_ef.adapter.constants import SELECTOR

from ast_to_ef.adapter.where_clause import build_where
from sql_to_ast.select_ast_builder import SelectAstBuilder


class TestWhereClauseAdapter(unittest.TestCase):
    def create_where(self, sql: str):
        # TODO: maybe we could make this work with only building the where clause
        select_ast = SelectAstBuilder.build(sql)

        return build_where(select_ast.where_clause)

    def test_simple(self):
        sql = "SELECT * FROM table1 WHERE x.a = 2"
        result = self.create_where(sql)
        self.assertEqual(result, f".Where({SELECTOR} => {SELECTOR}.x.a == 2)")

    def test_and(self):
        sql = "SELECT * FROM table1 WHERE x.a = 2 AND x.b = 3"
        result = self.create_where(sql)
        self.assertEqual(result, f".Where({SELECTOR} => {SELECTOR}.x.a == 2 && {SELECTOR}.x.b == 3)")

    def test_paranthesis(self):
        sql = "SELECT * FROM table1 WHERE a = b OR (c = d AND e = f)"
        result = self.create_where(sql)
        self.assertEqual(result, f".Where({SELECTOR} => {SELECTOR}.a == {SELECTOR}.b || ({SELECTOR}.c == {SELECTOR}.d && {SELECTOR}.e == {SELECTOR}.f))")


if __name__ == '__main__':
    unittest.main()
