import unittest
from ast_to_ef.transformers.constants import SELECTOR

from ast_to_ef.transformers.select_clause_transformer import build_select
from sql_to_ast.select_ast_builder import SelectAstBuilder


class TestSelectClauseTransformer(unittest.TestCase):
    def create_select(self, sql: str):
        # TODO: maybe we could make this work with only building the select clause
        select_ast = SelectAstBuilder.build(sql)

        return build_select(select_ast.select_clause)

    def test_simple(self):
        sql = "SELECT a, b FROM table1 WHERE x = 1"
        result = self.create_select(sql)
        self.assertEqual(result, f".Select({SELECTOR} => new {{ {SELECTOR}.a, {SELECTOR}.b }})")

    def test_distinct(self):
        sql = "SELECT DISTINCT a, b FROM table1 WHERE x = 1"
        result = self.create_select(sql)
        self.assertEqual(result, f".Select({SELECTOR} => new {{ {SELECTOR}.a, {SELECTOR}.b }}).Distinct()")

    def test_aliases(self):
        sql = "SELECT a AS a1, b AS b1 FROM table1 WHERE x = 1"
        result = self.create_select(sql)
        self.assertEqual(result, f".Select({SELECTOR} => new {{ a1 = {SELECTOR}.a, b1 = {SELECTOR}.b }})")


if __name__ == '__main__':
    unittest.main()
