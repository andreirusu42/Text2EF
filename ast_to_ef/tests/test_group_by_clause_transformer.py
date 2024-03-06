import unittest
from ast_to_ef.transformers.constants import SELECTOR

from ast_to_ef.transformers.group_by_clause_transformer import build_group_by
from sql_to_ast.select_ast_builder import SelectAstBuilder


class TestGroupByClauseTransformer(unittest.TestCase):
    def create_group_by(self, sql: str):
        # TODO: maybe we could make this work with only building the group by clause
        select_ast = SelectAstBuilder.build(sql)

        return build_group_by(select_ast.group_by_clause)

    def test_simple(self):
        sql = "SELECT a, b FROM table1 GROUP BY a, b"

        result = self.create_group_by(sql)

        self.assertEqual(result, f".GroupBy({SELECTOR} => new {{ {SELECTOR}.a, {SELECTOR}.b }})")

    def test_with_parent(self):
        sql = "SELECT a, b FROM table1 GROUP BY table1.a, table1.b"

        result = self.create_group_by(sql)

        self.assertEqual(result, f".GroupBy({SELECTOR} => new {{ {SELECTOR}.table1.a, {SELECTOR}.table1.b }})")


if __name__ == '__main__':
    unittest.main()
