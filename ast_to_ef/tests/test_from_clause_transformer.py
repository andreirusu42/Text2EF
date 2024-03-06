import unittest
from ast_to_ef.transformers.constants import CONTEXT, SELECTOR

from ast_to_ef.transformers.from_clause_transformer import build_from
from ast_to_ef.transformers.helpers import format_code
from sql_to_ast.select_ast_builder import SelectAstBuilder


class TestFromClauseTransformer(unittest.TestCase):
    def create_from(self, sql: str):
        # TODO: maybe we could make this work with only building the from clause
        select_ast = SelectAstBuilder.build(sql)

        return build_from(select_ast.from_clause)

    def test_simple(self):
        sql = "SELECT * FROM table1 AS t1 JOIN table2 AS t2 ON t1.id = t2.id WHERE x.a = 2"
        result = self.create_from(sql)

        self.assertEqual(result, f"{CONTEXT}.table1.Join(context.table2, t1 => t1.id, t2 => t2.id, (t1, t2) => new {{ t1, t2 }})")

    def test_multiple_joins(self):
        sql = "SELECT * FROM table1 AS t1 JOIN table2 AS t2 ON t1.id = t2.id JOIN table3 AS t3 ON t1.id = t3.id WHERE x.a = 2"
        result = self.create_from(sql)

        self.assertEqual(result, f"{CONTEXT}.table1.Join(context.table2, t1 => t1.id, t2 => t2.id, (t1, t2) => new {{ t1, t2 }}).Join(context.table3, t1 => t1.id, t3 => t3.id, (t1, t3) => new {{ t1, t3 }})")


if __name__ == '__main__':
    unittest.main()
