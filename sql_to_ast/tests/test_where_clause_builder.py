import unittest
import sqlparse

from sqlparse.sql import Token

from sql_to_ast.builder.where_clause_builder import get_where_clause
from sql_to_ast.models import condition
from sql_to_ast.models.select_ast import SelectAst


class TestWhereClauseBuilder(unittest.TestCase):
    def get_token(self, sql: str) -> Token:
        tokens = sqlparse.parse(sql)[0].tokens

        return tokens[0]

    def get_where(self, sql: str):
        token = self.get_token(sql)

        return get_where_clause(token)

    # def test_types_and_values(self):
    #     sql = "WHERE 1 = 1"
    #     conditions = self.get_where(sql).condition
    #     self.assertIsInstance(conditions, condition.SingleCondition)
    #     self.assertIsInstance(conditions.left, condition.IntOperand)

    #     self.assertEqual(conditions.left.value, 1)
    #     self.assertEqual(conditions.operator,
    #                      condition.ConditionOperator.EQUAL)
    #     self.assertIsInstance(conditions.right, condition.IntOperand)

    #     self.assertEqual(conditions.right.value, 1)

    #     sql = "WHERE 'a' = 1"
    #     conditions = self.get_where(sql).condition
    #     self.assertIsInstance(conditions, condition.SingleCondition)
    #     self.assertIsInstance(conditions.left, condition.StringOperand)
    #     self.assertEqual(conditions.left.value, "a")
    #     self.assertEqual(conditions.operator,
    #                      condition.ConditionOperator.EQUAL)
    #     self.assertIsInstance(conditions.right, condition.IntOperand)
    #     self.assertEqual(conditions.right.value, 1)

    #     sql = "WHERE a = '1'"
    #     conditions = self.get_where(sql).condition
    #     self.assertIsInstance(conditions, condition.SingleCondition)
    #     self.assertIsInstance(conditions.left, condition.Field)
    #     self.assertEqual(conditions.left.name, "a")
    #     self.assertEqual(conditions.left.alias, None)
    #     self.assertEqual(conditions.left.parent, None)
    #     self.assertEqual(conditions.operator,
    #                      condition.ConditionOperator.EQUAL)
    #     self.assertIsInstance(conditions.right, condition.StringOperand)
    #     self.assertEqual(conditions.right.value, "1")

    #     sql = "WHERE a = 'rain' OR b = 'snow' AND c = 'sunny'"
    #     conditions = self.get_where(sql).condition
    #     self.assertIsInstance(conditions, condition.ConditionLogicalExpression)
    #     self.assertIsInstance(conditions.left, condition.SingleCondition)
    #     self.assertIsInstance(
    #         conditions.right, condition.ConditionLogicalExpression)
    #     self.assertEqual(conditions.operator,
    #                      condition.ConditionLogicalOperator.OR)

    #     self.assertIsInstance(conditions.left.left, condition.Field)
    #     self.assertEqual(conditions.left.left.name, "a")
    #     self.assertEqual(conditions.left.left.alias, None)
    #     self.assertEqual(conditions.left.left.parent, None)
    #     self.assertEqual(conditions.left.operator,
    #                      condition.ConditionOperator.EQUAL)
    #     self.assertIsInstance(conditions.left.right, condition.StringOperand)
    #     self.assertEqual(conditions.left.right.value, "rain")

    #     self.assertEqual(conditions.right.operator,
    #                      condition.ConditionLogicalOperator.AND)
    #     self.assertIsInstance(conditions.right.left, condition.SingleCondition)
    #     self.assertIsInstance(conditions.right.right,
    #                           condition.SingleCondition)

    #     self.assertIsInstance(conditions.right.left.left, condition.Field)
    #     self.assertEqual(conditions.right.left.left.name, "b")
    #     self.assertEqual(conditions.right.left.left.alias, None)
    #     self.assertEqual(conditions.right.left.left.parent, None)
    #     self.assertEqual(conditions.right.left.operator,
    #                      condition.ConditionOperator.EQUAL)
    #     self.assertIsInstance(conditions.right.left.right,
    #                           condition.StringOperand)
    #     self.assertEqual(conditions.right.left.right.value, "snow")

    #     self.assertIsInstance(conditions.right.right.left, condition.Field)
    #     self.assertEqual(conditions.right.right.left.name, "c")
    #     self.assertEqual(conditions.right.right.left.alias, None)
    #     self.assertEqual(conditions.right.right.left.parent, None)
    #     self.assertEqual(conditions.right.right.operator,
    #                      condition.ConditionOperator.EQUAL)
    #     self.assertIsInstance(conditions.right.right.right,
    #                           condition.StringOperand)
    #     self.assertEqual(conditions.right.right.right.value, "sunny")

    #     sql = "WHERE T2.fname = 'Linda' AND T2.lname  = 'Smith'"
    #     conditions = self.get_where(sql).condition
    #     self.assertIsInstance(conditions, condition.ConditionLogicalExpression)
    #     self.assertIsInstance(conditions.left, condition.SingleCondition)
    #     self.assertIsInstance(conditions.right, condition.SingleCondition)
    #     self.assertEqual(conditions.operator,
    #                      condition.ConditionLogicalOperator.AND)

    #     self.assertIsInstance(conditions.left.left, condition.Field)
    #     self.assertEqual(conditions.left.left.name, "fname")
    #     self.assertEqual(conditions.left.left.alias, None)
    #     self.assertEqual(conditions.left.left.parent, "T2")
    #     self.assertEqual(conditions.left.operator,
    #                      condition.ConditionOperator.EQUAL)
    #     self.assertIsInstance(conditions.left.right, condition.StringOperand)
    #     self.assertEqual(conditions.left.right.value, "Linda")

    #     self.assertIsInstance(conditions.right.left, condition.Field)
    #     self.assertEqual(conditions.right.left.name, "lname")
    #     self.assertEqual(conditions.right.left.alias, None)
    #     self.assertEqual(conditions.right.left.parent, "T2")
    #     self.assertEqual(conditions.right.operator,
    #                      condition.ConditionOperator.EQUAL)
    #     self.assertIsInstance(conditions.right.right, condition.StringOperand)
    #     self.assertEqual(conditions.right.right.value, "Smith")

    # def test_operators_order(self):
    #     sql = "WHERE a = 'a' AND b = 'b' OR c = 'c'"
    #     conditions = self.get_where(sql).condition
    #     self.assertIsInstance(conditions, condition.ConditionLogicalExpression)
    #     self.assertIsInstance(
    #         conditions.left, condition.ConditionLogicalExpression)
    #     self.assertIsInstance(
    #         conditions.right, condition.SingleCondition)
    #     self.assertEqual(conditions.operator,
    #                      condition.ConditionLogicalOperator.OR)

    #     self.assertIsInstance(conditions.left.left, condition.SingleCondition)
    #     self.assertIsInstance(conditions.left.right, condition.SingleCondition)
    #     self.assertEqual(conditions.left.operator,
    #                      condition.ConditionLogicalOperator.AND)

    #     self.assertIsInstance(conditions.left.left.left, condition.Field)
    #     self.assertEqual(conditions.left.left.left.name, "a")
    #     self.assertEqual(conditions.left.left.left.alias, None)
    #     self.assertEqual(conditions.left.left.left.parent, None)
    #     self.assertEqual(conditions.left.left.operator,
    #                      condition.ConditionOperator.EQUAL)
    #     self.assertIsInstance(conditions.left.left.right,
    #                           condition.StringOperand)
    #     self.assertEqual(conditions.left.left.right.value, "a")

    #     self.assertIsInstance(conditions.left.right.left, condition.Field)
    #     self.assertEqual(conditions.left.right.left.name, "b")
    #     self.assertEqual(conditions.left.right.left.alias, None)
    #     self.assertEqual(conditions.left.right.left.parent, None)
    #     self.assertEqual(conditions.left.right.operator,
    #                      condition.ConditionOperator.EQUAL)
    #     self.assertIsInstance(conditions.left.right.right,
    #                           condition.StringOperand)
    #     self.assertEqual(conditions.left.right.right.value, "b")

    #     self.assertIsInstance(conditions.right.left, condition.Field)
    #     self.assertEqual(conditions.right.left.name, "c")
    #     self.assertEqual(conditions.right.left.alias, None)
    #     self.assertEqual(conditions.right.left.parent, None)
    #     self.assertEqual(conditions.right.operator,
    #                      condition.ConditionOperator.EQUAL)
    #     self.assertIsInstance(conditions.right.right, condition.StringOperand)
    #     self.assertEqual(conditions.right.right.value, "c")

    #     sql = "WHERE a = 'a' AND (b = 'b' OR c = 'c')"
    #     conditions = self.get_where(sql).condition
    #     self.assertIsInstance(conditions, condition.ConditionLogicalExpression)
    #     self.assertIsInstance(conditions.left, condition.SingleCondition)
    #     self.assertIsInstance(
    #         conditions.right, condition.ConditionLogicalExpression)
    #     self.assertEqual(conditions.operator,
    #                      condition.ConditionLogicalOperator.AND)

    # def test_nested_expressions(self):
    #     sql = "WHERE (a = 'a' AND b = 'b') OR (c = 'c' AND (d = 'd' OR e = 'e'))"
    #     conditions = self.get_where(sql).condition
    #     self.assertIsInstance(conditions, condition.ConditionLogicalExpression)
    #     self.assertIsInstance(
    #         conditions.left, condition.ConditionLogicalExpression)
    #     self.assertIsInstance(
    #         conditions.right, condition.ConditionLogicalExpression)
    #     self.assertEqual(conditions.operator,
    #                      condition.ConditionLogicalOperator.OR)

    #     self.assertIsInstance(conditions.left.left, condition.SingleCondition)
    #     self.assertIsInstance(conditions.left.right, condition.SingleCondition)
    #     self.assertEqual(conditions.left.operator,
    #                      condition.ConditionLogicalOperator.AND)

    #     self.assertIsInstance(conditions.right.left, condition.SingleCondition)
    #     self.assertIsInstance(conditions.right.right,
    #                           condition.ConditionLogicalExpression)
    #     self.assertEqual(conditions.right.operator,
    #                      condition.ConditionLogicalOperator.AND)

    #     self.assertIsInstance(conditions.right.right.left,
    #                           condition.SingleCondition)
    #     self.assertIsInstance(
    #         conditions.right.right.right, condition.SingleCondition)

    #     sql = "WHERE T2.fname = 'Linda' AND (T2.lname  = 'Smith' OR T2.lname = 'Jones')"
    #     conditions = self.get_where(sql).condition

    #     self.assertIsInstance(conditions, condition.ConditionLogicalExpression)
    #     self.assertEqual(conditions.operator,
    #                      condition.ConditionLogicalOperator.AND)

    #     self.assertEqual(conditions.right.operator,
    #                      condition.ConditionLogicalOperator.OR)

    def test_subqueries(self):
        sql = "WHERE a > (SELECT b FROM c)"
        conditions = self.get_where(sql).condition

        self.assertIsInstance(conditions, condition.SingleCondition)
        self.assertIsInstance(conditions.right, SelectAst)

        sql = "WHERE a > (SELECT b FROM c WHERE D > (SELECT count(*) FROM x))"
        conditions = self.get_where(sql).condition

        self.assertIsInstance(conditions, condition.SingleCondition)
        self.assertIsInstance(conditions.right, SelectAst)

        self.assertIsInstance(conditions.right.where_clause.condition,
                              condition.SingleCondition)
        self.assertIsInstance(conditions.right.where_clause.condition.right,
                              SelectAst)

        sql = "WHERE (SELECT b FROM c) > 1"
        conditions = self.get_where(sql).condition
        self.assertIsInstance(conditions, condition.SingleCondition)
        self.assertIsInstance(conditions.left, SelectAst)


if __name__ == '__main__':
    unittest.main()
