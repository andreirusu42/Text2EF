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

    def test_types_and_values(self):
        sql = "WHERE 1 = 1"
        conditions = self.get_where(sql).condition
        self.assertIsInstance(conditions, condition.SingleCondition)
        self.assertIsInstance(conditions.left_operand, condition.IntOperand)

        self.assertEqual(conditions.left_operand.value, 1)
        self.assertEqual(conditions.operator,
                         condition.ConditionOperator.EQUAL)
        self.assertIsInstance(conditions.right_operand, condition.IntOperand)

        self.assertEqual(conditions.right_operand.value, 1)

        sql = "WHERE 'a' = 1"
        conditions = self.get_where(sql).condition
        self.assertIsInstance(conditions, condition.SingleCondition)
        self.assertIsInstance(conditions.left_operand, condition.StringOperand)
        self.assertEqual(conditions.left_operand.value, "a")
        self.assertEqual(conditions.operator,
                         condition.ConditionOperator.EQUAL)
        self.assertIsInstance(conditions.right_operand, condition.IntOperand)
        self.assertEqual(conditions.right_operand.value, 1)

        sql = "WHERE a = '1'"
        conditions = self.get_where(sql).condition
        self.assertIsInstance(conditions, condition.SingleCondition)
        self.assertIsInstance(conditions.left_operand, condition.Field)
        self.assertEqual(conditions.left_operand.name, "a")
        self.assertEqual(conditions.left_operand.alias, None)
        self.assertEqual(conditions.left_operand.parent, None)
        self.assertEqual(conditions.operator,
                         condition.ConditionOperator.EQUAL)
        self.assertIsInstance(conditions.right_operand, condition.StringOperand)
        self.assertEqual(conditions.right_operand.value, "1")

        sql = "WHERE a = 'rain' OR b = 'snow' AND c = 'sunny'"
        conditions = self.get_where(sql).condition
        self.assertIsInstance(conditions, condition.ConditionLogicalExpression)
        self.assertIsInstance(conditions.left_operand, condition.SingleCondition)
        self.assertIsInstance(
            conditions.right_operand, condition.ConditionLogicalExpression)
        self.assertEqual(conditions.operator,
                         condition.ConditionBinaryLogicalOperator.OR)

        self.assertIsInstance(conditions.left_operand.left_operand, condition.Field)
        self.assertEqual(conditions.left_operand.left_operand.name, "a")
        self.assertEqual(conditions.left_operand.left_operand.alias, None)
        self.assertEqual(conditions.left_operand.left_operand.parent, None)
        self.assertEqual(conditions.left_operand.operator,
                         condition.ConditionOperator.EQUAL)
        self.assertIsInstance(conditions.left_operand.right_operand, condition.StringOperand)
        self.assertEqual(conditions.left_operand.right_operand.value, "rain")

        self.assertEqual(conditions.right_operand.operator,
                         condition.ConditionBinaryLogicalOperator.AND)
        self.assertIsInstance(conditions.right_operand.left_operand, condition.SingleCondition)
        self.assertIsInstance(conditions.right_operand.right_operand,
                              condition.SingleCondition)

        self.assertIsInstance(conditions.right_operand.left_operand.left_operand, condition.Field)
        self.assertEqual(conditions.right_operand.left_operand.left_operand.name, "b")
        self.assertEqual(conditions.right_operand.left_operand.left_operand.alias, None)
        self.assertEqual(conditions.right_operand.left_operand.left_operand.parent, None)
        self.assertEqual(conditions.right_operand.left_operand.operator,
                         condition.ConditionOperator.EQUAL)
        self.assertIsInstance(conditions.right_operand.left_operand.right_operand,
                              condition.StringOperand)
        self.assertEqual(conditions.right_operand.left_operand.right_operand.value, "snow")

        self.assertIsInstance(conditions.right_operand.right_operand.left_operand, condition.Field)
        self.assertEqual(conditions.right_operand.right_operand.left_operand.name, "c")
        self.assertEqual(conditions.right_operand.right_operand.left_operand.alias, None)
        self.assertEqual(conditions.right_operand.right_operand.left_operand.parent, None)
        self.assertEqual(conditions.right_operand.right_operand.operator,
                         condition.ConditionOperator.EQUAL)
        self.assertIsInstance(conditions.right_operand.right_operand.right_operand,
                              condition.StringOperand)
        self.assertEqual(conditions.right_operand.right_operand.right_operand.value, "sunny")

        sql = "WHERE T2.fname = 'Linda' AND T2.lname  = 'Smith'"
        conditions = self.get_where(sql).condition
        self.assertIsInstance(conditions, condition.ConditionLogicalExpression)
        self.assertIsInstance(conditions.left_operand, condition.SingleCondition)
        self.assertIsInstance(conditions.right_operand, condition.SingleCondition)
        self.assertEqual(conditions.operator,
                         condition.ConditionBinaryLogicalOperator.AND)

        self.assertIsInstance(conditions.left_operand.left_operand, condition.Field)
        self.assertEqual(conditions.left_operand.left_operand.name, "fname")
        self.assertEqual(conditions.left_operand.left_operand.alias, None)
        self.assertEqual(conditions.left_operand.left_operand.parent, "T2")
        self.assertEqual(conditions.left_operand.operator,
                         condition.ConditionOperator.EQUAL)
        self.assertIsInstance(conditions.left_operand.right_operand, condition.StringOperand)
        self.assertEqual(conditions.left_operand.right_operand.value, "Linda")

        self.assertIsInstance(conditions.right_operand.left_operand, condition.Field)
        self.assertEqual(conditions.right_operand.left_operand.name, "lname")
        self.assertEqual(conditions.right_operand.left_operand.alias, None)
        self.assertEqual(conditions.right_operand.left_operand.parent, "T2")
        self.assertEqual(conditions.right_operand.operator,
                         condition.ConditionOperator.EQUAL)
        self.assertIsInstance(conditions.right_operand.right_operand, condition.StringOperand)
        self.assertEqual(conditions.right_operand.right_operand.value, "Smith")

    def test_binary_operators(self):
        sql = "WHERE a = 'a' AND b = 'b' OR c = 'c'"
        conditions = self.get_where(sql).condition
        self.assertIsInstance(conditions, condition.ConditionLogicalExpression)
        self.assertIsInstance(
            conditions.left_operand, condition.ConditionLogicalExpression)
        self.assertIsInstance(
            conditions.right_operand, condition.SingleCondition)
        self.assertEqual(conditions.operator,
                         condition.ConditionBinaryLogicalOperator.OR)

        self.assertIsInstance(conditions.left_operand.left_operand, condition.SingleCondition)
        self.assertIsInstance(conditions.left_operand.right_operand, condition.SingleCondition)
        self.assertEqual(conditions.left_operand.operator,
                         condition.ConditionBinaryLogicalOperator.AND)

        self.assertIsInstance(conditions.left_operand.left_operand.left_operand, condition.Field)
        self.assertEqual(conditions.left_operand.left_operand.left_operand.name, "a")
        self.assertEqual(conditions.left_operand.left_operand.left_operand.alias, None)
        self.assertEqual(conditions.left_operand.left_operand.left_operand.parent, None)
        self.assertEqual(conditions.left_operand.left_operand.operator,
                         condition.ConditionOperator.EQUAL)
        self.assertIsInstance(conditions.left_operand.left_operand.right_operand,
                              condition.StringOperand)
        self.assertEqual(conditions.left_operand.left_operand.right_operand.value, "a")

        self.assertIsInstance(conditions.left_operand.right_operand.left_operand, condition.Field)
        self.assertEqual(conditions.left_operand.right_operand.left_operand.name, "b")
        self.assertEqual(conditions.left_operand.right_operand.left_operand.alias, None)
        self.assertEqual(conditions.left_operand.right_operand.left_operand.parent, None)
        self.assertEqual(conditions.left_operand.right_operand.operator,
                         condition.ConditionOperator.EQUAL)
        self.assertIsInstance(conditions.left_operand.right_operand.right_operand,
                              condition.StringOperand)
        self.assertEqual(conditions.left_operand.right_operand.right_operand.value, "b")

        self.assertIsInstance(conditions.right_operand.left_operand, condition.Field)
        self.assertEqual(conditions.right_operand.left_operand.name, "c")
        self.assertEqual(conditions.right_operand.left_operand.alias, None)
        self.assertEqual(conditions.right_operand.left_operand.parent, None)
        self.assertEqual(conditions.right_operand.operator,
                         condition.ConditionOperator.EQUAL)
        self.assertIsInstance(conditions.right_operand.right_operand, condition.StringOperand)
        self.assertEqual(conditions.right_operand.right_operand.value, "c")

        sql = "WHERE a = 'a' AND (b = 'b' OR c = 'c')"
        conditions = self.get_where(sql).condition
        self.assertIsInstance(conditions, condition.ConditionLogicalExpression)
        self.assertIsInstance(conditions.left_operand, condition.SingleCondition)
        self.assertIsInstance(
            conditions.right_operand, condition.ConditionLogicalExpression)
        self.assertEqual(conditions.operator,
                         condition.ConditionBinaryLogicalOperator.AND)

    def test_nested_expressions(self):
        sql = "WHERE (a = 'a' AND b = 'b') OR (c = 'c' AND (d = 'd' OR e = 'e'))"
        conditions = self.get_where(sql).condition
        self.assertIsInstance(conditions, condition.ConditionLogicalExpression)
        self.assertIsInstance(
            conditions.left_operand, condition.ConditionLogicalExpression)
        self.assertIsInstance(
            conditions.right_operand, condition.ConditionLogicalExpression)
        self.assertEqual(conditions.operator,
                         condition.ConditionBinaryLogicalOperator.OR)

        self.assertIsInstance(conditions.left_operand.left_operand, condition.SingleCondition)
        self.assertIsInstance(conditions.left_operand.right_operand, condition.SingleCondition)
        self.assertEqual(conditions.left_operand.operator,
                         condition.ConditionBinaryLogicalOperator.AND)

        self.assertIsInstance(conditions.right_operand.left_operand, condition.SingleCondition)
        self.assertIsInstance(conditions.right_operand.right_operand,
                              condition.ConditionLogicalExpression)
        self.assertEqual(conditions.right_operand.operator,
                         condition.ConditionBinaryLogicalOperator.AND)

        self.assertIsInstance(conditions.right_operand.right_operand.left_operand,
                              condition.SingleCondition)
        self.assertIsInstance(
            conditions.right_operand.right_operand.right_operand, condition.SingleCondition)

        sql = "WHERE T2.fname = 'Linda' AND (T2.lname  = 'Smith' OR T2.lname = 'Jones')"
        conditions = self.get_where(sql).condition

        self.assertIsInstance(conditions, condition.ConditionLogicalExpression)
        self.assertEqual(conditions.operator,
                         condition.ConditionBinaryLogicalOperator.AND)

        self.assertEqual(conditions.right_operand.operator,
                         condition.ConditionBinaryLogicalOperator.OR)

    def test_subqueries(self):
        sql = "WHERE a > (SELECT b FROM c)"
        conditions = self.get_where(sql).condition

        self.assertIsInstance(conditions, condition.SingleCondition)
        self.assertIsInstance(conditions.right_operand, SelectAst)

        sql = "WHERE a > (SELECT b FROM c WHERE D > (SELECT count(*) FROM x))"
        conditions = self.get_where(sql).condition

        self.assertIsInstance(conditions, condition.SingleCondition)
        self.assertIsInstance(conditions.right_operand, SelectAst)

        self.assertIsInstance(conditions.right_operand.where_clause.condition,
                              condition.SingleCondition)
        self.assertIsInstance(conditions.right_operand.where_clause.condition.right_operand,
                              SelectAst)

        sql = "WHERE (SELECT b FROM c) > 1"
        conditions = self.get_where(sql).condition
        self.assertIsInstance(conditions, condition.SingleCondition)
        self.assertIsInstance(conditions.left_operand, SelectAst)

    def test_unary_operators(self):
        sql = "WHERE NOT a = 3"
        conditions = self.get_where(sql).condition

        self.assertIsInstance(conditions, condition.ConditionUnaryLogicalExpression)
        self.assertEqual(conditions.operator,
                         condition.ConditionUnaryLogicalOperator.NOT)
        self.assertIsInstance(conditions.operand, condition.SingleCondition)

        sql = "WHERE NOT (a = 3 OR NOT b = 4)"
        conditions = self.get_where(sql).condition

        self.assertIsInstance(conditions, condition.ConditionUnaryLogicalExpression)
        self.assertEqual(conditions.operator,
                         condition.ConditionUnaryLogicalOperator.NOT)
        self.assertIsInstance(conditions.operand, condition.ConditionLogicalExpression)

        self.assertIsInstance(conditions.operand.left_operand, condition.SingleCondition)
        self.assertIsInstance(conditions.operand.right_operand, condition.ConditionUnaryLogicalExpression)
        self.assertEqual(conditions.operand.right_operand.operator,
                         condition.ConditionUnaryLogicalOperator.NOT)
        self.assertIsInstance(conditions.operand.right_operand.operand, condition.SingleCondition)

    def test_in(self):
        sql = "WHERE a IN (1.0)"
        conditions = self.get_where(sql).condition

        self.assertIsInstance(conditions, condition.SingleCondition)
        self.assertIsInstance(conditions.right_operand, condition.ListOperand)
        self.assertEqual(conditions.operator, condition.ConditionOperator.IN)
        self.assertEqual(conditions.left_operand.name, "a")
        self.assertEqual(len(conditions.right_operand.value), 1)
        self.assertEqual(conditions.right_operand.value[0].value, 1.0)

    def test_not_in(self):
        sql = "WHERE a NOT IN (1, 2, 3) AND (b > 3 OR x IN (SELECT y FROM z))"
        conditions = self.get_where(sql).condition
        print(conditions)


if __name__ == '__main__':
    unittest.main()
