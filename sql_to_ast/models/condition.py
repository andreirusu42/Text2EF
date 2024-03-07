from __future__ import annotations
from typing import Union, TYPE_CHECKING

from enum import Enum

from sql_to_ast.models.field import Field

if TYPE_CHECKING:
    from sql_to_ast.models.select_ast import SelectAst


class StringOperand:
    def __init__(self, value: str):
        self.value = value

    def __repr__(self):
        return f'StringOperand({self.value})'


class IntOperand:
    def __init__(self, value: str):
        self.value = value

    def __repr__(self):
        return f'IntOperand({self.value})'


class FloatOperand:
    def __init__(self, value: str):
        self.value = value

    def __repr__(self):
        return f'FloatOperand({self.value})'


ConditionOperand = Union[Field, StringOperand, IntOperand, FloatOperand, 'SelectAst']


class ConditionOperator(Enum):
    EQUAL = "="
    LT = "<"
    GT = ">"
    LTE = "<="
    GTE = ">="
    LIKE = "LIKE"

    @staticmethod
    def from_string(s: str):
        for op in ConditionOperator:
            if op.value == s:
                return op
        raise ValueError("Invalid operator string")


class SingleCondition:
    def __init__(self, left_operand: ConditionOperand, operator: ConditionOperator, right_operand: ConditionOperand):
        self.left_operand = left_operand
        self.operator = operator
        self.right_operand = right_operand

    def __repr__(self):
        return f'Condition(left_operand={self.left_operand}, operator="{self.operator}", right_operand={self.right_operand})'


class ConditionBinaryLogicalOperator(Enum):
    AND = "AND"
    OR = "OR"

    @staticmethod
    def from_string(s: str):
        for op in ConditionBinaryLogicalOperator:
            if op.value == s:
                return op
        raise ValueError("Invalid operator string")


class ConditionUnaryLogicalOperator(Enum):
    NOT = "NOT"

    @staticmethod
    def from_string(s: str):
        for op in ConditionUnaryLogicalOperator:
            if op.value == s:
                return op
        raise ValueError("Invalid operator string")


class ConditionBinaryLogicalExpression:
    def __init__(self, left_operand: Condition, operator: ConditionBinaryLogicalOperator, right_operand: Condition):
        self.left_operand = left_operand
        self.operator = operator
        self.right_operand = right_operand

    def __repr__(self):
        return f'ConditionLogicalExpression(left_operand={self.left_operand}, operator={self.operator}, right_operand={self.right_operand})'


class ConditionUnaryLogicalExpression:
    def __init__(self, operator: ConditionUnaryLogicalOperator, operand: Condition):
        self.operator = operator
        self.operand = operand

    def __repr__(self):
        return f'ConditionUnaryLogicalExpression(operator={self.operator}, operand={self.operand})'


ConditionLogicalExpression = Union[ConditionBinaryLogicalExpression, ConditionUnaryLogicalExpression]

Condition = SingleCondition | ConditionBinaryLogicalExpression | ConditionUnaryLogicalExpression
