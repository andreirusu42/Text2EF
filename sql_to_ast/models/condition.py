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
    def __init__(self, left: ConditionOperand, operator: ConditionOperator, right: ConditionOperand):
        self.left = left
        self.operator = operator
        self.right = right

    def __repr__(self):
        return f'Condition(left={self.left}, operator="{self.operator}", right={self.right})'


class ConditionLogicalOperator(Enum):
    AND = "AND"
    OR = "OR"
    NOT = "NOT"

    @staticmethod
    def from_string(s: str):
        for op in ConditionLogicalOperator:
            if op.value == s:
                return op
        raise ValueError("Invalid operator string")


class ConditionLogicalExpression:
    def __init__(self, left: SingleCondition, operator: ConditionLogicalOperator, right: SingleCondition):
        self.left = left
        self.operator = operator
        self.right = right

    def __repr__(self):
        return f'ConditionLogicalExpression(left={self.left}, operator={self.operator}, right={self.right})'


Condition = SingleCondition | ConditionLogicalExpression
