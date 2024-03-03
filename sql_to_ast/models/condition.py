from enum import Enum
from models.field import Field


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


ConditionOperand = Field | StringOperand | IntOperand | FloatOperand


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


class Condition:
    def __init__(self, left: ConditionOperand, operator: ConditionOperator, right: ConditionOperand):
        self.left = left
        self.operator = operator
        self.right = right

    def __repr__(self):
        return f'Condition(left={self.left}, operator={self.operator}, right={self.right})'


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
    def __init__(self, left: Condition, operator: ConditionLogicalOperator, right: Condition):
        self.left = left
        self.operator = operator
        self.right = right

    def __repr__(self):
        return f'ConditionLogicalExpression(left={self.left}, operator={self.operator}, right={self.right})'
