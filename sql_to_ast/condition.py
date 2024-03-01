from enum import Enum
from field import Field


class ConditionOperator(Enum):
    EQUAL = "="
    LT = "<"
    GT = ">"
    LTE = "<="
    GTE = ">="

    @staticmethod
    def from_string(s: str):
        for op in ConditionOperator:
            if op.value == s:
                return op
        raise ValueError("Invalid operator string")


class Condition:
    def __init__(self, field: Field, operator: ConditionOperator, value: str):
        self.field = field
        self.operator = operator
        self.value = value

    def __repr__(self):
        return f'Condition(field={self.field}, operator={self.operator}, value={self.value})'


class ConditionLogicalOperator(Enum):
    AND = "AND"
    OR = "OR"

    @staticmethod
    def from_string(s: str):
        for op in ConditionLogicalOperator:
            if op.value == s:
                return op
        raise ValueError("Invalid operator string")


class ConditionLogicalExpression:
    def __init__(self, operator: ConditionOperator = None):
        self.operator = operator
        self.conditions = []
        self.children = []

    def add_condition(self, condition: Condition):
        self.conditions.append(condition)

    def add_child(self, child_logical_expr):
        self.children.append(child_logical_expr)

    def __repr__(self):
        if self.operator:
            return f'({self.operator})'
        else:
            return f'({", ".join(str(c) for c in self.conditions)})'
