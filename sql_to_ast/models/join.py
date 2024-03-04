from enum import Enum

from models.field import Field
from models.table import Table


class JoinType(Enum):
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"

    @staticmethod
    def from_string(s: str):
        for join_type in JoinType:
            if join_type.value == s:
                return join_type
        raise ValueError("Invalid join type string")


class JoinConditionOperator(Enum):
    EQUAL = "="
    LT = "<"
    GT = ">"
    LTE = "<="
    GTE = ">="

    @staticmethod
    def from_string(s: str):
        for op in JoinConditionOperator:
            if op.value == s:
                return op
        raise ValueError("Invalid operator string")


# TODO: Fields for this will never have "alias".
class JoinCondition:
    def __init__(self, left: Field, operator: JoinConditionOperator, right: Field):
        self.left = left
        self.operator = operator
        self.right = right

    def __repr__(self):
        return f'JoinCondition(left={self.left}, operator="{self.operator}", right={self.right})'


class Join:
    def __init__(self, table: Table, type: JoinType, condition: JoinCondition | None):
        self.table = table
        self.type = type
        self.condition = condition

    def __repr__(self):
        return f'Join(table={self.table}, type={self.type}, condition={self.condition})'
