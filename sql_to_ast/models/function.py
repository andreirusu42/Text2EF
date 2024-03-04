from enum import Enum

from models import field


class FunctionType(Enum):
    AVG = "AVG"

    @staticmethod
    def from_string(s: str):
        for t in FunctionType:
            if t.value == s:
                return t
        raise ValueError("Invalid function type string")


class Function:
    def __init__(self, type: FunctionType, field: field.Field, alias: str):
        self.type = type
        self.field = field
        self.alias = alias

    def __repr__(self):
        return f'Function(type={self.type}, field={self.field}, alias={self.alias})'
