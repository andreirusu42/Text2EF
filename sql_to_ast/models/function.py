from enum import Enum

from sql_to_ast.models.field import Field
from sql_to_ast.models.wildcard import Wildcard

FunctionArgument = Field | Wildcard


class FunctionType(Enum):
    AVG = "AVG"
    COUNT = "COUNT"
    MIN = "MIN"

    @staticmethod
    def from_string(s: str):
        for t in FunctionType:
            if t.value.lower() == s.lower():
                return t
        raise ValueError("Invalid function type string")


class Function:
    def __init__(self, type: FunctionType, argument: FunctionArgument, alias: str):
        self.type = type
        self.argument = argument
        self.alias = alias

    def __repr__(self):
        return f'Function(type={self.type}, argument={self.argument}, alias={self.alias})'


class CountFunction(Function):
    is_distinct: bool

    def __init__(self, argument: FunctionArgument, is_distinct: bool, alias: str):
        super().__init__(FunctionType.COUNT, argument, alias)

        self.is_distinct = is_distinct

    def __repr__(self):
        return f'CountFunction(argument={self.argument}, is_distinct={self.is_distinct}, alias={self.alias})'


class AvgFunction(Function):
    is_distinct: bool

    def __init__(self, argument: FunctionArgument, is_distinct: bool, alias: str):
        super().__init__(FunctionType.AVG, argument, alias)

        self.is_distinct = is_distinct

    def __repr__(self):
        return f'AvgFunction(argument={self.argument}, is_distinct={self.is_distinct}, alias={self.alias})'
