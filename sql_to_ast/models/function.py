from enum import Enum

from sql_to_ast.models import field, wildcard

FunctionArgument = field.Field | wildcard.Wildcard


class FunctionType(Enum):
    AVG = "AVG"
    COUNT = "COUNT"

    @staticmethod
    def from_string(s: str):
        for t in FunctionType:
            if t.value == s:
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
