from typing import List

from sql_to_ast.models.field import Field
from sql_to_ast.models.function import Function
from sql_to_ast.models.wildcard import Wildcard

SelectField = Field | Function | Wildcard


class SelectClause:
    def __init__(self, fields: List[SelectField], is_distinct: bool):
        self.fields = fields
        self.is_distinct = is_distinct

    def __repr__(self):
        return f"Select(fields={self.fields}, is_distinct={self.is_distinct})"
