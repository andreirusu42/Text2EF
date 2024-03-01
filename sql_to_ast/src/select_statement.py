
from typing import List

from field import Field
from join import Join
from table import Table
from condition import Condition


class SqlSelectStatement:
    def __init__(self, fields: List[Field], table: Table, joins: List[Join], conditions: List[Condition], is_wildcard: bool):
        self.fields = fields
        self.table = table
        self.joins = joins
        self.conditions = conditions
        self.is_wildcard = is_wildcard

    def __repr__(self):
        return f"""SelectStatement(
            fields={self.fields},
            is_wildcard={self.is_wildcard},
            table={self.table},
            joins={self.joins},
            conditions={self.conditions}"""


class SelectStatementBuilder:
    def __init__(self):
        self.fields = []
        self.joins = []
        self.is_wildcard = False
        self.conditions = []
        self.table = None

    def add_field(self, field: Field):
        self.fields.append(field)
        return self

    def set_wildcard(self):
        self.is_wildcard = True
        return self

    def add_condition(self, condition: Condition):
        self.conditions.append(condition)
        return self

    def set_table(self, table: Table):
        self.table = table
        return self

    def add_join(self, join: Join):
        self.joins.append(join)
        return self

    def build(self):
        if not self.table:
            raise ValueError("Table must be set")

        return SqlSelectStatement(
            fields=self.fields,
            table=self.table,
            joins=self.joins,
            conditions=self.conditions,
            is_wildcard=self.is_wildcard)
