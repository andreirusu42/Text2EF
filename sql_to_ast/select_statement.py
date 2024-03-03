
from typing import List

from models import field, table, join, condition


class SqlSelectStatement:
    def __init__(self, fields: List[field.Field], table: table.Table, joins: List[join.Join], conditions: condition.Conditions, is_wildcard: bool):
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
        self.conditions = None
        self.table = None

    def add_field(self, field: field.Field):
        self.fields.append(field)
        return self

    def set_wildcard(self):
        self.is_wildcard = True
        return self

    def add_conditions(self, conditions: condition.Conditions):
        self.conditions = conditions
        return self

    def set_table(self, table: table.Table):
        self.table = table
        return self

    def add_join(self, join: join.Join):
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
