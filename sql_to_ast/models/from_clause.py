from typing import List
from sql_to_ast.models.table import Table
from sql_to_ast.models.join import Join


class FromClause:
    def __init__(self, table: Table, joins: List[Join]):
        self.table = table
        self.joins = joins

    def __repr__(self):
        return f"From(table={self.table}, joins={self.joins})"
