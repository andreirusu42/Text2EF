from typing import List


class DatabaseSchemaTable:
    def __init__(self, name: str, columns: List[str]):
        self.name = name
        self.columns = columns

    def __repr__(self):
        return f"DatabaseSchemaTable(name={self.name}, columns={self.columns})"


class DatabaseSchema:
    def __init__(self, name: str, tables: List[DatabaseSchemaTable]):
        self.name = name
        self.tables = tables

    def __repr__(self):
        return f"DatabaseSchema(name={self.name}, tables={self.tables})"
