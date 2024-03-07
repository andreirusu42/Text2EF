from typing import List


class GroupByField:
    def __init__(self, name: str, parent: str):
        self.name = name
        self.parent = parent

    def __repr__(self):
        return f"GroupByField(name={self.name}, parent={self.parent})"


class GroupByClause:
    def __init__(self, fields: List[GroupByField]):
        self.fields = fields

    def __repr__(self):
        return f"GroupBy(fields={self.fields})"
