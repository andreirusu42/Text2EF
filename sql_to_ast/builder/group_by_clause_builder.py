import sqlparse

from typing import List
from sql_to_ast.builder.helpers import remove_punctuation, remove_whitespaces


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


def get_group_by_clause(tokens: List[sqlparse.sql.Token]) -> GroupByClause:
    group_by_token = tokens[0]

    if group_by_token.ttype != sqlparse.tokens.Keyword or group_by_token.value.upper() != 'GROUP BY':
        raise ValueError(f"Expected GROUP BY, got {(group_by_token,)}")

    tokens = remove_whitespaces(tokens[1:])
    tokens = remove_punctuation(tokens)

    fields = __build_group_by(tokens)

    return GroupByClause(fields=fields)


def __build_group_by(tokens: List[sqlparse.sql.Token]) -> List[GroupByField]:
    fields: List[GroupByField] = []

    if len(tokens) == 1 and isinstance(tokens[0], sqlparse.sql.IdentifierList):
        tokens = remove_whitespaces(tokens[0].tokens)
        tokens = remove_punctuation(tokens)

    for token in tokens:
        if isinstance(token, sqlparse.sql.Identifier):
            fields.append(GroupByField(
                name=token.get_real_name(),
                parent=token.get_parent_name(),
            ))
        else:
            raise Exception(f"Unexpected token {(token,)}")

    return fields
