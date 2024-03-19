from typing import List
from sql_to_ast.builder import group_by_clause_builder
from sql_to_ast.models import function, field

from ast_to_ef.transformers.constants import SELECTOR


def build_group_by(group_by_clause: group_by_clause_builder.GroupByClause):
    result = __build_group_by_fields(group_by_clause.fields)

    return f".GroupBy({SELECTOR} => new {{ {', '.join(result)} }})"


def __build_group_by_fields(fields: List[group_by_clause_builder.GroupByField]):
    result = []

    for group_by_field in fields:
        result.append(f"""{SELECTOR}.{f"{group_by_field.parent}." if group_by_field.parent is not None else ""}{group_by_field.name}""")

    return result
