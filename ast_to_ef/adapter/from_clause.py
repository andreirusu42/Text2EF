from typing import List
from sql_to_ast.builder import from_clause_builder
from sql_to_ast.models import join

from ast_to_ef.adapter.constants import CONTEXT, JOIN_LEFT_DEFAULT, JOIN_RIGHT_DEFAULT, SELECTOR


def build_from(from_clause: from_clause_builder.FromClause):
    joins = __build_joins(from_clause.joins)

    # TODO: table alias?
    return f"{CONTEXT}.{from_clause.table.name}{joins}"


def __build_joins(joins: List[join.Join]):
    result = []

    for item in joins:
        x = item.condition.left.parent or JOIN_LEFT_DEFAULT
        y = item.condition.right.parent or JOIN_RIGHT_DEFAULT

        q = f".Join(context.{item.table.name}, {x} => {x}.{item.condition.left.name}, {y} => {y}.{item.condition.right.name}, ({x}, {y}) => new {{ {x}, {y} }})"

        result.append(q)

    return "".join(result)
