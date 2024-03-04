from typing import List
from sql_to_ast.builder import select_clause_builder
from sql_to_ast.models import field, condition

from ast_to_ef.adapter.constants import SELECTOR


def build_select(select_clause: select_clause_builder.SelectClause):
    result = __build_select_fields(select_clause.fields)

    if select_clause.is_distinct:
        result = f"{result}.Distinct()"

    return result


# TODO: handle Wildcard, though a quick lookup shows that it's not recommended to use it
def __build_select_fields(select_fields: List[select_clause_builder.SelectField]):
    fields = []

    for select_field in select_fields:
        if isinstance(select_field, field.Field):
            result = f"{f'{select_field.alias} = ' if select_field.alias is not None else ""}{SELECTOR}.{select_field.parent + '.' if select_field.parent else ''}{select_field.name}"
            fields.append(result)
        else:
            raise ValueError(f"Invalid select field ({select_field})")

    return f".Select({SELECTOR} => new {{ {', '.join(fields)} }})"
