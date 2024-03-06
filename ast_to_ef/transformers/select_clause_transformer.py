from typing import List
from sql_to_ast.builder import select_clause_builder
from sql_to_ast.models import field, function, wildcard

from ast_to_ef.transformers.constants import SELECTOR


def build_select(select_clause: select_clause_builder.SelectClause):
    result = __build_select_fields(select_clause.fields)

    if select_clause.is_distinct:
        result = f"{result}.Distinct()"

    return result


# TODO: handle Wildcard, though a quick lookup shows that it's not recommended to use it
def __build_select_fields(select_fields: List[select_clause_builder.SelectField]):
    fields = []
    functions = []

    for select_field in select_fields:
        if isinstance(select_field, field.Field):
            result = f"{f'{select_field.alias} = ' if select_field.alias is not None else ""}{SELECTOR}.{select_field.parent + '.' if select_field.parent else ''}{select_field.name}"
            fields.append(result)
        elif isinstance(select_field, function.Function):
            result = __build_function(select_field)
            functions.append(result)
        else:
            raise ValueError(f"Invalid select field ({select_field})")

    if fields and functions:
        raise ValueError(f"Invalid select fields ({fields}, {functions})")

    if fields:
        return f".Select({SELECTOR} => new {{ {', '.join(fields)} }})"
    elif functions:
        return f"{'\n'.join(functions)}"
    else:
        raise ValueError(f"No select fields found")


def __build_function(func: function.Function):
    if isinstance(func, function.CountFunction):
        return __build_count_function(func)
    else:
        raise ValueError(f"Invalid function type ({func})")


def __build_count_function(func: function.CountFunction):
    if isinstance(func.argument, wildcard.Wildcard):
        if func.is_distinct:
            raise ValueError(f"Handling of count(distinct *) is not supported yet")
        return f".Count()"
    else:
        raise ValueError(f"Invalid count function argument ({(func.argument,)})")
