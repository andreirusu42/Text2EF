from typing import List
import sqlparse

from sql_to_ast.models import field, function, wildcard
from sql_to_ast.builder.helpers import remove_whitespaces, remove_punctuation


SelectField = field.Field | function.Function | wildcard.Wildcard


class SelectClause:
    def __init__(self, fields: List[SelectField], is_distinct: bool):
        self.fields = fields
        self.is_distinct = is_distinct

    def __repr__(self):
        return f"Select(fields={self.fields}, is_distinct={self.is_distinct})"


def get_select_clause(tokens: List[sqlparse.sql.Token]) -> SelectClause:
    select_token = tokens[0]

    if select_token.ttype != sqlparse.tokens.DML or select_token.value.upper() != 'SELECT':
        raise ValueError(f"Expected SELECT, got {select_token}")

    tokens = remove_whitespaces(tokens[1:])

    return __build_select(tokens)


def __extract_field_from_identifier(token: sqlparse.sql.Identifier) -> field.Field:
    return field.Field(
        name=token.get_real_name(),
        alias=token.get_alias(),
        parent=token.get_parent_name()
    )


def __extract_function_from_token(token: sqlparse.sql.Function) -> function.Function:
    field_token = token.tokens[1]

    if not isinstance(field_token, sqlparse.sql.Parenthesis):
        raise ValueError(f"Expected Parenthesis, got {(field_identifier, )}")

    field_identifier = field_token.tokens[1]

    # get argument of function

    return function.Function(
        type=function.FunctionType.from_string(token.get_name()),
        field=__extract_field_from_identifier(field_identifier),
        alias=token.get_alias()
    )


def __build_select(tokens: List[sqlparse.sql.Token]) -> List[SelectField]:
    is_distinct = False

    if tokens[0].ttype == sqlparse.tokens.Keyword and tokens[0].value.upper() == 'DISTINCT':
        is_distinct = True
        tokens = tokens[1:]

    fields = __build_select_helper(tokens)

    return SelectClause(
        fields=fields,
        is_distinct=is_distinct
    )


def __build_select_helper(tokens: List[sqlparse.sql.Token]) -> List[SelectField]:
    fields = []

    for token in tokens:
        if isinstance(token, sqlparse.sql.IdentifierList):
            tokens = remove_whitespaces(token.tokens)
            tokens = remove_punctuation(tokens)

            fields.extend(__build_select_helper(tokens))

        elif isinstance(token, sqlparse.sql.Identifier):
            fields.append(__extract_field_from_identifier(token))

        elif isinstance(token, sqlparse.sql.Function):
            fields.append(__extract_function_from_token(token))

        elif token.ttype == sqlparse.tokens.Wildcard and token.value == '*':
            fields.append(wildcard.Wildcard())

        else:
            raise ValueError(f"Unexpected token {(token, )}")

    return fields
