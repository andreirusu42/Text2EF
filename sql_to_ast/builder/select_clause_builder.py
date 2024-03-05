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
    if isinstance(token.tokens[0], sqlparse.sql.Function):
        tokens = remove_whitespaces(token.tokens)

        return __extract_function_from_tokens(tokens)

    return field.Field(
        name=token.get_real_name(),
        alias=token.get_alias(),
        parent=token.get_parent_name()
    )


def __extract_function_from_tokens(tokens: List[sqlparse.sql.Token]) -> function.Function:
    function_token = tokens[0]

    if not isinstance(function_token, sqlparse.sql.Function):
        raise ValueError(f"Expected Function, got {(function_token,)}")

    argument_token = function_token.tokens[1]

    if not isinstance(argument_token, sqlparse.sql.Parenthesis):
        raise ValueError(f"Expected Parenthesis, got {(argument_token, )}")

    argument_tokens: List[sqlparse.sql.Token] = remove_whitespaces(argument_token.tokens[1:-1])

    function_type = function.FunctionType.from_string(function_token.get_name().upper())

    if len(tokens) == 3:
        if tokens[1].ttype != sqlparse.tokens.Keyword or tokens[1].value.upper() != 'AS':
            raise ValueError(f"Expected AS, got {(tokens[1],)}")

        alias = tokens[2].get_real_name()
    else:
        alias = None

    if function_type == function.FunctionType.COUNT:
        return __build_count_function(argument_tokens, alias)
    elif function_type == function.FunctionType.AVG:
        return __build_avg_function(argument_tokens, alias)
    else:
        raise ValueError(f"Unexpected function type {function_type}")


def __build_avg_function(argument_tokens: List[sqlparse.sql.Token], alias: str) -> function.AvgFunction:
    if len(argument_tokens) == 0 or len(argument_tokens) > 2:
        raise ValueError(f"Unexpected avg function arguments {(argument_tokens,)}")

    is_distinct = False

    if len(argument_tokens) == 2:
        argument = argument_tokens[0]

        if argument.ttype == sqlparse.tokens.Keyword and argument.value.upper() == 'DISTINCT':
            is_distinct = True

        argument = argument_tokens[1]
    else:
        argument = argument_tokens[0]

    if isinstance(argument, sqlparse.sql.Identifier):
        field_identifier = __extract_field_from_identifier(argument)
        return function.AvgFunction(
            argument=field_identifier,
            is_distinct=False,
            alias=alias
        )
    elif argument.ttype == sqlparse.tokens.Wildcard and argument.value == '*':
        return function.AvgFunction(
            argument=wildcard.Wildcard(),
            is_distinct=is_distinct,
            alias=alias,
        )

    else:
        raise ValueError(f"Unexpected token {argument}")


def __build_count_function(argument_tokens: List[sqlparse.sql.Token], alias: str) -> function.CountFunction:
    if len(argument_tokens) == 0 or len(argument_tokens) > 2:
        raise ValueError(f"Unexpected count function arguments {(argument_tokens,)}")

    is_distinct = False

    if len(argument_tokens) == 2:
        argument = argument_tokens[0]

        if argument.ttype == sqlparse.tokens.Keyword and argument.value.upper() == 'DISTINCT':
            is_distinct = True

        argument = argument_tokens[1]
    else:
        argument = argument_tokens[0]

    if isinstance(argument, sqlparse.sql.Identifier):
        field_identifier = __extract_field_from_identifier(argument)

        return function.CountFunction(
            argument=field_identifier,
            is_distinct=is_distinct,
            alias=alias,
        )

    elif argument.ttype == sqlparse.tokens.Wildcard and argument.value == '*':
        return function.CountFunction(
            argument=wildcard.Wildcard(),
            is_distinct=is_distinct,
            alias=alias,
        )

    else:
        raise ValueError(f"Unexpected token {argument}")


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
            fields.append(__extract_function_from_tokens([token]))

        elif token.ttype == sqlparse.tokens.Wildcard and token.value == '*':
            fields.append(wildcard.Wildcard())

        else:
            raise ValueError(f"Unexpected token {(token, )}")

    return fields
