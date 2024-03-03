import sqlparse

from typing import List

from helpers import clean_tokens
from models import field, condition


def get_conditions(token: sqlparse.sql.Token):
    if type(token) != sqlparse.sql.Where:
        raise ValueError(f"Expected WHERE, got {token}")

    cleaned = clean_tokens(token.tokens)[1:]

    return __build_where_ast(cleaned)


# TODO: If we have WHERE "a" = 1, because of the double quotes, it's a field, not a string. If using single quotes, it's ok. This is weird
def __construct_operand(token: sqlparse.sql.Token) -> condition.ConditionOperand:
    if isinstance(token, sqlparse.sql.Identifier):
        return field.Field(
            name=token.get_real_name(),
            alias=token.get_alias(),
            parent=token.get_parent_name()
        )
    elif token.ttype == sqlparse.tokens.Literal.Number.Integer:
        return condition.IntOperand(value=int(token.value))
    elif token.ttype == sqlparse.tokens.Literal.Number.Float:
        return condition.FloatOperand(value=float(token.value))
    elif token.ttype == sqlparse.tokens.Literal.String.Single:
        return condition.StringOperand(value=token.value[1:-1])
    else:
        raise ValueError(f"Unsupported token: {token}")


def __build_condition(token: sqlparse.sql.Token) -> condition.Condition:
    if not isinstance(token, sqlparse.sql.Comparison):
        raise ValueError(f"Expected comparison, got {token}")

    cleaned: List[sqlparse.sql.Token] = clean_tokens(token.tokens)

    if len(cleaned) != 3:
        raise ValueError(
            f"Expected 3 tokens, got: {len(cleaned)} [{(cleaned,)}]")

    [left, operator, right] = cleaned

    # TODOO: Check, if left or right are Paranthesis, it's a subquery
    return condition.Condition(
        left=__construct_operand(left),
        operator=condition.ConditionOperator.from_string(operator.value),
        right=__construct_operand(right)
    )


def __build_where_ast(tokens: List[sqlparse.sql.Token]) -> condition.ConditionLogicalExpression | condition.Condition:
    if len(tokens) == 1:
        [token] = tokens

        return __build_condition(token)

    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token.ttype == sqlparse.tokens.Keyword and token.value.upper() in ["AND", "OR", "NOT"]:
            logical_operator = condition.ConditionLogicalExpression(
                left=__build_where_ast(tokens[:index]),
                operator=condition.ConditionLogicalOperator.from_string(
                    token.value),
                right=__build_where_ast(tokens[index + 1:])
            )

            return logical_operator

        index += 1

    return None
