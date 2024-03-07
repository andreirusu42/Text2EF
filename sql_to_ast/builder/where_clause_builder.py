import sqlparse

from typing import List

from sql_to_ast.builder.helpers import is_select, remove_whitespaces
from sql_to_ast.select_ast_builder import build_select_ast
from sql_to_ast.models.where_clause import WhereClause, WhereCondition
from sql_to_ast.models.field import Field
from sql_to_ast.models.condition import ConditionBinaryLogicalOperator, ConditionUnaryLogicalOperator, \
    ConditionBinaryLogicalExpression, ConditionUnaryLogicalExpression, ConditionOperand, \
    IntOperand, FloatOperand, StringOperand, SingleCondition, ConditionOperator


def is_unary_operator(operator: str):
    return operator in [ConditionUnaryLogicalOperator.NOT.value]


def is_binary_operator(operator: str):
    return operator in [ConditionBinaryLogicalOperator.AND.value, ConditionBinaryLogicalOperator.OR.value]


__condition_logical_operators_precedence = {
    ConditionBinaryLogicalOperator.OR.value: 1,
    ConditionBinaryLogicalOperator.AND.value: 2,
    ConditionUnaryLogicalOperator.NOT.value: 3
}

__condition_logical_operators = [
    ConditionBinaryLogicalOperator.AND.value,
    ConditionBinaryLogicalOperator.OR.value,
    ConditionUnaryLogicalOperator.NOT.value,
]


def get_where_clause(token: sqlparse.sql.Token) -> WhereClause:
    if type(token) != sqlparse.sql.Where:
        raise ValueError(f"Expected WHERE, got {token}")

    return __build_where(token.tokens[1:])


# TODO: If we have WHERE "a" = 1, because of the double quotes, it's a field, not a string. If using single quotes, it's ok. This is weird
def __construct_operand(token: sqlparse.sql.Token) -> ConditionOperand:
    if isinstance(token, sqlparse.sql.Identifier):
        return Field(
            name=token.get_real_name(),
            alias=token.get_alias(),
            parent=token.get_parent_name()
        )
    elif token.ttype == sqlparse.tokens.Literal.Number.Integer:
        return IntOperand(value=int(token.value))
    elif token.ttype == sqlparse.tokens.Literal.Number.Float:
        return FloatOperand(value=float(token.value))
    elif token.ttype == sqlparse.tokens.Literal.String.Single:
        return StringOperand(value=token.value[1:-1])
    else:
        raise ValueError(f"Unsupported token: {token}")


def __build_condition(token: sqlparse.sql.Token) -> SingleCondition:
    # TODO: if the column is really called "column", this fails, because sqlparse sees it as a keyword
    if not isinstance(token, sqlparse.sql.Comparison):
        raise ValueError(f"Expected comparison, got {(token, )}")

    cleaned: List[sqlparse.sql.Token] = remove_whitespaces(token.tokens)

    if len(cleaned) != 3:
        raise ValueError(
            f"Expected 3 tokens, got: {len(cleaned)} [{(cleaned,)}]")

    [left_operand, operator, right_operand] = cleaned

    if isinstance(left_operand, sqlparse.sql.Parenthesis):
        tokens = remove_whitespaces(left_operand.tokens)[1:-1]

        if not is_select(tokens[0]):
            raise ValueError(f"Expected SELECT, got {(tokens[0],)}")

        left_operand = build_select_ast(left_operand.value[1:-1])
    else:
        left_operand = __construct_operand(left_operand)

    if isinstance(right_operand, sqlparse.sql.Parenthesis):
        tokens = remove_whitespaces(right_operand.tokens)[1:-1]

        if not is_select(tokens[0]):
            raise ValueError(f"Expected SELECT, got {(tokens[0],)}")

        right_operand = build_select_ast(right_operand.value[1:-1])
    else:
        right_operand = __construct_operand(right_operand)

    return SingleCondition(
        left_operand=left_operand,
        operator=ConditionOperator.from_string(operator.value),
        right_operand=right_operand
    )


def __build_where(tokens: List[sqlparse.sql.Token]) -> WhereClause:
    condition = __build_where_helper(tokens)

    return WhereClause(condition=condition)


def __build_where_helper(tokens: List[sqlparse.sql.Token]) -> WhereCondition:
    tokens = remove_whitespaces(tokens)

    if len(tokens) == 1:
        [token] = tokens

        single_condition = __build_condition(token)

        return single_condition

    operator_stack: List[str] = []
    operand_stack: List[WhereCondition] = []

    def __build_operand():
        operator = operator_stack.pop()

        if is_unary_operator(operator):
            operand = operand_stack.pop()

            operand_stack.append(ConditionUnaryLogicalExpression(
                operator=ConditionUnaryLogicalOperator.from_string(operator),
                operand=operand
            ))

        elif is_binary_operator(operator):
            right_operand = operand_stack.pop()
            left_operand = operand_stack.pop()

            operand_stack.append(ConditionBinaryLogicalExpression(
                left_operand=left_operand,
                operator=ConditionBinaryLogicalOperator.from_string(operator),
                right_operand=right_operand
            ))
        else:
            raise ValueError(f"Unexpected operator: {operator}")

    for token in tokens:
        if token.ttype == sqlparse.tokens.Keyword and token.value.upper() in __condition_logical_operators:
            while operator_stack and __condition_logical_operators_precedence[operator_stack[-1]] >= __condition_logical_operators_precedence[token.value.upper()]:
                __build_operand()

            operator_stack.append(token.value.upper())
        elif isinstance(token, sqlparse.sql.Parenthesis):
            sub_conditions = __build_where_helper(
                remove_whitespaces(token.tokens)[1:-1])

            operand_stack.append(sub_conditions)
        else:
            operand_stack.append(__build_condition(token))

    while operator_stack:
        __build_operand()

    return operand_stack[0]
