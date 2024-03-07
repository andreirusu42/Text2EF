import sqlparse

from typing import List

from sql_to_ast.builder.helpers import is_select, remove_whitespaces
from sql_to_ast.select_ast_builder import build_select_ast
from sql_to_ast.models.where_clause import WhereClause, WhereCondition
from sql_to_ast.models.field import Field
from sql_to_ast.models.condition import ConditionLogicalOperator, ConditionLogicalExpression, ConditionOperand, \
    IntOperand, FloatOperand, StringOperand, SingleCondition, ConditionOperator


__condition_logical_operators_precedence = {
    ConditionLogicalOperator.OR.value: 1,
    ConditionLogicalOperator.AND.value: 2,
    ConditionLogicalOperator.NOT.value: 3
}

__condition_logical_operators = [
    ConditionLogicalOperator.AND.value,
    ConditionLogicalOperator.OR.value,
    ConditionLogicalOperator.NOT.value,
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

    [left, operator, right] = cleaned

    if isinstance(left, sqlparse.sql.Parenthesis):
        tokens = remove_whitespaces(left.tokens)[1:-1]

        if not is_select(tokens[0]):
            raise ValueError(f"Expected SELECT, got {(tokens[0],)}")

        left = build_select_ast(left.value[1:-1])
    else:
        left = __construct_operand(left)

    if isinstance(right, sqlparse.sql.Parenthesis):
        tokens = remove_whitespaces(right.tokens)[1:-1]

        if not is_select(tokens[0]):
            raise ValueError(f"Expected SELECT, got {(tokens[0],)}")

        right = build_select_ast(right.value[1:-1])
    else:
        right = __construct_operand(right)

    # TODOO: Check for subqueries
    return SingleCondition(
        left=left,
        operator=ConditionOperator.from_string(operator.value),
        right=right
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

    for token in tokens:
        if token.ttype == sqlparse.tokens.Keyword and token.value.upper() in __condition_logical_operators:
            while operator_stack and __condition_logical_operators_precedence[operator_stack[-1]] >= __condition_logical_operators_precedence[token.value.upper()]:
                operator = operator_stack.pop()
                right_operand = operand_stack.pop()
                left_operand = operand_stack.pop()
                operand_stack.append(
                    ConditionLogicalExpression(
                        left=left_operand,
                        operator=ConditionLogicalOperator.from_string(
                            operator),
                        right=right_operand
                    )
                )
            operator_stack.append(token.value.upper())
        elif isinstance(token, sqlparse.sql.Parenthesis):
            sub_conditions = __build_where_helper(
                remove_whitespaces(token.tokens)[1:-1])

            operand_stack.append(sub_conditions)
        else:
            operand_stack.append(__build_condition(token))

    while operator_stack:
        operator = operator_stack.pop()
        right_operand = operand_stack.pop()
        left_operand = operand_stack.pop()
        operand_stack.append(
            ConditionLogicalExpression(
                left=left_operand,
                operator=ConditionLogicalOperator.from_string(
                    operator),
                right=right_operand
            )
        )

    return operand_stack[0]
