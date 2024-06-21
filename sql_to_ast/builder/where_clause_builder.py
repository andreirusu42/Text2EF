import sqlparse

from typing import List

from sql_to_ast.builder.helpers import is_select, remove_punctuation, remove_whitespaces
from sql_to_ast.select_ast_builder import build_select_ast
from sql_to_ast.models.where_clause import WhereClause, WhereCondition
from sql_to_ast.models.field import Field
from sql_to_ast.models.condition import ConditionBinaryLogicalOperator, ConditionUnaryLogicalOperator, \
    ConditionBinaryLogicalExpression, ConditionUnaryLogicalExpression, ConditionOperand, \
    IntOperand, FloatOperand, StringOperand, SingleCondition, ConditionOperator, ListOperand


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


def __construct_operand_helper(token: sqlparse.sql.Token) -> ConditionOperand:
    if isinstance(token, sqlparse.sql.Identifier):
        return Field(
            name=token.get_real_name(),
            alias=token.get_alias(),
            parent=token.get_parent_name()
        )

    elif isinstance(token, sqlparse.sql.IdentifierList):
        tokens = remove_punctuation(token.tokens)
        tokens = remove_whitespaces(tokens)

        return ListOperand(
            value=[__construct_operand(token) for token in tokens]
        )

    elif token.ttype == sqlparse.tokens.Literal.Number.Integer:
        return IntOperand(value=int(token.value))

    elif token.ttype == sqlparse.tokens.Literal.Number.Float:
        return FloatOperand(value=float(token.value))

    elif token.ttype == sqlparse.tokens.Literal.String.Single:
        return StringOperand(value=token.value[1:-1])
    # TODO: this is a bold assumption, for things like "WHERE year = 1";
    elif token.ttype == sqlparse.tokens.Keyword:
        return Field(
            name=token.value
        )
    else:
        raise ValueError(f"Unsupported token: {token}")

# TODO: If we have WHERE "a" = 1, because of the double quotes, it's a field, not a string. If using single quotes, it's ok. This is weird


def __construct_operand(token: sqlparse.sql.Token) -> ConditionOperand:
    if isinstance(token, sqlparse.sql.Parenthesis):
        tokens = remove_whitespaces(token.tokens)[1:-1]

        if is_select(tokens[0]):
            return build_select_ast(token.value[1:-1])

        elif isinstance(tokens[0], sqlparse.sql.IdentifierList):
            tokens = remove_punctuation(tokens[0].tokens)
            tokens = remove_whitespaces(tokens)

            return ListOperand(
                value=[__construct_operand(token) for token in tokens]
            )

        elif len(tokens) == 1:
            return ListOperand(value=[__construct_operand_helper(tokens[0])])
        else:
            raise ValueError(f"Expected SELECT or an IdentifierList, got {(tokens[0],)}")

    return __construct_operand_helper(token)


def __build_comparison(token: sqlparse.sql.Token) -> SingleCondition:
    # TODO: if the column is really called "column", this fails, because sqlparse sees it as a keyword

    if not isinstance(token, sqlparse.sql.Comparison):
        raise ValueError(f"Expected comparison, got {(token, )}")

    cleaned: List[sqlparse.sql.Token] = remove_whitespaces(token.tokens)

    if len(cleaned) != 3:
        raise ValueError(
            f"Expected 3 tokens, got: {len(cleaned)} [{(cleaned,)}]")

    [left_token, operator, right_token] = cleaned

    return SingleCondition(
        left_operand=__construct_operand(left_token),
        operator=ConditionOperator.from_string(operator.value),
        right_operand=__construct_operand(right_token)
    )


def __build_in(tokens: List[sqlparse.sql.Token]) -> SingleCondition:
    if len(tokens) != 3:
        raise ValueError(f"Expected 3 tokens, got: {len(tokens)} [{(tokens,)}]")

    [left_token, _, right_token] = tokens

    return SingleCondition(
        left_operand=__construct_operand(left_token),
        operator=ConditionOperator.IN,
        right_operand=__construct_operand(right_token)
    )


def __build_between(tokens: List[sqlparse.sql.Token]) -> SingleCondition:
    if len(tokens) != 5:
        raise ValueError(f"Expected 5 tokens, got: {len(tokens)} [{(tokens,)}]")

    [left_token, _, value_1, _, value_2] = tokens

    return SingleCondition(
        left_operand=__construct_operand(left_token),
        operator=ConditionOperator.BETWEEN,
        right_operand=__construct_operand(sqlparse.sql.IdentifierList([value_1, value_2]))
    )


def __build_where(tokens: List[sqlparse.sql.Token]) -> WhereClause:
    condition = __build_where_helper(tokens)

    return WhereClause(condition=condition)


def __build_where_helper(tokens: List[sqlparse.sql.Token]) -> WhereCondition:
    tokens = remove_whitespaces(tokens)

    if len(tokens) == 1:
        [token] = tokens

        single_condition = __build_comparison(token)

        return single_condition

    operator_stack: List[str] = []
    operand_stack: List[WhereCondition] = []

    def __build_logical_expression():
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

    token_index = 0
    number_of_tokens = len(tokens)

    while token_index < number_of_tokens:
        token = tokens[token_index]

        if token.ttype == sqlparse.tokens.Keyword and token.value.upper() in __condition_logical_operators:
            while operator_stack and __condition_logical_operators_precedence[operator_stack[-1]] >= __condition_logical_operators_precedence[token.value.upper()]:
                __build_logical_expression()

            operator_stack.append(token.value.upper())

            token_index += 1
        elif isinstance(token, sqlparse.sql.Parenthesis):
            sub_conditions = __build_where_helper(
                remove_whitespaces(token.tokens)[1:-1])

            operand_stack.append(sub_conditions)

            token_index += 1
        elif isinstance(token, sqlparse.sql.Identifier):
            if tokens[token_index + 1].ttype == sqlparse.tokens.Keyword and tokens[token_index + 1].value.upper() == 'NOT':
                # This expects to have 2 more tokens after the NOT
                tokens_without_not = [token] + tokens[token_index + 2: token_index + 4]

                condition = __build_where_helper(tokens_without_not)

                operand_stack.append(condition)
                operator_stack.append('NOT')

                token_index += 4

            elif tokens[token_index + 1].ttype == sqlparse.tokens.Keyword and tokens[token_index + 1].value.upper() == 'IN':
                operand_stack.append(__build_in(tokens[token_index: token_index + 3]))
                token_index += 3

            elif tokens[token_index + 1].ttype == sqlparse.tokens.Keyword and tokens[token_index + 1].value.upper() == 'BETWEEN':
                operand_stack.append(__build_between(tokens[token_index: token_index + 5]))
                token_index += 5
            else:
                raise ValueError(f"Unexpected token: {token}")
        # TODO: this is another bold assumption, for things like "WHERE year = 1"
        elif token.ttype == sqlparse.tokens.Keyword:
            comparison = sqlparse.sql.Comparison(tokens[token_index: token_index + 3])

            condition = __build_comparison(comparison)

            operand_stack.append(condition)
            token_index += 3
        else:
            operand_stack.append(__build_comparison(token))
            token_index += 1

    while operator_stack:
        __build_logical_expression()

    return operand_stack[0]
