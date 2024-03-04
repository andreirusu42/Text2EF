import sqlparse

from typing import List
from sql_to_ast.models import table, join, field
from sql_to_ast.builder.helpers import remove_whitespaces


class FromClause:
    def __init__(self, table: table.Table, joins: List[join.Join]):
        self.table = table
        self.joins = joins

    def __repr__(self):
        return f"From(table={self.table}, joins={self.joins})"


def get_from_clause(tokens: List[sqlparse.sql.Token]) -> FromClause:
    from_token = tokens[0]

    if from_token.ttype != sqlparse.tokens.Keyword or from_token.value.upper() != 'FROM':
        raise ValueError(f"Expected FROM, got {from_token}")

    tokens = remove_whitespaces(tokens[1:])

    table_, joins = __build_from(tokens)

    return FromClause(
        table=table_,
        joins=joins
    )


def __build_table_from_identifier(token: sqlparse.sql.Identifier) -> table.Table:
    return table.Table(
        name=token.get_real_name(),
        alias=token.get_alias(),
    )


def __build_join_condition_field(token: sqlparse.sql.Identifier) -> field.Field:
    return field.Field(
        name=token.get_real_name(),
        alias=token.get_alias(),
        parent=token.get_parent_name()
    )


def __build_join_condition_from_comparison(token: sqlparse.sql.Comparison) -> join.JoinCondition:
    cleaned: List[sqlparse.sql.Token] = list(
        filter(lambda token: not token.is_whitespace, token.tokens))

    if len(cleaned) != 3:
        raise ValueError(f"Expected 3 tokens, got: {len(cleaned)}")

    [left, operator, right] = cleaned

    return join.JoinCondition(
        left=__build_join_condition_field(left),
        operator=join.JoinConditionOperator.from_string(operator.value),
        right=__build_join_condition_field(right)
    )


def __build_from(tokens: List[sqlparse.sql.Token]) -> FromClause:
    token_index = 0

    # TODO: For some reason, if the table is really called "table", it fails, because sqlparse sees it as a keyword
    if not isinstance(tokens[token_index], sqlparse.sql.Identifier):
        raise ValueError(f"Expected identifier, got {(tokens[0], )}")

    table_ = __build_table_from_identifier(tokens[token_index])

    token_index += 1

    tokens_length = len(tokens)

    if token_index == tokens_length:
        return table_, []

    joins = []

    while token_index < tokens_length:
        token = tokens[token_index]

        join_type = None

        if token.ttype == sqlparse.tokens.Keyword and token.value.upper() == "JOIN":
            join_type = join.JoinType.INNER
        else:
            raise ValueError(f"Expected JOIN, got {(token, )}")

        token_index += 1

        if not isinstance(tokens[token_index], sqlparse.sql.Identifier):
            raise ValueError(
                f"Expected table name, got {(tokens[token_index], )}")

        join_table = __build_table_from_identifier(tokens[token_index])

        token_index += 1

        if not (tokens[token_index].ttype == sqlparse.tokens.Keyword and tokens[token_index].value.upper() == "ON"):
            raise ValueError(f"Expected ON, got {(tokens[token_index], )}")

        token_index += 1

        if not isinstance(tokens[token_index], sqlparse.sql.Comparison):
            raise ValueError(
                f"Expected comparison, got {(tokens[token_index], )}")

        join_condition = __build_join_condition_from_comparison(
            tokens[token_index])

        joins.append(join.Join(
            table=join_table,
            type=join_type,
            condition=join_condition
        ))

        token_index += 1

    return table_, joins
