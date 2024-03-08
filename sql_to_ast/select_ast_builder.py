import sqlparse

from typing import List

from sql_to_ast.builder.helpers import remove_whitespaces

from sql_to_ast.builder.select_clause_builder import get_select_clause, SelectField
from sql_to_ast.builder.from_clause_builder import get_from_clause
from sql_to_ast.builder.group_by_clause_builder import get_group_by_clause
from sql_to_ast.builder.database_schema_builder import build_database_schema
from sql_to_ast.models.database_schema import DatabaseSchema
from sql_to_ast.models.table import Table
from sql_to_ast.models.field import Field
from sql_to_ast.models.select_ast import SelectAst


class ClauseTokens:
    def __init__(self,
                 select_clause: List[sqlparse.sql.Token],
                 from_clause: List[sqlparse.sql.Token],
                 where_clause: List[sqlparse.sql.Token],
                 group_by_clause: List[sqlparse.sql.Token],
                 having_clause: List[sqlparse.sql.Token],
                 order_by_clause: List[sqlparse.sql.Token],
                 limit_clause: List[sqlparse.sql.Token]):
        self.select_clause = select_clause
        self.from_clause = from_clause
        self.where_clause = where_clause
        self.group_by_clause = group_by_clause
        self.having_clause = having_clause
        self.order_by_clause = order_by_clause
        self.limit_clause = limit_clause

    def __repr__(self):
        return f"ClauseTokens(select_clause={self.select_clause}, from_clause={self.from_clause}, where_clause={self.where_clause}, group_by_clause={self.group_by_clause}, having_clause={self.having_clause}, order_by_clause={self.order_by_clause}, limit_clause={self.limit_clause})"


def __get_clause_tokens(tokens: List[sqlparse.sql.Token]) -> ClauseTokens:
    clauses = {
        "SELECT": {"start": None, "end": None},
        "FROM": {"start": None, "end": None},
        "WHERE": {"start": None, "end": None},
        "GROUP BY": {"start": None, "end": None},
        "HAVING": {"start": None, "end": None},
        "ORDER BY": {"start": None, "end": None},
        "LIMIT": {"start": None, "end": None}
    }

    last_clause = None

    for index, token in enumerate(tokens):
        if token.ttype == sqlparse.tokens.Keyword and token.normalized in clauses or isinstance(token, sqlparse.sql.Where) or \
                token.ttype == sqlparse.tokens.DML and token.normalized in clauses:

            current_clause = token.normalized
            if isinstance(token, sqlparse.sql.Where):
                current_clause = "WHERE"

            if last_clause and last_clause != current_clause:
                clauses[last_clause]["end"] = index
            if clauses[current_clause]["start"] is None:
                clauses[current_clause]["start"] = index
            last_clause = current_clause

            last_clause = current_clause

    if last_clause:
        clauses[last_clause]["end"] = len(tokens)

    def __get_tokens(clause: str):
        start = clauses[clause]["start"]
        end = clauses[clause]["end"]

        if start is None or end is None:
            return []

        return tokens[start:end]

    return ClauseTokens(
        select_clause=__get_tokens("SELECT"),
        from_clause=__get_tokens("FROM"),
        where_clause=__get_tokens("WHERE"),
        group_by_clause=__get_tokens("GROUP BY"),
        having_clause=__get_tokens("HAVING"),
        order_by_clause=__get_tokens("ORDER BY"),
        limit_clause=__get_tokens("LIMIT")
    )


def __normalize_field(
        field: SelectField,
        tables: List[Table],
        schema: DatabaseSchema
):
    if not isinstance(field, Field):
        return

    if field.parent is not None:
        return

    for table in schema.tables:
        if field.name in table.columns:
            for ast_table in tables:
                if ast_table.name == table.name:
                    if ast_table.alias:
                        field.parent = ast_table.alias
                    else:
                        field.parent = ast_table.name
                return


def __normalize_select_ast(
    ast: SelectAst,
    schema: DatabaseSchema
) -> SelectAst:

    all_tables: List[Table] = [ast.from_clause.table] + [join.table for join in ast.from_clause.joins]

    if ast.from_clause.joins:
        for field in ast.select_clause.fields:
            __normalize_field(field, all_tables, schema)

    return ast


def build_select_ast(sql: str, schema: DatabaseSchema):
    # TODO: this was the only solution I could find to avoid the circular import
    from sql_to_ast.builder.where_clause_builder import get_where_clause

    statements = sqlparse.parse(sql)

    if len(statements) != 1:
        raise ValueError('SQL should contain only one statement')

    tokens = remove_whitespaces(statements[0].tokens)

    clause_tokens = __get_clause_tokens(tokens)

    select_clause = get_select_clause(clause_tokens.select_clause)
    from_clause = get_from_clause(clause_tokens.from_clause)

    # [0] because of how it's being parsed.
    where_clause = get_where_clause(clause_tokens.where_clause[0]) if clause_tokens.where_clause else None
    group_by_clause = get_group_by_clause(clause_tokens.group_by_clause) if clause_tokens.group_by_clause else None

    ast = SelectAst(
        select_clause=select_clause,
        from_clause=from_clause,
        where_clause=where_clause,
        group_by_clause=group_by_clause
    )

    return __normalize_select_ast(ast, schema)


if __name__ == '__main__':
    schema = build_database_schema('mydb', f"""
        CREATE TABLE Table1 (
            a INT,
            b INT
        );

        CREATE TABLE Table2 (
            a INT,
            c INT,
            d INT
        );
    """)

    result = build_select_ast(f"""
    SELECT a
    FROM Table1 JOIN table2 ON Table1.a = table2.a
""", schema)

    print(result)
