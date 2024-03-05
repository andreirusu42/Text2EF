from typing import List
import sqlparse

from sql_to_ast.builder.helpers import remove_whitespaces
from sql_to_ast.builder import select_clause_builder, where_clause_builder, from_clause_builder


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


class SelectAst:
    def __init__(self, select_clause: select_clause_builder.SelectClause, from_clause: from_clause_builder.FromClause, where_clause: where_clause_builder.WhereClause):
        self.select_clause = select_clause
        self.from_clause = from_clause
        self.where_clause = where_clause

    def __repr__(self):
        return f"SelectAst(select_clause={self.select_clause}, from_clause={self.from_clause}, where_clause={self.where_clause})"


class SelectAstBuilder:
    @staticmethod
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

        return ClauseTokens(
            select_clause=tokens[clauses["SELECT"]["start"]:clauses["SELECT"]["end"]],
            from_clause=tokens[clauses["FROM"]["start"]:clauses["FROM"]["end"]],
            where_clause=tokens[clauses["WHERE"]["start"]:clauses["WHERE"]["end"]],
            group_by_clause=tokens[clauses["GROUP BY"]["start"]:clauses["GROUP BY"]["end"]],
            having_clause=tokens[clauses["HAVING"]["start"]:clauses["HAVING"]["end"]],
            order_by_clause=tokens[clauses["ORDER BY"]["start"]:clauses["ORDER BY"]["end"]],
            limit_clause=tokens[clauses["LIMIT"]["start"]:clauses["LIMIT"]["end"]]
        )

    @staticmethod
    def build(sql: str):
        statements = sqlparse.parse(sql)

        if len(statements) != 1:
            raise ValueError('SQL should contain only one statement')

        tokens = remove_whitespaces(statements[0].tokens)

        clause_tokens = SelectAstBuilder.__get_clause_tokens(tokens)

        select_clause = select_clause_builder.get_select_clause(clause_tokens.select_clause)
        from_clause = from_clause_builder.get_from_clause(clause_tokens.from_clause)
        where_clause = where_clause_builder.get_where_clause(clause_tokens.where_clause[0])  # [0] because of how it's being parsed.

        return SelectAst(
            select_clause=select_clause,
            from_clause=from_clause,
            where_clause=where_clause
        )
