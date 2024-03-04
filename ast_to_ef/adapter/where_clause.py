from sql_to_ast.builder import where_clause_builder


def build_where(where_clause: where_clause_builder.WhereClause):
    condition = where_clause.condition

    print(condition)
