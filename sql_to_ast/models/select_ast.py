from sql_to_ast.models.select_clause import SelectClause
from sql_to_ast.models.from_clause import FromClause
from sql_to_ast.models.where_clause import WhereClause
from sql_to_ast.models.group_by_clause import GroupByClause


class SelectAst:
    def __init__(self, select_clause: SelectClause,
                 from_clause: FromClause,
                 where_clause: WhereClause,
                 group_by_clause: GroupByClause):
        self.select_clause = select_clause
        self.from_clause = from_clause
        self.where_clause = where_clause
        self.group_by_clause = group_by_clause

    def __repr__(self):
        return f"SelectAst(select_clause={self.select_clause}, from_clause={self.from_clause}, where_clause={self.where_clause}, group_by_clause={self.group_by_clause})"
