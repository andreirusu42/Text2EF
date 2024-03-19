from sql_to_ast.models.select_ast import SelectAst
from sql_to_ast.models.from_clause import FromClause

from ast_to_ef.schema_mapper import SchemaMapping

def map_select_ast_fields(ast: SelectAst, schema_mapping: SchemaMapping):
    print(ast.from_clause)

    __map_select_ast_from_clause(ast.from_clause, schema_mapping)

    print(ast.from_clause)
    input("y: ")
    return ast

def __map_select_ast_from_clause(from_clause: FromClause, schema_mapping: SchemaMapping):
    from_clause.table.name = schema_mapping.get_table_name(from_clause.table.name)

    for join in from_clause.joins:
        if join.condition:
            join.condition.left.name = schema_mapping.get_column_name(join.table.name, join.condition.left.name)
            join.condition.right.name = schema_mapping.get_column_name(join.table.name, join.condition.right.name)

            if schema_mapping.has_table(join.condition.left.parent):
                join.condition.left.parent = schema_mapping.get_table_name(join.condition.left.parent)
            
            if schema_mapping.has_table(join.condition.right.parent):
                join.condition.right.parent = schema_mapping.get_table_name(join.condition.right.parent)

        join.table.name = schema_mapping.get_table_name(join.table.name)

    return 