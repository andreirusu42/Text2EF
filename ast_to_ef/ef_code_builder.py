from ast_to_ef.select_ast_fields_mapper import map_select_ast_fields
from ast_to_ef.transformers.constants import CONTEXT
from sql_to_ast.select_ast_builder import build_select_ast
from sql_to_ast.models import select_ast
from sql_to_ast.models.database_schema import DatabaseSchema
from ast_to_ef.schema_mapper import SchemaMapping
from ast_to_ef.transformers import from_clause_transformer, select_clause_transformer, group_by_clause_transformer, where_clause_transformer


def build_ef_code_from_select_ast(ast: select_ast.SelectAst):
    from_code = from_clause_transformer.build_from(ast.from_clause)
    where_code = where_clause_transformer.build_where(ast.where_clause) if ast.where_clause else None
    group_by_code = group_by_clause_transformer.build_group_by(ast.group_by_clause) if ast.group_by_clause else None
    select_code = select_clause_transformer.build_select(ast.select_clause, has_group_by=group_by_code is not None)

    code = from_code

    if where_code:
        code += where_code

    if group_by_code:
        code += group_by_code

    code += select_code

    return code


def build_ef_raw_sql_code(sql: str, ast: select_ast.SelectAst):
    select_code = select_clause_transformer.build_select(ast.select_clause, has_group_by=False)
    from_code = from_clause_transformer.build_from(ast.from_clause, with_joins=False)

    return f"""{from_code}.FromSqlRaw(@"{sql}"){select_code}"""


def build_ef_code(sql: str, database_schema: DatabaseSchema, schema_mapping: SchemaMapping):
    ast = build_select_ast(sql, database_schema)
    ast = map_select_ast_fields(ast, schema_mapping)

    raw_method_syntax_code = build_ef_code_from_select_ast(ast)
    raw_raw_sql_query = build_ef_raw_sql_code(sql, ast)

    method_syntax_code = f"var methodSyntaxResult = {raw_method_syntax_code}"
    raw_sql_code = f"var rawSqlResult = {raw_raw_sql_query}"

    code = f"""
        using var {CONTEXT} = new {database_schema.name}_Context();

        {method_syntax_code}
"""

    return code
