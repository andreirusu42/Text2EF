from ast_to_ef.transformers.constants import CONTEXT
from sql_to_ast.select_ast_builder import build_select_ast
from sql_to_ast.models import select_ast
from ast_to_ef.transformers import from_clause_transformer, select_clause_transformer, group_by_clause_transformer, where_clause_transformer


def build_ef_code_from_select_ast(ast: select_ast.SelectAst):
    select_code = select_clause_transformer.build_select(ast.select_clause)
    from_code = from_clause_transformer.build_from(ast.from_clause)
    where_code = where_clause_transformer.build_where(ast.where_clause) if ast.where_clause else None
    group_by_code = group_by_clause_transformer.build_group_by(ast.group_by_clause) if ast.group_by_clause else None

    code = from_code

    if where_code:
        code += where_code

    if group_by_code:
        code += group_by_code

    code += select_code

    return code


def build_ef_raw_sql_code(sql: str, ast: select_ast.SelectAst):
    select_code = select_clause_transformer.build_select(ast.select_clause)
    from_code = from_clause_transformer.build_from(ast.from_clause, with_joins=False)

    return f"""{from_code}.FromSqlRaw(@"{sql}"){select_code}"""


def main():
    sql = f"""
SELECT DISTINCT T2.Name
FROM country AS T1
JOIN city AS T2 ON T2.CountryCode  =  T1.Code
WHERE T1.Continent  =  'Europe'
AND T1.Name NOT IN (SELECT T3.Name
                    FROM country AS T3
                    JOIN countrylanguage AS T4 ON T3.Code  =  T4.CountryCode WHERE T4.IsOfficial  =  'T'
                    AND T4.Language  =  'English')"""

    # sql = f"""SELECT Name FROM country WHERE IndepYear  >  1950"""
    sql = f"SELECT Region FROM country AS T1 JOIN city AS T2 ON T1.Code  =  T2.CountryCode WHERE T2.Name  =  'Kabul'"

    ast = build_select_ast(sql)

    raw_method_syntax_code = build_ef_code_from_select_ast(ast)
    raw_raw_sql_query = build_ef_raw_sql_code(sql, ast)

    method_syntax_code = f"var methodSyntaxResult = {raw_method_syntax_code};"
    raw_sql_code = f"var rawSqlResult = {raw_raw_sql_query};"

    test_code = f"""
    using var {CONTEXT} = new {ast.from_clause.table.name}Context();

    {method_syntax_code}
    {raw_sql_code}

    var areEqual = methodSyntaxResult.ToList().SequenceEqual(rawSqlResult.ToList());

    Console.WriteLine($"Are equal: {{areEqual}}");
"""

    print(test_code)


if __name__ == "__main__":
    main()
