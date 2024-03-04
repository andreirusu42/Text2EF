from sql_to_ast.select_ast_builder import SelectAstBuilder

from ast_to_ef.adapter import select_clause, from_clause, where_clause


def build_ef(sql: str):
    ast = SelectAstBuilder.build(sql)

    return f"{from_clause.build_from(ast.from_clause)}\n{where_clause.build_where(ast.where_clause)}\n{select_clause.build_select(ast.select_clause)}"


def main():
    sql = f"""
    SELECT T1.fname ,  T1.lname FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID  =  T2.advisor WHERE T2.fname  =  "Linda" AND T2.lname  =  "Smith"
    """

    ef = build_ef(sql)

    print(ef)


if __name__ == "__main__":
    main()
