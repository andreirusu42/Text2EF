from sql_to_ast import SqlSelectStatement, Sql2AstBuilder


class Ast2EfBuilder:
    __sql: str
    __sql_ast: SqlSelectStatement

    def __init__(self, sql: str):
        self.__sql = sql

        self.__sql_ast = Sql2AstBuilder(sql).build()

    def build(self) -> str:
        query = f"context.{self.__sql_ast.table.name}"

        if self.__sql_ast.joins:
            for join in self.__sql_ast.joins:
                x = join.condition.left.parent or "x"
                y = join.condition.right.parent or "y"

                query += f".Join(context.{join.table.name}, {x} => {x}.{join.condition.left.name}, {y} => {y}.{join.condition.right.name}, ({x}, {y}) => new {{ {x}, {y} }})"

        if self.__sql_ast.conditions:
            for condition in self.__sql_ast.conditions:
                query += f".Where(x => x.{f"{condition.field.parent}." if condition.field.parent else ""}{condition.field.name} {"==" or condition.operator} {condition.value})"

        if self.__sql_ast.is_wildcard:
            query += ".Select()"
        else:
            query += f".Select(lambda x: new {{ {', '.join(f'x.{field.name}' for field in self.__sql_ast.fields)} }})"


        return query


def main():
    sql = f"""SELECT T1.fname ,  T1.lname FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID  =  T2.advisor WHERE T2.fname  =  "Linda" AND T2.lname  =  "Smith"
    """

    ef = Ast2EfBuilder(sql).build()

    print(ef)


if __name__ == "__main__":
    main()
