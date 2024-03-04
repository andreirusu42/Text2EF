from sql_to_ast.models import condition, field
from sql_to_ast.builder import where_clause_builder
from sql_to_ast.select_ast_builder import SelectAstBuilder


class Ast2EfBuilder:
    @staticmethod
    def build(sql: str) -> str:
        [select_clause, from_clause,
            where_clause] = SelectAstBuilder.build(sql)

        query = f"context.{from_clause.table.name}"

        if from_clause.joins:
            for join in from_clause.joins:
                x = join.condition.left.parent or "x"
                y = join.condition.right.parent or "y"

                query += f".Join(context.{join.table.name}, {x} => {x}.{join.condition.left.name}, " \
                    f"{y} => {y}.{join.condition.right.name}, ({x}, {
                    y}) => new {{ {x}, {y} }})"

        if where_clause.condition:
            if isinstance(where_clause.condition, condition.SingleCondition):
                query += f".Where(x => x.{where_clause.condition.left.name} " \
                    f"{where_clause.condition.operator.value} {
                        where_clause.condition.right.name})"

        if select_clause.is_distinct:
            query += ".Distinct()"

        query += ".Select(x => new { " + ', '.join([f"x.{field.parent + '.' if field.parent else ''}{
            field.name}" for field in select_clause.fields]) + " })"

        return query

    @staticmethod
    def __build_where(where_clause: where_clause_builder.WhereClause):
        condition = where_clause.condition

        return Ast2EfBuilder.__build_where_condition(condition)

    @staticmethod
    def __build_where_condition(where_condition: where_clause_builder.WhereCondition):
        if isinstance(where_condition, condition.SingleCondition):
            if isinstance(where_condition.left, field.Field) and isinstance(where_condition.right, field.Field):
                left = f"x.{where_condition.left.parent +
                            '.' if where_condition.left.parent else ''}{where_condition.left.name}"
                right = f"x.{where_condition.right.parent +
                             '.' if where_condition.right.parent else ''}{where_condition.right.name}"

                return f"{left} {where_condition.operator.value} {right}"
            else:
                raise Exception("Can only handle field = field")

        elif isinstance(where_condition, condition.ConditionLogicalExpression):
            left = Ast2EfBuilder.__build_where_condition(where_condition.left)
            right = Ast2EfBuilder.__build_where_condition(
                where_condition.right)
            return f"{left} {where_condition.operator.value} {right}"


def main():
    sql = f"""
    SELECT T1.fname ,  T1.lname FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID  =  T2.advisor WHERE T2.fname  =  "Linda" AND T2.lname  =  "Smith"
    """

    ef = Ast2EfBuilder.build(sql)

    print(ef)


if __name__ == "__main__":
    main()
