from sql_to_ast.models.condition import SingleCondition, ConditionLogicalExpression


WhereCondition = SingleCondition | ConditionLogicalExpression


class WhereClause:
    def __init__(self, condition: WhereCondition):
        self.condition = condition

    def __repr__(self):
        return f"Where(condition={self.condition})"
