from enum import Enum
from sql_to_ast.builder import where_clause_builder
from sql_to_ast.models import field, condition, select_ast

from ast_to_ef.transformers.constants import SELECTOR


class ConditionBinaryLogicalOperatorToEF(Enum):
    AND = "&&"
    OR = "||"

    @staticmethod
    def from_condition_logical_operator(operator: condition.ConditionBinaryLogicalOperator):
        if operator == condition.ConditionBinaryLogicalOperator.AND:
            return ConditionBinaryLogicalOperatorToEF.AND.value
        elif operator == condition.ConditionBinaryLogicalOperator.OR:
            return ConditionBinaryLogicalOperatorToEF.OR.value
        else:
            raise ValueError(f"Invalid operator {operator}")


class ConditionOperatorToEF(Enum):
    EQUAL = "=="
    LT = "<"
    GT = ">"
    LTE = "<="
    GTE = ">="
    LIKE = "LIKE"
    IN = "IN"

    @staticmethod
    def from_condition_operator(operator: condition.ConditionOperator):
        if operator == condition.ConditionOperator.EQUAL:
            return ConditionOperatorToEF.EQUAL.value
        elif operator == condition.ConditionOperator.LT:
            return ConditionOperatorToEF.LT.value
        elif operator == condition.ConditionOperator.GT:
            return ConditionOperatorToEF.GT.value
        elif operator == condition.ConditionOperator.LTE:
            return ConditionOperatorToEF.LTE.value
        elif operator == condition.ConditionOperator.GTE:
            return ConditionOperatorToEF.GTE.value
        elif operator == condition.ConditionOperator.LIKE:
            return ConditionOperatorToEF.LIKE.value
        elif operator == condition.ConditionOperator.IN:
            return ConditionOperatorToEF.IN.value
        else:
            raise ValueError("Invalid operator")


def build_where(where_clause: where_clause_builder.WhereClause):
    where_condition = where_clause.condition

    return f".Where({SELECTOR} => {__build_where_helper(where_condition)})"


def __build_where_field(field: field.Field):
    return f"{SELECTOR}.{field.parent + '.' if field.parent else ''}{field.name}"


def __build_where_string_operand(operand: condition.StringOperand):
    return f'"{operand.value}"'


def __build_where_int_operand(operand: condition.IntOperand):
    return f'{operand.value}'


def __build_where_float_operand(operand: condition.FloatOperand):
    return f'{operand.value}'


def __build_where_condition_operand(operand: condition.ConditionOperand):
    from ast_to_ef.ef_code_builder import build_ef_code_from_select_ast

    if isinstance(operand, field.Field):
        return __build_where_field(operand)
    elif isinstance(operand, condition.StringOperand):
        return __build_where_string_operand(operand)
    elif isinstance(operand, condition.IntOperand):
        return __build_where_int_operand(operand)
    elif isinstance(operand, condition.FloatOperand):
        return __build_where_float_operand(operand)
    elif isinstance(operand, select_ast.SelectAst):
        return build_ef_code_from_select_ast(operand)
    else:
        raise ValueError(f"Unsupported operand: {operand}")


def __build_where_helper(where_condition: where_clause_builder.WhereCondition):
    if isinstance(where_condition, condition.SingleCondition):
        left = __build_where_condition_operand(where_condition.left_operand)
        right = __build_where_condition_operand(where_condition.right_operand)

        return f"""{left} {ConditionOperatorToEF.from_condition_operator(
            where_condition.operator
        )} {right}"""

    elif isinstance(where_condition, condition.ConditionBinaryLogicalExpression):
        left = __build_where_helper(where_condition.left_operand)
        right = __build_where_helper(
            where_condition.right_operand)

        if isinstance(where_condition.left_operand, condition.ConditionLogicalExpression):
            left = f"({left})"

        if isinstance(where_condition.right_operand, condition.ConditionLogicalExpression):
            right = f"({right})"

        return f"""{left} {ConditionBinaryLogicalOperatorToEF.from_condition_logical_operator(
            where_condition.operator
        )} {right}"""

    elif isinstance(where_condition, condition.ConditionUnaryLogicalExpression):
        operand = __build_where_helper(where_condition.operand)

        return f"!{operand}"

    elif isinstance(where_condition, select_ast.SelectAst):
        return f"{SELECTOR} => {where_condition}"

    else:
        raise ValueError(f"Unsupported condition: {where_condition}")
