import sqlparse

from typing import List

from sqlparse.sql import TokenList, Identifier, Statement, Token, IdentifierList, Function, Comparison, Where
from condition import Condition, ConditionOperator
from field import Field
from join import Join, JoinCondition, JoinConditionOperator

from select_statement import SelectStatementBuilder, SqlSelectStatement
from table import Table


def _get_conditions(token: Token):
    cleaned: List[Identifier] = list(
        filter(lambda token: not token.is_whitespace, token.tokens)
    )[1:]

    conditions = []

    for token in cleaned:
        # TODO: handle or :))) lol
        if token.ttype == sqlparse.tokens.Keyword and token.value == 'AND':
            conditions.append("AND")
            continue

        if token.ttype == sqlparse.tokens.Keyword and token.value == 'OR':
            conditions.append("OR")
            continue

        # check if token is a comparison

        print(token.ttype, token.value)

        if token.ttype == sqlparse.tokens.Comparison:
            print((token, 1))

            for a in token.tokens:
                print((a, 2))

        # if isinstance(token, Comparison):
        #     cleaned: List[Identifier] = list(
        #         filter(lambda token: not token.is_whitespace, token.tokens))

        #     if len(cleaned) != 3:
        #         raise ValueError(
        #             f"Expected 3 tokens, got: {len(cleaned)}")

        #     [left, operator, right] = cleaned
        #     # print((left, operator, right))
        #     # print(left.ttype, operator.ttype, right.ttype)
        #     # print(type(left), type(operator), type(right))

        #     if not isinstance(left, Identifier) \
        #         or not (isinstance(operator, Token) and operator.ttype == sqlparse.tokens.Comparison) \
        #             or not (isinstance(right, Identifier) or right.ttype == sqlparse.tokens.String.Single):
        #         raise ValueError(
        #             f"Expected identifier comparison identifier, got: {left}, {operator}, {right}")

        #     condition = Condition(
        #         field=Field(
        #             name=left.get_real_name(),
        #             alias=left.get_alias(),
        #             parent=left.get_parent_name()
        #         ),
        #         operator=ConditionOperator.from_string(
        #             operator.value),
        #         value=right.value
        #     )

        #     conditions.append(condition)

    return conditions


# This only works for select statements :)
class Sql2AstBuilder:
    __token_index: int
    __sql: str
    __statement: Statement
    __tokens: List[TokenList | Token | IdentifierList | Identifier]
    __tree: SqlSelectStatement

    def __init__(self, sql: str):
        statements = sqlparse.parse(sql)

        if len(statements) != 1:
            raise ValueError('SQL should contain only one statement')

        self.__sql = sql
        self.__token_index = 0
        self._tree = None
        self.__statement = statements[0]
        self.__tokens = self.__statement.tokens

    def _get_token(self):
        return self.__tokens[self.__token_index]

    def _get_next_token(self, skip_current=True):
        if skip_current:
            self.__token_index += 1

        while self.__token_index < len(self.__tokens) and self._get_token().is_whitespace:
            self.__token_index += 1

        return self.__tokens[self.__token_index]

    def _get_conditions(self, token: Token):
        cleaned: List[Identifier] = list(
            filter(lambda token: not token.is_whitespace, token.tokens)
        )[1:]

        print((cleaned, 1))

    def build(self):
        select_statement_builder = SelectStatementBuilder()

        token = self._get_next_token(skip_current=False)

        # Find the SELECT
        if token.ttype == sqlparse.tokens.DML:
            right = token.value.upper()

            if right != 'SELECT':
                raise ValueError(f'Unsupported DML: {right}')

        token = self._get_next_token()

        # After Select, we can have IdentifierList - we're selecting the fields, or a Function
        if isinstance(token, IdentifierList):
            identifiers: List[Identifier |
                              Comparison | Function] = token.get_identifiers()

            for identifier in identifiers:
                if isinstance(identifier, Identifier):
                    field = Field(name=identifier.get_real_name(),
                                  alias=identifier.get_alias(),
                                  parent=identifier.get_parent_name())

                    select_statement_builder.add_field(field)

                # TODO
                else:
                    raise ValueError(f'Unsupported identifier: {identifier}')
        elif token.ttype == sqlparse.tokens.Wildcard:
            select_statement_builder.set_wildcard()
            pass
        # TODO
        else:
            raise ValueError(f'Unsupported token: {token}')

        token = self._get_next_token()

        # Expecting FROM here
        if token.ttype == sqlparse.tokens.Keyword:
            if token.value != 'FROM':
                raise ValueError(f'Expecting FROM, got: {token.value}')

        token = self._get_next_token()

        if isinstance(token, Identifier):
            table = Table(
                name=token.get_real_name(),
                alias=token.get_alias()
            )

            select_statement_builder.set_table(table)

        else:
            raise ValueError(f'Unsupported token: {token}')

        token = self._get_next_token()

        # TODO: here you might have joins
        if token.ttype == sqlparse.tokens.Keyword:
            if token.value == 'JOIN':
                token = self._get_next_token()

                if not isinstance(token, Identifier):
                    raise ValueError(f"Expected identifier, got: {token}")

                table = Table(
                    name=token.get_real_name(),
                    alias=token.get_alias()
                )

                condition = None

                token = self._get_next_token()

                if token.ttype == sqlparse.tokens.Keyword and token.value == 'ON':
                    token = self._get_next_token()

                    if not isinstance(token, Comparison):
                        raise ValueError(f"Expected comparison, got: {token}")

                    cleaned: List[Identifier] = list(
                        filter(lambda token: not token.is_whitespace, token.tokens))

                    if len(cleaned) != 3:
                        raise ValueError(
                            f"Expected 3 tokens, got: {len(cleaned)}")

                    [left, operator, right] = cleaned

                    condition = JoinCondition(
                        left=Field(
                            name=left.get_real_name(),
                            alias=left.get_alias(),
                            parent=left.get_parent_name()
                        ),
                        operator=JoinConditionOperator.from_string(
                            operator.value),
                        right=Field(
                            name=right.get_real_name(),
                            alias=right.get_alias(),
                            parent=right.get_parent_name()
                        )
                    )

                join = Join(table, condition)

                select_statement_builder.add_join(join)

                # print((token, 5))

                # print(token.get_real_name(), token.get_alias())

            token = self._get_next_token()

        if not isinstance(token, Where):
            raise Exception(f"Expected WHERE, got {token}")

        cleaned: List[Identifier] = list(
            filter(lambda token: not token.is_whitespace, token.tokens)
        )[1:]

        print((cleaned, 1))

        for token in cleaned:
            # TODO: handle or :))) lol
            if isinstance(token, Token) and token.ttype == sqlparse.tokens.Keyword and token.value == 'AND':
                print("AND")
                continue

            if isinstance(token, Token) and token.ttype == sqlparse.tokens.Keyword and token.value == 'OR':
                print("OR")
                continue

            if isinstance(token, Comparison):
                cleaned: List[Identifier] = list(
                    filter(lambda token: not token.is_whitespace, token.tokens))

                if len(cleaned) != 3:
                    raise ValueError(
                        f"Expected 3 tokens, got: {len(cleaned)}")

                [left, operator, right] = cleaned
                # print((left, operator, right))
                # print(left.ttype, operator.ttype, right.ttype)
                # print(type(left), type(operator), type(right))

                if not isinstance(left, Identifier) \
                    or not (isinstance(operator, Token) and operator.ttype == sqlparse.tokens.Comparison) \
                        or not (isinstance(right, Identifier) or right.ttype == sqlparse.tokens.String.Single):
                    raise ValueError(
                        f"Expected identifier comparison identifier, got: {left}, {operator}, {right}")

                condition = Condition(
                    field=Field(
                        name=left.get_real_name(),
                        alias=left.get_alias(),
                        parent=left.get_parent_name()
                    ),
                    operator=ConditionOperator.from_string(
                        operator.value),
                    value=right.value
                )

                select_statement_builder.add_condition(condition)

        select_statement = select_statement_builder.build()

        return select_statement


def main():
    # sql = f"""SELECT fname, lname FROM Faculty WHERE Rank = "Instructor" OR Rank = "ABC" AND fname = "X" """  # IdentifierList second
    # sql = f"""SELECT f.fname as first_name, f.lname as last_name FROM Faculty AS f WHERE f.Rank = "Instructor" """  # IdentifierList second
    # sql = f"""SELECT AVG(*) FROM Faculty WHERE Rank = "Instructor" """  # Function second
    # sql = f"""SELECT name AS x FROM Faculty;"""
    #     sql = f"""SELECT CompanyName as C,
    #        ProductCount = (SELECT COUNT(P.id)
    #                          FROM [Product] P
    #                         WHERE P.SupplierId = S.Id)
    #   FROM Supplier S"""

    # sql = f"""
    # SELECT T1.fname ,  T1.lname FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID  =  T2.advisor WHERE T2.fname  =  "Linda" AND T2.lname  =  "Smith"
    # """

    # sql = f"""
    # SELECT T1.fname ,  T1.lname FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID  =  T2.advisor WHERE T2.fname  =  "Linda" AND T2.lname  =  "Smith"
    # """

    sql = f"""SELECT * FROM table1 WHERE column1 = 'value1' AND column2 = "value2" """

    builder = Sql2AstBuilder(sql)

    statement = builder.build()

    print(statement.conditions)


if __name__ == '__main__':
    main()
