import sqlparse

from typing import List


def remove_whitespaces(tokens: List[sqlparse.sql.Token]):
    return list(filter(lambda token: not token.is_whitespace, tokens))
