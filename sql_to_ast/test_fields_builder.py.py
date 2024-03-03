import unittest
import sqlparse

from sqlparse.sql import Token

from builder.select import get_fields

from models import condition


class TestFieldsBuilder(unittest.TestCase):
    def get_token(self, sql: str) -> Token:
        return sqlparse.parse(sql)[0].tokens[0]

    def get_fields(self, sql: str):
        token = self.get_token(sql)

        return get_fields(token)


if __name__ == '__main__':
    unittest.main()
