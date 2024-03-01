import unittest
import sqlparse

from sqlparse.sql import Token

from builder import _get_conditions


class TestBuilder(unittest.TestCase):
    def get_token(self, sql: str) -> Token:
        return sqlparse.parse(sql)[0].tokens[0]

    def test_get_conditions(self):
        sql = "WHERE condition = 'rain' OR condition = 'snow' AND condition = 'sunny'"
        token = self.get_token(sql)

        conditions = _get_conditions(token)

        print(conditions)


if __name__ == '__main__':
    unittest.main()
