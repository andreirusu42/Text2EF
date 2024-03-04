import unittest

from adapter.where_clause import build_where


class TestWhereClauseAdapter(unittest.TestCase):
    def test_where_clause_adapter(self):
        build_where(None)


if __name__ == '__main__':
    unittest.main()
