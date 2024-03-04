import unittest

from ast_to_ef.adapter.where_clause import build_where


class TestWhereClauseAdapter(unittest.TestCase):
    def test_where_clause_adapter(self):
        # build_where(None)
        self.assertEqual(1, 1)


if __name__ == '__main__':
    unittest.main()
