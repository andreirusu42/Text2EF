import unittest

from sql_to_ast.builder.database_schema_builder import build_database_schema


class TestDatabaseSchemaBuilder(unittest.TestCase):
    def test_build_schema(self):
        schema = f"""
        CREATE TABLE A (
            id INT,
            name TEXT
        );

        CREATE TABLE B (
            id INT,
            test TEXT
        );
"""
        name = "test"

        result = build_database_schema(name, schema)

        self.assertEqual(result.name, name)
        self.assertEqual(len(result.tables), 2)
        self.assertEqual(result.tables[0].columns, ['id', 'name'])
        self.assertEqual(result.tables[1].columns, ['id', 'test'])


if __name__ == '__main__':
    unittest.main()
