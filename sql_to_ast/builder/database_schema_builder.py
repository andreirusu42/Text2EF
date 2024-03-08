import sqlite3

from sql_to_ast.models.database_schema import DatabaseSchema, DatabaseSchemaTable


def build_database_schema(name: str, schema: str) -> DatabaseSchema:
    conn = sqlite3.connect(':memory:')
    cursor = conn.cursor()

    cursor.executescript(schema)

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    table_names_query_result = cursor.fetchall()

    schema_info = {}

    tables = []

    for table_name in table_names_query_result:
        table_name = table_name[0]

        cursor.execute(f"PRAGMA table_info({table_name});")

        columns = cursor.fetchall()
        columns = [col[1].lower() for col in columns]

        schema_info[table_name] = columns

        table = DatabaseSchemaTable(
            name=table_name,
            columns=columns
        )

        tables.append(table)

    conn.close()

    return DatabaseSchema(
        name=name,
        tables=tables
    )
