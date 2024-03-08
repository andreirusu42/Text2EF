import os

from ast_to_ef.ef_code_builder import build_ef_code
from sql_to_ast.builder.database_schema_builder import build_database_schema

from constants import constants


def main2():
    schema = build_database_schema('mydb', f"""
        CREATE TABLE Table1 (
            a INT,
            b INT
        );

        CREATE TABLE Table2 (
            a INT,
            c INT,
            d INT
        );
    """)

    result = build_ef_code(f"""
    SELECT count(a)
    FROM Table1 JOIN table2 AS t2 ON Table1.a = t2.a
""", schema)

    print(result)


def main():
    with open(constants.DATASET_TRAIN_PATH, 'r') as file:
        raw_sql = file.readlines()

    database_schemas = dict()
    queries = set()

    for line in raw_sql:
        line = line.strip()

        [query, database_name] = line.split('\t')

        if database_name != "activity_1":
            continue

        queries.add((query, database_name))

        database_schema_path = os.path.join(constants.DATASET_TEST_DATABASE_PATH, database_name, 'schema.sql')

        with open(database_schema_path, 'r') as file:
            database_schema = file.read()

            database_schemas[database_name] = build_database_schema(database_name, database_schema)

    for query, database_name in queries:
        print(f"Processing query: {query} | from database: {database_name}")

        database_schema = database_schemas[database_name]

        ef_code = build_ef_code(query, database_schema)

        print(ef_code)

        input("Next: ")


if __name__ == '__main__':
    main()
