import os

from ast_to_ef.ef_code_builder import build_ef_code
from ast_to_ef.schema_mapper import create_schema_map, SchemaMapping
from sql_to_ast.builder.database_schema_builder import build_database_schema, DatabaseSchema
from sql_to_ast.select_ast_builder import build_select_ast
from constants import constants


def find_context_file(database_name: str) -> str:
    folder_path = f"{constants.EF_PROJECT_MODELS_PATH}/{database_name}/"

    print((folder_path,))

    for filename in os.listdir(folder_path):
        if filename.endswith("Context.cs"):
            return os.path.join(folder_path, filename)

    raise ValueError(f"Context file not found for database: {database_name}")


def main():
    schema = build_database_schema('mydb', f"""
       create table Activity (
  actid INTEGER PRIMARY KEY,
  activity_name varchar(25)
);

create table Participates_in (
  stuid INTEGER,
  actid INTEGER,
  FOREIGN KEY(stuid) REFERENCES Student(StuID),
  FOREIGN KEY(actid) REFERENCES Activity(actid)
);

create table Faculty_Participates_in (
  FacID INTEGER,
  actid INTEGER,
  FOREIGN KEY(FacID) REFERENCES Faculty(FacID),
  FOREIGN KEY(actid) REFERENCES Activity(actid)
);

create table Student (
        StuID        INTEGER PRIMARY KEY,
        LName        VARCHAR(12),
        Fname        VARCHAR(12),
        Age      INTEGER,
        Sex      VARCHAR(1),
        Major        INTEGER,
        Advisor      INTEGER,
        city_code    VARCHAR(3)
 );

create table Faculty (
       FacID 	       INTEGER PRIMARY KEY,
       Lname		VARCHAR(15),
       Fname		VARCHAR(15),
       Rank		VARCHAR(15),
       Sex		VARCHAR(1),
       Phone		INTEGER,
       Room		VARCHAR(5),
       Building		VARCHAR(13)
);
    """)

    query = "SELECT fname, T1.lname FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID = T2.advisor WHERE T2.fname = \"Linda\" AND T2.lname = \"Smith\""

    schema_mapping = create_schema_map("./entity-framework/Models/activity_1/Activity1Context.cs")
    result = build_ef_code(query, schema, schema_mapping)

    #  ORDER BY count(*) DESC LIMIT 1

    print(result)


def main2():
    with open(constants.DATASET_TRAIN_PATH, 'r') as file:
        raw_sql = file.readlines()

    database_schemas: dict[str, DatabaseSchema] = dict()
    schema_mappings: dict[str, SchemaMapping] = dict()
    queries = set()

    for line in raw_sql:
        line = line.strip()

        [query, database_name] = line.split('\t')

        queries.add((query, database_name))

        if database_name != 'activity_1':
            continue

        database_schema_path = os.path.join(constants.DATASET_TEST_DATABASE_PATH, database_name, 'schema.sql')
        context_file_path = find_context_file(database_name=database_name)

        with open(database_schema_path, 'r') as file:
            database_schema = file.read()
        database_schemas[database_name] = build_database_schema(database_name, database_schema)

        schema_mappings[database_name] = create_schema_map(context_file_path=context_file_path)

    mapping = schema_mappings['activity_1']

    queries = list(queries)
    queries = sorted(queries, key=lambda x: x[0])

    print(len(queries))

    query = "SELECT fname, T1.lname FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID = T2.advisor WHERE T2.fname = \"Linda\" AND T2.lname = \"Smith\""

    ast = build_select_ast(query)

    print(ast)

    # for __index, (query, database_name) in enumerate(queries):
    #     print(f"Processing query: {query} | from database: {database_name}")

    #     print(f"{__index} / {len(queries)}")

    #     # database_schema = database_schemas[database_name]
    #     # schema_mapping = schema_mappings[database_name]

    #     ast = build_select_ast(query)

    #     # print(query)
    #     # print(ast)


if __name__ == '__main__':
    main()
