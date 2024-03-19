import os

from ast_to_ef.ef_code_builder import build_ef_code
from ast_to_ef.schema_mapper import create_schema_map, SchemaMapping
from sql_to_ast.builder.database_schema_builder import build_database_schema, DatabaseSchema

from constants import constants

def find_context_file(database_name: str) -> str:
    folder_path = f"{constants.EF_PROJECT_MODELS_PATH}/{database_name}/"

    for filename in os.listdir(folder_path):
        if filename.endswith("Context.cs"):
            return os.path.join(folder_path, filename)
        
    raise ValueError(f"Context file not found for database: {database_name}")

def main2():
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

    result = build_ef_code(f"""
    SELECT COUNT(*) FROM Faculty GROUP BY building
""", schema)

    #  ORDER BY count(*) DESC LIMIT 1

    print(result)


def main():
    with open(constants.DATASET_TRAIN_PATH, 'r') as file:
        raw_sql = file.readlines()

    database_schemas: dict[str, DatabaseSchema] = dict()
    schema_mappings: dict[str, SchemaMapping] = dict()
    queries = set()

    for line in raw_sql:
        line = line.strip()

        [query, database_name] = line.split('\t')

        if database_name != "activity_1":
            continue

        queries.add((query, database_name))

        database_schema_path = os.path.join(constants.DATASET_TEST_DATABASE_PATH, database_name, 'schema.sql')
        context_file_path = find_context_file(database_name=database_name)

        with open(database_schema_path, 'r') as file:
            database_schema = file.read()
            database_schemas[database_name] = build_database_schema(database_name, database_schema)

        schema_mappings[database_name] = create_schema_map(context_file_path=context_file_path)

    for query, database_name in queries:
        print(f"Processing query: {query} | from database: {database_name}")

        query = f"""
            SELECT COUNT(*)
            FROM Faculty AS f
            JOIN Faculty_Participates_in
            ON f.FacID = Faculty_Participates_in.FacID
        """

        database_schema = database_schemas[database_name]
        schema_mapping = schema_mappings[database_name]

        ef_code = build_ef_code(query, database_schema, schema_mapping)

        print(ef_code)

        input("Next: ")


if __name__ == '__main__':
    main()
