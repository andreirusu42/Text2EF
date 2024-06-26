from collections import defaultdict
import re
from typing import List


class Mapping:
    original_value: str
    mapped_value: str

    def __init__(self, original_value: str, mapped_value: str):
        self.original_value = original_value
        self.mapped_value = mapped_value

    def __repr__(self):
        return f"Mapping(original_value={self.original_value}, mapped_value={self.mapped_value})"


class SchemaMapTable:
    table: Mapping
    columns: defaultdict[str, Mapping]

    def __init__(self, table: Mapping, columns: dict[str, Mapping]):
        self.table = table
        self.columns = columns

    def __repr__(self):
        return f"SchemaMapTable(table={self.table}, columns={self.columns})"


class SchemaMapping:
    tables: defaultdict[str, SchemaMapTable]

    def __init__(self, tables: dict):
        self.tables = tables

    def get_table_name(self, original_table_name: str) -> str:
        return self.tables[original_table_name.lower()].table.mapped_value

    def get_column_name(self, original_table_name: str, original_column_name: str) -> str:
        table = self.tables[original_table_name.lower()]

        print(table)

        return table.columns[original_column_name.lower()]

    def has_table(self, original_table_name: str) -> bool:
        return original_table_name.lower() in self.tables

    def __repr__(self):
        return f"SchemaMapping(tables={self.tables})"


def create_schema_map(context_file_path: str) -> SchemaMapping:
    with open(context_file_path, "r") as file:
        content = file.read()

    tables = {}

    regex = r"public virtual DbSet<(.*)> (.*) { get; set; }"

    matches = re.finditer(regex, content, re.MULTILINE)

    for _, original_column_match in enumerate(matches, start=1):
        original_entity_name = original_column_match.group(1)
        entity_name = original_column_match.group(2)

        tables[original_entity_name.lower()] = entity_name

    regex = r"modelBuilder\.Entity<(.*)>\(\s*entity\s* =>\s*{([\s\S]*?)}\);"
    matches = re.findall(regex, content)

    schema_map_tables = {}

    for original_column_match in matches:
        entity_name = original_column_match[0].lower()
        entity_content = original_column_match[1]

        table_name = re.search(r".ToTable\(\"(.*)\"\);", entity_content).group(1).lower()

        property_occurrences = re.findall(r"entity\s*\.\s*Property\s*\((.*?);", entity_content, re.DOTALL)
        properties = defaultdict()

        has_column_name_pattern = r"HasColumnName\(\"(.+?)\""
        mapped_column_name_pattern = r"e => e\.(\w+)\)"

        for occurrence in property_occurrences:
            original_column_match = re.search(has_column_name_pattern, occurrence)
            mapped_column_match = re.search(mapped_column_name_pattern, occurrence)

            mapped_column_name = mapped_column_match.group(1)
            original_column_name = None

            if original_column_match:
                original_column_name = original_column_match.group(1).lower()
            else:
                original_column_name = mapped_column_name.lower()

            properties[original_column_name] = mapped_column_name if mapped_column_name else original_column_name

        table_mapping = Mapping(original_value=entity_name, mapped_value=tables[entity_name])
        schema_map_table = SchemaMapTable(table=table_mapping, columns=properties)
        schema_map_tables[table_name] = schema_map_table

    return SchemaMapping(tables=schema_map_tables)
