use std::collections::HashMap;
use std::fs;

use indexmap::IndexMap;

#[derive(Debug, Clone, PartialEq)]
pub enum FieldType {
    Int,
    String,
    Bool,
    Double,
    Decimal,
    Date,
    Long,
}

impl FieldType {
    pub fn is_numeric(&self) -> bool {
        match self {
            FieldType::Int => true,
            FieldType::Double => true,
            FieldType::Decimal => true,
            FieldType::Long => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnType {
    Varchar,
    Int,
    Bool,
    Double,
    Decimal,
    None,
    Datetime,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub field_type: FieldType,
    pub is_optional: bool,
    pub column_type: ColumnType,
}

#[derive(Debug)]
pub struct ContextFileColumn {
    pub name: String,
    pub column_type: ColumnType,
}

#[derive(Debug)]
pub struct SchemaMapM2mTableRelation {
    pub table: String,
    pub column: String,
}

#[derive(Debug)]
pub struct SchemaMapTable {
    pub table: String,
    pub columns: IndexMap<String, Column>,
    pub is_m2m: bool,
    pub relations: Option<Vec<SchemaMapM2mTableRelation>>,
    pub parent_table: Option<String>,
}

#[derive(Debug)]
pub struct SchemaMapping {
    pub context: String,
    pub tables: HashMap<String, SchemaMapTable>,
}

fn field_type_to_enum(field_type: &str) -> FieldType {
    match field_type {
        "string" => FieldType::String,
        "int" => FieldType::Int,
        "bool" => FieldType::Bool,
        "double" => FieldType::Double,
        "DateTime" => FieldType::Date,
        "DateOnly" => FieldType::Date,
        "decimal" => FieldType::Decimal,
        "long" => FieldType::Long,
        "short" => FieldType::Int,
        "TimeSpan" => FieldType::Int,
        "byte[]" => FieldType::String,

        _ => panic!("Unknown field type: {}", field_type),
    }
}

fn column_type_to_enum(column_type: &str) -> ColumnType {
    if column_type.is_empty() {
        return ColumnType::None;
    }

    let result = match column_type {
        "datetime" => ColumnType::Datetime,
        "int" => ColumnType::Int,
        "integer" => ColumnType::Int,
        "bit" => ColumnType::Bool,
        "numeric" => ColumnType::Decimal,
        "boolean" => ColumnType::Bool,
        "bool" => ColumnType::Bool,
        "smallint unsigned" => ColumnType::Int,
        "tinyint unsigned" => ColumnType::Int,
        "bigint" => ColumnType::Int,
        "double" => ColumnType::Double,
        "date" => ColumnType::Datetime,
        "timestamp" => ColumnType::Datetime,
        _ => ColumnType::None,
    };

    if result != ColumnType::None {
        return result;
    }

    if column_type.contains("varchar") {
        return ColumnType::Varchar;
    }

    if column_type.contains("char") {
        return ColumnType::Varchar;
    }

    if column_type.contains("float") {
        return ColumnType::Double;
    }

    if column_type.contains("decimal") {
        return ColumnType::Decimal;
    }

    if column_type.contains("numeric") {
        return ColumnType::Decimal;
    }

    if column_type.contains("number") {
        return ColumnType::Decimal;
    }

    if column_type.contains("bigint") {
        return ColumnType::Int;
    }

    if column_type.contains("int") {
        return ColumnType::Int;
    }

    if column_type.contains("year") {
        return ColumnType::Int;
    }

    panic!("Unknown column type: {}", column_type);
}

impl SchemaMapping {
    pub fn get_table(&self, original_table_name: &str) -> Option<&SchemaMapTable> {
        self.tables.get(&original_table_name.to_lowercase())
    }

    pub fn get_table_name(&self, original_table_name: &str) -> Option<&String> {
        self.get_table(original_table_name)
            .map(|table| &table.table)
    }

    pub fn get_table_columns(
        &self,
        original_table_name: &str,
    ) -> Option<&IndexMap<String, Column>> {
        let columns = self
            .get_table(original_table_name)
            .map(|table| &table.columns);

        return columns;
    }

    pub fn get_column(
        &self,
        original_table_name: &str,
        original_column_name: &str,
    ) -> Option<&Column> {
        let column = self
            .get_table(original_table_name)
            .and_then(|table| table.columns.get(&original_column_name.to_lowercase()));

        return column;
    }

    pub fn get_column_name(
        &self,
        original_table_name: &str,
        original_column_name: &str,
    ) -> Option<&String> {
        let column = self.get_column(original_table_name, original_column_name);

        return column.map(|column| &column.name);
    }

    pub fn get_other_relation_column(
        &self,
        table: &SchemaMapTable,
        table_name: &str,
    ) -> Option<String> {
        if let Some(relations) = &table.relations {
            for relation in relations {
                let is_table = relation.table == table_name.to_lowercase();
                let is_column = relation.column.to_lowercase() == table_name.to_lowercase();

                if !is_table && !is_column {
                    return Some(relation.column.to_string());
                }
            }
        }

        None
    }
    pub fn get_relation_column(&self, table: &SchemaMapTable, table_name: &str) -> Option<String> {
        let lower_table_name = table_name.to_lowercase();

        if let Some(relations) = &table.relations {
            for relation in relations {
                if relation.table.to_lowercase() == lower_table_name {
                    return Some(relation.column.to_string());
                }

                // TODO: This doesn't make sense???
                if relation.column.to_lowercase() == lower_table_name {
                    return Some(relation.column.to_string());
                }
            }
        }

        None
    }

    // pub fn has_table(&self, original_table_name: &str) -> bool {
    //     self.tables
    //         .contains_key(&original_table_name.to_lowercase())
    // }
}

fn find_context_cs_file(folder_path: &str) -> Option<String> {
    let entries = match fs::read_dir(folder_path) {
        Ok(entries) => entries,
        Err(_) => return None,
    };

    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };

        let path = entry.path();
        let path = match path.to_str() {
            Some(path) => path,
            None => continue,
        };

        if path.to_lowercase().ends_with("context.cs") {
            return Some(path.to_string());
        }
    }

    None
}

pub fn extract_context_and_models(model_folder_path: &str) -> (String, Vec<String>) {
    let context_file_path = find_context_cs_file(model_folder_path).expect(&format!(
        "Context file not found for the model {}",
        model_folder_path
    ));

    let context_content =
        fs::read_to_string(context_file_path).expect("Failed to read the context file");

    let entity_regex = regex::Regex::new(
        r"modelBuilder\.Entity<(\w+)>\(\s*entity\s*=>\s*\{((?:\{[^{}]*\}|[^{}])*)\}\);",
    )
    .unwrap();

    let mut models: Vec<String> = Vec::new();

    for cap in entity_regex.captures_iter(&context_content) {
        let entity_name = &cap[1];

        let model_file_path = format!("{}/{}.cs", model_folder_path, entity_name);
        let model_content =
            fs::read_to_string(model_file_path).expect("Failed to read the model file");

        let re = regex::Regex::new(r"public\s+partial\s+class\s+\w+\s*\{(?:[^{}]*|\{[^{}]*\})*\}")
            .unwrap();

        if let Some(mat) = re.find(&model_content) {
            let content = &model_content[mat.start()..mat.end()];
            models.push(content.to_string());
        } else {
            panic!("Failed to find the model content");
        }
    }

    return (context_content, models);
}

pub fn create_schema_map(model_folder_path: &str) -> SchemaMapping {
    let context_file_path = find_context_cs_file(model_folder_path).expect(&format!(
        "Context file not found for the model {}",
        model_folder_path
    ));

    let context_content =
        fs::read_to_string(context_file_path).expect("Failed to read the context file");

    let context_name_regex = regex::Regex::new(r"public partial class (.*) : DbContext").unwrap();
    let context_name = context_name_regex
        .captures(&context_content)
        .expect("Failed to find the context name")
        .get(1)
        .unwrap()
        .as_str()
        .to_string();

    let mut tables = HashMap::new();
    let mut schema_map_tables = HashMap::new();

    let table_regex =
        regex::Regex::new(r"public virtual DbSet<(.*)> (.*) \{ get; set; \}").unwrap();
    let entity_regex = regex::Regex::new(
        r"modelBuilder\.Entity<(\w+)>\(\s*entity\s*=>\s*\{((?:\{[^{}]*\}|[^{}])*)\}\);",
    )
    .unwrap();

    let model_property_regex = regex::Regex::new(r"public (.*) (.*) \{ get; set; \}").unwrap();
    let context_property_regex = regex::Regex::new(r"entity\s*\.\s*Property\s*\(([^;]+);").unwrap();
    let mapped_column_name_pattern: regex::Regex = regex::Regex::new(r"e => e\.(\w+)\)").unwrap();
    let original_table_name_regex = regex::Regex::new(r#".ToTable\(\"(.*)\"\);"#).unwrap();
    let has_column_name_regex = regex::Regex::new(r#"HasColumnName\(\"(.+?)\""#).unwrap();
    let has_column_type_regex = regex::Regex::new(r#"HasColumnType\(\"(.+?)\""#).unwrap();
    let m2m_table_regex = regex::Regex::new(
        r#"(?s)entity\s*\.HasMany\(d => d\.(.*?)\)\.WithMany\(p => p\.(.*?)\)(.*?)r => r\.HasOne<(.*?)>(.*)l => l\.HasOne<(.*?)>(.*)\}\);"#,
    )
    .unwrap();

    let m2m_table_and_alias_regex =
        regex::Regex::new(r#"\.UsingEntity<.*>\(\s*\"(.*)\"[\s\S]*?.ToTable\(\"(.*)\""#).unwrap();
    let join_table_fields_regex = regex::Regex::new(
        r#"\.IndexerProperty<([^>]*)>\s*\("([^"]*)"\)\s*(?:\.HasColumnType\("([^"]*)"\)\s*)?\.HasColumnName\("([^"]*)"\);"#,
    )
    .unwrap();

    for cap in table_regex.captures_iter(&context_content) {
        let original_entity_name = cap[1].to_lowercase();
        let entity_name = cap[2].to_string();

        tables.insert(original_entity_name, entity_name);
    }

    for cap in entity_regex.captures_iter(&context_content) {
        let mut model_file_properties: IndexMap<String, Column> = IndexMap::new();
        let mut context_file_mapping: HashMap<String, ContextFileColumn> = HashMap::new();
        let mut columns: IndexMap<String, Column> = IndexMap::new();

        let entity_name = &cap[1];
        let entity_content = &cap[2];

        let model_file_path = format!("{}/{}.cs", model_folder_path, entity_name);
        let model_content =
            fs::read_to_string(model_file_path).expect("Failed to read the model file");

        let original_table_name_capture = original_table_name_regex.captures(entity_content);

        let original_table_name = if let Some(original) = original_table_name_capture {
            original[1].to_lowercase()
        } else {
            tables[&entity_name.to_lowercase()].to_lowercase()
        };

        // TODO: you need to correct the columns, the names should be based on the primary key in the related table!
        // This is the m2m table case
        for m2m_table_content_capture in m2m_table_regex.captures_iter(entity_content) {
            let m2m_table_content = m2m_table_content_capture.get(0).unwrap().as_str();

            let m2m_table_and_alias = m2m_table_and_alias_regex
                .captures(m2m_table_content)
                .unwrap();

            let mapped_table_name = m2m_table_and_alias.get(1).unwrap().as_str();
            let table_name = m2m_table_and_alias.get(2).unwrap().as_str();

            let mut columns: IndexMap<String, Column> = IndexMap::new();

            let m2m_table_fields = join_table_fields_regex.captures_iter(m2m_table_content);

            let has_many_column = m2m_table_content_capture.get(1).unwrap().as_str();
            let with_many_column = m2m_table_content_capture.get(2).unwrap().as_str();
            let has_many_table = m2m_table_content_capture.get(4).unwrap().as_str();
            let with_many_table = m2m_table_content_capture.get(6).unwrap().as_str();

            for (index, field) in m2m_table_fields.enumerate() {
                let field_type = field.get(1).unwrap().as_str();
                let field_name = field.get(2).unwrap().as_str();
                let column_name = field.get(4).unwrap().as_str();

                columns.insert(
                    column_name.to_lowercase(),
                    Column {
                        name: field_name.to_string(),
                        field_type: field_type_to_enum(field_type),
                        is_optional: false,
                        column_type: ColumnType::None,
                    },
                );
            }

            let mut relations: Vec<SchemaMapM2mTableRelation> = Vec::new();
            relations.push(SchemaMapM2mTableRelation {
                table: has_many_table.to_string(),
                column: has_many_column.to_string(),
            });
            relations.push(SchemaMapM2mTableRelation {
                table: with_many_table.to_string(),
                column: with_many_column.to_string(),
            });

            let schema_map_table = SchemaMapTable {
                table: mapped_table_name.to_string(),
                columns,
                is_m2m: true,
                relations: Some(relations),
                parent_table: Some(original_table_name.to_string()),
            };

            schema_map_tables.insert(table_name.to_string().to_lowercase(), schema_map_table);
        }

        let context_property_occurrences: Vec<&str> = context_property_regex
            .captures_iter(entity_content)
            .map(|cap| cap.get(1).unwrap().as_str())
            .collect();
        for occurrence in context_property_occurrences {
            let mapped_column_match = mapped_column_name_pattern.captures(occurrence).unwrap();
            let mapped_column_name = mapped_column_match[1].to_string();

            let original_column_name = has_column_name_regex
                .captures(occurrence)
                .map(|original_column_match| original_column_match[1].to_string())
                .unwrap_or_else(|| mapped_column_name.to_lowercase());

            let column_type = has_column_type_regex
                .captures(occurrence)
                .map(|column_type_match| column_type_match[1].to_lowercase().to_string())
                .unwrap_or("".to_string());

            context_file_mapping.insert(
                mapped_column_name.to_lowercase(),
                ContextFileColumn {
                    name: original_column_name,
                    column_type: column_type_to_enum(column_type.as_str()),
                },
            );
        }

        let model_property_occurrences = model_property_regex.captures_iter(&model_content);
        for occurrence in model_property_occurrences {
            let field_type_with_qm = &occurrence[1];
            let field_type = field_type_with_qm.trim_end_matches('?');
            let is_optional = field_type_with_qm.ends_with('?');

            let field_name = &occurrence[2];

            if field_type.contains("virtual") {
                continue;
            }

            if !model_file_properties.contains_key(&field_name.to_lowercase()) {
                model_file_properties.insert(
                    field_name.to_lowercase(),
                    Column {
                        name: field_name.to_string(),
                        field_type: field_type_to_enum(field_type),
                        is_optional,
                        column_type: ColumnType::None,
                    },
                );
            }
        }

        // The key in model_file_properties is the mapped column name, lowercased
        // The value in model_file_properties' Column is the mapped column name, original

        // The key in context_file_mapping is the mapped column name, lowercased
        // The value in context_file_mapping's ContextFileColumn is the original column name

        // Hence, key(model_file_property) = value(context_file_mapping).name

        for (key, column) in &model_file_properties {
            if let Some(mapped_column) = context_file_mapping.get(key) {
                columns.insert(
                    mapped_column.name.to_lowercase(),
                    Column {
                        name: column.name.clone(),
                        field_type: column.field_type.clone(),
                        is_optional: column.is_optional,
                        column_type: mapped_column.column_type.clone(),
                    },
                );
            } else {
                columns.insert(
                    key.to_string(),
                    Column {
                        name: column.name.clone(),
                        field_type: column.field_type.clone(),
                        is_optional: column.is_optional,
                        column_type: column.column_type.clone(),
                    },
                );
            }
        }

        // println!("Model file properties: {:?}", model_file_properties);
        // println!("Context file mapping: {:?}", context_file_mapping);
        // println!("Columns: {:?}", columns);

        let schema_map_table = SchemaMapTable {
            table: tables[&entity_name.to_lowercase()].clone(),
            columns,
            relations: None,
            parent_table: None,
            is_m2m: false,
        };

        schema_map_tables.insert(original_table_name.to_string(), schema_map_table);
    }

    SchemaMapping {
        context: context_name,
        tables: schema_map_tables,
    }
}
