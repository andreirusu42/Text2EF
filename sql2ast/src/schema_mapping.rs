use std::collections::HashMap;
use std::fs;

#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub field_type: String,
    pub is_optional: bool,
}

#[derive(Debug)]
pub struct SchemaMapTable {
    pub table: String,
    pub columns: HashMap<String, Column>,
}

#[derive(Debug)]
pub struct SchemaMapping {
    pub context: String,
    pub tables: HashMap<String, SchemaMapTable>,
}

impl SchemaMapping {
    pub fn get_table_name(&self, original_table_name: &str) -> Option<&String> {
        self.tables
            .get(&original_table_name.to_lowercase())
            .map(|table| &table.table)
    }

    pub fn get_table_columns(&self, original_table_name: &str) -> Option<&HashMap<String, Column>> {
        let columns = self
            .tables
            .get(&original_table_name.to_lowercase())
            .map(|table| &table.columns);

        return columns;
    }

    pub fn get_column(
        &self,
        original_table_name: &str,
        original_column_name: &str,
    ) -> Option<&Column> {
        let column = self
            .tables
            .get(&original_table_name.to_lowercase())
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

    for cap in table_regex.captures_iter(&context_content) {
        let original_entity_name = cap[1].to_lowercase();
        let entity_name = cap[2].to_string();

        tables.insert(original_entity_name, entity_name);
    }

    for cap in entity_regex.captures_iter(&context_content) {
        let mut model_file_properties: HashMap<String, Column> = HashMap::new();
        let mut context_file_mapping: HashMap<String, String> = HashMap::new();
        let mut columns: HashMap<String, Column> = HashMap::new();

        let entity_name = &cap[1];
        let entity_content = &cap[2];

        let model_file_path = format!("{}/{}.cs", model_folder_path, entity_name);
        let model_content =
            fs::read_to_string(model_file_path).expect("Failed to read the model file");

        let model_property_regex = regex::Regex::new(r"public (.*) (.*) \{ get; set; \}").unwrap();
        let context_property_regex =
            regex::Regex::new(r"entity\s*\.\s*Property\s*\(([^;]+);").unwrap();
        let mapped_column_name_pattern: regex::Regex =
            regex::Regex::new(r"e => e\.(\w+)\)").unwrap();
        let original_table_name_regex = regex::Regex::new(r#".ToTable\(\"(.*)\"\);"#).unwrap();
        let has_column_name_regex = regex::Regex::new(r#"HasColumnName\(\"(.+?)\""#).unwrap();

        let original_table_name_capture = original_table_name_regex.captures(entity_content);

        let original_table_name = if let Some(original) = original_table_name_capture {
            original[1].to_lowercase()
        } else {
            tables[&entity_name.to_lowercase()].to_lowercase()
        };

        let context_property_occurrences: Vec<&str> = context_property_regex
            .captures_iter(entity_content)
            .map(|cap| cap.get(1).unwrap().as_str())
            .collect();
        for occurrence in context_property_occurrences {
            let mapped_column_match = mapped_column_name_pattern.captures(occurrence).unwrap();
            let mapped_column_name = mapped_column_match[1].to_string();

            let original_column_name = has_column_name_regex
                .captures(occurrence)
                .map(|original_column_match| original_column_match[1].to_lowercase())
                .unwrap_or_else(|| mapped_column_name.to_lowercase());

            context_file_mapping.insert(original_column_name, mapped_column_name.to_lowercase());
        }

        let model_property_occurrences = model_property_regex.captures_iter(&model_content);
        for occurrence in model_property_occurrences {
            let field_type_with_qm = &occurrence[1];
            let field_type = field_type_with_qm.trim_end_matches('?');
            let is_optional = field_type_with_qm.ends_with('?');

            let field_name = &occurrence[2];

            // TODO: we'll sometime have to do something with the virtual fields (which are relations basically)

            if !model_file_properties.contains_key(&field_name.to_lowercase()) {
                model_file_properties.insert(
                    field_name.to_lowercase(),
                    Column {
                        name: field_name.to_string(),
                        field_type: field_type.to_string(),
                        is_optional,
                    },
                );
            }
        }

        for (original_column_name, mapped_column_name) in context_file_mapping {
            if let Some(property) = model_file_properties.get(&mapped_column_name) {
                columns.insert(
                    original_column_name,
                    Column {
                        name: property.name.clone(),
                        field_type: property.field_type.clone(),
                        is_optional: property.is_optional,
                    },
                );
                model_file_properties.remove(&mapped_column_name);
            }
        }
        for (column_name, property) in model_file_properties {
            columns.insert(
                column_name,
                Column {
                    name: property.name,
                    field_type: property.field_type,
                    is_optional: property.is_optional,
                },
            );
        }

        let schema_map_table = SchemaMapTable {
            table: tables[&entity_name.to_lowercase()].clone(),
            columns,
        };
        schema_map_tables.insert(original_table_name, schema_map_table);
    }

    SchemaMapping {
        context: context_name,
        tables: schema_map_tables,
    }
}
