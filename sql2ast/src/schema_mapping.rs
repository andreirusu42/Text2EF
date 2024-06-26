use std::collections::HashMap;
use std::fs;

#[derive(Debug)]
pub struct SchemaMapTable {
    pub table: String,
    pub columns: HashMap<String, String>,
}

#[derive(Debug)]
pub struct SchemaMapping {
    pub tables: HashMap<String, SchemaMapTable>,
}

impl SchemaMapping {
    pub fn get_table_name(&self, original_table_name: &str) -> Option<&String> {
        self.tables
            .get(&original_table_name.to_lowercase())
            .map(|table| &table.table)
    }

    pub fn get_table_columns(&self, original_table_name: &str) -> Option<&HashMap<String, String>> {
        self.tables
            .get(&original_table_name.to_lowercase())
            .map(|table| &table.columns)
    }

    pub fn get_column_name(
        &self,
        original_table_name: &str,
        original_column_name: &str,
    ) -> Option<&String> {
        self.tables
            .get(&original_table_name.to_lowercase())
            .and_then(|table| table.columns.get(&original_column_name.to_lowercase()))
    }

    pub fn has_table(&self, original_table_name: &str) -> bool {
        self.tables
            .contains_key(&original_table_name.to_lowercase())
    }
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

    let mut tables = HashMap::new();
    let mut schema_map_tables = HashMap::new();

    let table_regex =
        regex::Regex::new(r"public virtual DbSet<(.*)> (.*) \{ get; set; \}").unwrap();
    let entity_regex =
        regex::Regex::new(r"modelBuilder\.Entity<(.*)>\(\s*entity\s* =>\s*\{([\s\S]*?)\}\);")
            .unwrap();

    for cap in table_regex.captures_iter(&context_content) {
        let original_entity_name = cap[1].to_lowercase();
        let entity_name = cap[2].to_string();
        tables.insert(original_entity_name, entity_name);
    }

    for cap in entity_regex.captures_iter(&context_content) {
        let mut properties = HashMap::new();

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

        let original_table_name =
            original_table_name_regex.captures(entity_content).unwrap()[1].to_lowercase();

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

            properties.insert(original_column_name, mapped_column_name);
        }

        let model_property_occurrences = model_property_regex.captures_iter(&model_content);
        for occurrence in model_property_occurrences {
            let field_type_with_qm = &occurrence[1];
            let field_type = field_type_with_qm.trim_end_matches('?');
            let is_optional = field_type_with_qm.ends_with('?');

            let field_name = &occurrence[2];

            // TODO: we'll sometime have to do something with the virtual fields (which are relations basically)

            if !properties.contains_key(&field_name.to_lowercase()) {
                properties.insert(field_name.to_lowercase(), field_name.to_string());
            }
        }

        let schema_map_table = SchemaMapTable {
            table: tables[&entity_name.to_lowercase()].clone(),
            columns: properties,
        };
        schema_map_tables.insert(original_table_name, schema_map_table);
    }

    SchemaMapping {
        tables: schema_map_tables,
    }
}
