use crate::constants;
use crate::dataset::extract_queries;
use crate::schema_mapping::extract_context_and_models;
use serde::Serialize;
use serde_json::to_string_pretty;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::fs::File;
use std::io::Read;
use std::path::Path;

#[derive(Serialize)]
pub struct ContextAndModels {
    context: String,
    models: Vec<String>,
}

fn clean_string(input: &str) -> String {
    input
        .replace("\r\n", "\n")
        .replace("\r", "\n")
        .replace("\u{feff}", "")
}

pub fn extract_context_for_databases(dataset_name: &str) {
    let dataset = extract_queries(
        constants::EF_MODELS_DIR,
        dataset_name,
        constants::SPIDER_DATASET_FILE_PATH,
    );

    let db_names: HashSet<String> = dataset
        .db_names
        .iter()
        .map(|db_name| db_name.to_string())
        .collect();

    let mut context_map: HashMap<String, ContextAndModels> = HashMap::new();

    for db_name in db_names {
        let context_and_models = extract_context_and_models(
            Path::new(constants::EF_MODELS_DIR)
                .join(dataset_name)
                .join(&db_name)
                .to_str()
                .unwrap(),
        );

        let cleaned_context = clean_string(&context_and_models.0);
        let cleaned_models: Vec<String> = context_and_models
            .1
            .iter()
            .map(|model| clean_string(model))
            .collect();

        context_map.insert(
            db_name,
            ContextAndModels {
                context: cleaned_context,
                models: cleaned_models,
            },
        );
    }

    let json = to_string_pretty(&context_map).unwrap();
    fs::write(constants::CONTEXT_JSON_FILE_PATH, json).unwrap();
}
