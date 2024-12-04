use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufReader};
use std::path::Path;

#[derive(Debug)]
pub struct RawQuery {
    pub sql: String,
    pub question: String,
}

#[derive(Debug)]
pub struct Dataset {
    pub dataset_name: String,
    pub db_names: Vec<String>,
    pub queries: HashMap<String, Vec<RawQuery>>,
}

#[derive(Deserialize)]
struct QueryRecord {
    db_id: String,
    query: String,
    question: String,
}

pub fn extract_queries(models_dir: &str, dataset_name: &str, dataset_file_path: &str) -> Dataset {
    let mut db_names: Vec<String> = Vec::new();
    for entry in std::fs::read_dir(Path::new(models_dir).join(dataset_name)).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();

        if path.is_dir() {
            let file_name = path.file_name().unwrap().to_str().unwrap().to_string();
            let mut files = std::fs::read_dir(path).unwrap();

            if files.next().is_some() {
                db_names.push(file_name);
            }
        }
    }

    let path = Path::new(dataset_file_path);

    let file = File::open(&path).unwrap();
    let reader = BufReader::new(file);

    let mut queries: HashMap<String, Vec<RawQuery>> = HashMap::new();

    let json_data: Vec<QueryRecord> = serde_json::from_reader(reader).unwrap();

    for record in json_data {
        let db_id = record.db_id;
        let query = record.query;

        queries.entry(db_id).or_insert(Vec::new()).push(RawQuery {
            question: record.question,
            sql: query,
        });
    }

    Dataset {
        db_names,
        queries,
        dataset_name: dataset_name.to_string(),
    }
}
