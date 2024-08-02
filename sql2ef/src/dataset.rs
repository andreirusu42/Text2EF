use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufRead};
use std::path::Path;

pub struct Dataset {
    pub db_names: Vec<String>,
    pub queries: HashMap<String, Vec<String>>,
}

pub fn extract_queries(models_dir: &str, dataset_file_path: &str) -> Dataset {
    let mut db_names: Vec<String> = Vec::new();
    for entry in fs::read_dir(models_dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();

        if path.is_dir() {
            let file_name = path.file_name().unwrap().to_str().unwrap().to_string();
            let mut files = fs::read_dir(path).unwrap();

            if files.next().is_some() {
                db_names.push(file_name);
            }
        }
    }

    let path = Path::new(dataset_file_path);

    let file = File::open(&path).unwrap();
    let reader = io::BufReader::new(file);

    let mut queries: HashMap<String, Vec<String>> = HashMap::new();

    for line in reader.lines() {
        let line = line.unwrap();

        let parts: Vec<&str> = line.split_whitespace().collect();

        if parts.len() > 1 {
            let db_id = parts.last().unwrap().to_string();

            let query = parts[..parts.len() - 1].join(" ");

            queries.entry(db_id).or_insert(Vec::new()).push(query);
        }
    }

    Dataset { db_names, queries }
}
