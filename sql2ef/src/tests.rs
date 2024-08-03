use serde::{Deserialize, Serialize};
use std::fs::{self};
use std::io::{self, Read};
use std::path::Path;

use crate::constants;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Test {
    pub query: String,
    pub result: String,
    pub db_name: String,
}

pub fn write_test(test: Test) -> io::Result<()> {
    let file_path = constants::TESTS_JSON_FILE_PATH;

    let mut tests = read_tests(file_path)?;

    tests.push(test);

    let serialized = serde_json::to_string(&tests)?;
    fs::write(file_path, serialized)?;

    Ok(())
}

pub fn get_test(query: &str, db_name: &str) -> Option<Test> {
    let file_path = constants::TESTS_JSON_FILE_PATH;

    if let Ok(tests) = read_tests(file_path) {
        for test in tests {
            if test.query == query && test.db_name == db_name {
                return Some(test);
            }
        }
    }

    return None;
}

pub fn update_test(query: &str, updated_test: Test) -> io::Result<()> {
    let file_path = constants::TESTS_JSON_FILE_PATH;

    let mut tests = read_tests(file_path)?;

    for test in &mut tests {
        if test.query == query {
            *test = updated_test;
            break;
        }
    }

    let serialized = serde_json::to_string(&tests)?;
    fs::write(file_path, serialized)?;

    Ok(())
}

pub fn read_tests<P: AsRef<Path>>(path: P) -> io::Result<Vec<Test>> {
    if !path.as_ref().exists() {
        return Ok(Vec::new());
    }

    let mut file = fs::File::open(path)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;

    let tests: Vec<Test> = serde_json::from_str(&contents)?;
    Ok(tests)
}
