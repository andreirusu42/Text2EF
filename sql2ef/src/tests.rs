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

pub fn does_test_exist(query: &str) -> bool {
    let file_path = constants::TESTS_JSON_FILE_PATH;

    if let Ok(tests) = read_tests(file_path) {
        tests.iter().any(|t| t.query == query)
    } else {
        false
    }
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
