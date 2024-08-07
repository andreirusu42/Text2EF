use serde::{Deserialize, Serialize};
use std::fs::{self};
use std::io::{self, Read};
use std::path::Path;

use crate::constants;

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum TestStatus {
    Passed,
    BuildFailed,
    CodeFailed,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct Test {
    pub query: String,
    pub result: String,
    pub db_name: String,
    pub status: TestStatus,
    pub error: Option<String>,
    pub should_retest: bool,
    pub split: String,
}

pub struct TestManager {
    tests: Vec<Test>,
    file_path: String,
}

impl TestManager {
    pub fn new(file_path: &str) -> io::Result<Self> {
        let tests = Self::read_tests(file_path)?;
        Ok(Self {
            tests,
            file_path: file_path.to_string(),
        })
    }

    pub fn get_tests(&self) -> &Vec<Test> {
        &self.tests
    }

    fn read_tests<P: AsRef<Path>>(path: P) -> io::Result<Vec<Test>> {
        if !path.as_ref().exists() {
            return Ok(Vec::new());
        }

        let mut file = fs::File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        let tests: Vec<Test> = serde_json::from_str(&contents)?;
        Ok(tests)
    }

    fn write_tests(&self) -> io::Result<()> {
        let serialized = serde_json::to_string_pretty(&self.tests)?;
        fs::write(&self.file_path, serialized)?;
        Ok(())
    }

    pub fn write_test(&mut self, test: Test) -> io::Result<()> {
        self.tests.push(test);
        self.write_tests()
    }

    pub fn write_test_or_update(&mut self, test: Test) -> io::Result<()> {
        let mut found = false;
        for t in &mut self.tests {
            if t.query == test.query && t.db_name == test.db_name {
                *t = test.clone();
                found = true;
                break;
            }
        }

        if !found {
            self.tests.push(test);
        }

        self.write_tests()
    }

    pub fn get_test(&self, query: &str, db_name: &str) -> Option<Test> {
        for test in &self.tests {
            if test.query == query && test.db_name == db_name {
                return Some(test.clone());
            }
        }
        None
    }

    pub fn update_test(&mut self, query: &str, updated_test: Test) -> io::Result<()> {
        for test in &mut self.tests {
            if test.query == query {
                *test = updated_test;
                break;
            }
        }

        self.write_tests()
    }
}
