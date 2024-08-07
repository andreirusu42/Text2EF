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
    QueryBuildFailed,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum SplitType {
    Train,
    Dev,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct Query {
    pub sql: String,
    pub linq: String,
    pub db_name: String,
    pub status: TestStatus,
    pub error: Option<String>,
    pub should_retest: bool,
    pub split: SplitType,
}

pub struct QueryManager {
    queries: Vec<Query>,
    file_path: String,
}

impl QueryManager {
    pub fn new(file_path: &str) -> io::Result<Self> {
        let tests = Self::read_queries(file_path)?;
        Ok(Self {
            queries: tests,
            file_path: file_path.to_string(),
        })
    }

    pub fn get_queries(&self) -> &Vec<Query> {
        &self.queries
    }

    fn read_queries<P: AsRef<Path>>(path: P) -> io::Result<Vec<Query>> {
        if !path.as_ref().exists() {
            return Ok(Vec::new());
        }

        let mut file = fs::File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        let tests: Vec<Query> = serde_json::from_str(&contents)?;
        Ok(tests)
    }

    fn write_queries(&self) -> io::Result<()> {
        let serialized = serde_json::to_string_pretty(&self.queries)?;
        fs::write(&self.file_path, serialized)?;
        Ok(())
    }

    pub fn write_query(&mut self, test: Query) -> io::Result<()> {
        self.queries.push(test);
        self.write_queries()
    }

    pub fn write_test_or_update(&mut self, test: Query) -> io::Result<()> {
        let mut found = false;
        for t in &mut self.queries {
            if t.sql == test.sql && t.db_name == test.db_name {
                *t = test.clone();
                found = true;
                break;
            }
        }

        if !found {
            self.queries.push(test);
        }

        self.write_queries()
    }

    pub fn get_query(&self, query: &str, db_name: &str) -> Option<Query> {
        for test in &self.queries {
            if test.sql == query && test.db_name == db_name {
                return Some(test.clone());
            }
        }
        None
    }

    pub fn update_query(&mut self, query: &str, updated_test: Query) -> io::Result<()> {
        for test in &mut self.queries {
            if test.sql == query {
                *test = updated_test;
                break;
            }
        }

        self.write_queries()
    }
}
