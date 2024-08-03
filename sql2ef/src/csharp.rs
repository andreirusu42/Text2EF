use regex::Regex;
use std::fs::File;
use std::io::{self, Write};
use std::process::{Command, Output};

#[derive(Debug)]
pub struct ExecutionResult {
    pub build_result: BuildResultStatus,
    pub code_result: Option<CodeResultStatus>,
}

#[derive(Debug)]
pub enum CodeResultStatus {
    OK,
    Fail(ExceptionDetails),
}

#[derive(Debug)]
pub enum BuildResultStatus {
    OK,
    Fail(String),
}

#[derive(Debug)]
pub struct ExceptionDetails {
    pub sql_results: String,
    pub linq_results: String,
}

pub fn execute_csharp_code(project_dir: &str, code: &str) -> ExecutionResult {
    let file_path = format!("{}/Program.cs", project_dir);

    save_to_file(&file_path, &code).unwrap();

    match build_project(project_dir) {
        Ok(output) => {
            if !output.status.success() {
                let error_message = String::from_utf8_lossy(&output.stderr).to_string();
                return ExecutionResult {
                    build_result: BuildResultStatus::Fail(error_message),
                    code_result: None,
                };
            } else {
                match run_project(project_dir) {
                    Ok(output) => {
                        if !output.status.success() {
                            let error_message = String::from_utf8_lossy(&output.stderr).to_string();

                            let exception_details = parse_exception_details(&error_message);
                            return ExecutionResult {
                                build_result: BuildResultStatus::OK,
                                code_result: Some(CodeResultStatus::Fail(exception_details)),
                            };
                        } else {
                            return ExecutionResult {
                                build_result: BuildResultStatus::OK,
                                code_result: Some(CodeResultStatus::OK),
                            };
                        }
                    }
                    _ => panic!("This shouldn't happen (as every programmer believes initially)"),
                }
            }
        }
        Err(e) => {
            return ExecutionResult {
                build_result: BuildResultStatus::Fail(e.to_string()),
                code_result: None,
            };
        }
    }
}

fn save_to_file(path: &str, content: &str) -> io::Result<()> {
    let mut file = File::create(path)?;
    file.write_all(content.as_bytes())?;
    Ok(())
}

fn build_project(project_dir: &str) -> io::Result<Output> {
    Command::new("dotnet")
        .arg("build")
        .current_dir(project_dir)
        .output()
}

fn run_project(project_dir: &str) -> io::Result<Output> {
    Command::new("dotnet")
        .arg("run")
        .current_dir(project_dir)
        .output()
}

fn parse_exception_details(error_message: &str) -> ExceptionDetails {
    let sql_regex = Regex::new(r"SQL Results: (.+?)\n").unwrap();
    let linq_regex = Regex::new(r"LINQ Results: (.+?)\n").unwrap();

    let sql_results = sql_regex
        .captures(error_message)
        .and_then(|cap| cap.get(1))
        .map_or(String::new(), |m| m.as_str().to_string());

    let linq_results = linq_regex
        .captures(error_message)
        .and_then(|cap| cap.get(1))
        .map_or(String::new(), |m| m.as_str().to_string());

    ExceptionDetails {
        sql_results,
        linq_results,
    }
}
