mod case_insensitive_hashset;
mod case_insensitive_indexmap;
mod constants;
mod csharp;
mod dataset;
mod determine_join_order;
mod linq_query_builder;
mod manual_tests;
mod schema_mapping;
mod tests;

use std::fs::File;
use std::panic::catch_unwind;
use std::path::Path;
use std::{collections::HashSet, io::Write};

use csharp::{execute_csharp_code, BuildResultStatus, CodeResultStatus};
use dataset::extract_queries;
use linq_query_builder::LinqQueryBuilder;
use tests::{Test, TestManager, TestStatus};

fn create_code_execution_code(
    context_name: &str,
    db_name: &str,
    query: &str,
    result: &str,
) -> String {
    let c_sharp_code = format!(
        r#"using entity_framework.Models.{}; 
using Microsoft.EntityFrameworkCore;

class Program {{
public static void Main() {{
var context = new {}();

var sql = "{}";
var linq = {}

Tester.Test(linq, sql, context);
}}

}}"#,
        db_name,
        context_name,
        query.escape_debug(),
        result
    );

    return c_sharp_code;
}

fn execute_query_and_update_tests_file(
    context_name: &str,
    db_name: &str,
    query: &str,
    result: &str,
    test_manager: &mut TestManager,
) {
    let c_sharp_code = create_code_execution_code(context_name, db_name, query, result);

    let execution_result = execute_csharp_code(constants::EF_PROJECT_DIR, &c_sharp_code);

    match execution_result.build_result {
        BuildResultStatus::OK => {
            let code_result = execution_result.code_result.unwrap();

            match code_result {
                CodeResultStatus::OK => {
                    println!("Query executed successfully");

                    let test = Test {
                        db_name: db_name.to_string(),
                        query: query.to_string(),
                        result: result.to_string(),
                        error: None,
                        status: TestStatus::Passed,
                    };

                    test_manager.write_test_or_update(test).unwrap();
                }

                CodeResultStatus::Fail(exception_details) => {
                    println!("Query execution failed: {:?}", exception_details);

                    let test = Test {
                        db_name: db_name.to_string(),
                        query: query.to_string(),
                        result: result.to_string(),
                        error: Some(format!("{:?}", exception_details)),
                        status: TestStatus::CodeFailed,
                    };

                    test_manager.write_test_or_update(test).unwrap();
                }
            }
        }
        BuildResultStatus::Fail(error_message) => {
            println!("Build failed: {}", error_message);

            let test = Test {
                db_name: db_name.to_string(),
                query: query.to_string(),
                result: result.to_string(),
                error: Some(error_message),
                status: TestStatus::BuildFailed,
            };
            test_manager.write_test_or_update(test).unwrap();
        }
    }
}

// This is being used for when I modify something in the logic of testing the queries (in C#).
fn run_queries_bulk() {
    let test_manager = TestManager::new(constants::TESTS_JSON_FILE_PATH).unwrap();

    let mut c_sharp_code = String::new();
    let mut main_code = String::new();

    let db_names: HashSet<String> = test_manager
        .get_tests()
        .iter()
        .map(|test| test.db_name.clone())
        .collect();

    for db_name in &db_names {
        c_sharp_code = format!(
            "{}using entity_framework.Models.{};\n",
            c_sharp_code, db_name
        );
    }

    c_sharp_code = format!(
        "{}\nusing Microsoft.EntityFrameworkCore;\n\nclass Program {{\n",
        c_sharp_code
    );

    for db_name in &db_names {
        let linq_query_builder = LinqQueryBuilder::new(
            Path::new(constants::EF_MODELS_DIR)
                .join(db_name)
                .to_str()
                .unwrap(),
        );

        let context_name = linq_query_builder.get_context_name();
        c_sharp_code.push_str(&format!(
            "\nstatic void Test{}() {{ var context = new {}(); \n var tests = new (object, string)[] {{\n",
            context_name, context_name
        ));

        let sql_and_results: Vec<(&str, &str)> = test_manager
            .get_tests()
            .iter()
            .filter(|test| test.db_name == *db_name && test.status == TestStatus::Passed)
            .map(|test| (test.query.as_str(), test.result.as_str()))
            .collect();

        let result = sql_and_results
            .iter()
            .map(|(a, b)| format!("({}, \"{}\"),", b.trim_end_matches(';'), a.escape_default()))
            .collect::<Vec<String>>()
            .join("\n");

        c_sharp_code.push_str(&result);

        c_sharp_code.push_str(
            "};\n\n for (int i = 0; i < tests.Length; ++i) { var (linq_query, sql_query) = tests[i];\n\n  try { Tester.Test(linq_query, sql_query, context); } catch(Exception e) { Console.WriteLine($\"Query {sql_query} failed \"); throw e; } } }",
        );

        main_code.push_str(&format!(
            "Console.WriteLine(\"Running tests for {}\");\n Test{}();\n",
            context_name, context_name
        ));
    }

    c_sharp_code.push_str(&format!("\nstatic void Main() {{\n{}\n}}", main_code));
    c_sharp_code.push_str("\n}");

    let path = Path::new(constants::EF_PROJECT_DIR).join("Program.cs");
    let mut file = File::create(&path).expect("Unable to create file");
    file.write_all(c_sharp_code.as_bytes())
        .expect("Unable to write data");
}

fn run_queries_sequentially() {
    let dataset = extract_queries(constants::EF_MODELS_DIR, constants::GOLD_DATASET_FILE_PATH);
    let mut test_manager = TestManager::new(constants::TESTS_JSON_FILE_PATH).unwrap();

    for db_name in &dataset.db_names {
        let queries = if let Some(queries) = dataset.queries.get(db_name.as_str()) {
            queries
        } else {
            continue;
        };

        let linq_query_builder = if let Ok(res) = catch_unwind(|| {
            LinqQueryBuilder::new(
                Path::new(constants::EF_MODELS_DIR)
                    .join(db_name)
                    .to_str()
                    .unwrap(),
            )
        }) {
            res
        } else {
            println!("Failed to create LinqQueryBuilder for {}", db_name);
            continue;
        };

        let context_name = linq_query_builder.get_context_name();

        for (index, query) in queries.iter().enumerate() {
            println!("Processing query {} for {} - {}", index, db_name, query);

            let result = if let Ok(res) = catch_unwind(|| linq_query_builder.build_query(query)) {
                res
            } else {
                println!("Failed to build query for {}", query);
                panic!("Why?");
                continue;
            };

            if let Some(test) = test_manager.get_test(query, db_name) {
                if test.result == result && test.status == TestStatus::Passed {
                    println!("Query already exists in the tests file and it's passed.");
                    continue;
                }

                if test.status != TestStatus::Passed {
                    println!("Query already exists in the tests file, but it's not passed.");
                    continue;
                }

                println!("Query already exists in the tests file, but the result is different.");
                execute_query_and_update_tests_file(
                    &context_name,
                    &db_name,
                    &query,
                    &result,
                    &mut test_manager,
                );
            }

            execute_query_and_update_tests_file(
                &context_name,
                &db_name,
                &query,
                &result,
                &mut test_manager,
            );
        }
    }
}

fn run_tests() {
    let test_manager = TestManager::new(constants::TESTS_JSON_FILE_PATH).unwrap();

    for (index, test) in test_manager.get_tests().iter().enumerate() {
        let linq_query_builder = LinqQueryBuilder::new(
            Path::new(constants::EF_MODELS_DIR)
                .join(&test.db_name)
                .to_str()
                .unwrap(),
        );

        let result = linq_query_builder.build_query(&test.query);

        if result != test.result {
            println!("Test {} failed: {}", index, test.query);
            println!("Expected: {}", test.result);
            println!("Got:      {}", result);
        } else {
            println!("Test {} passed", index);
        }
    }
}

fn debug_query(db_name: &str, query: &str, with_code_execution: bool) {
    let linq_query_builder = LinqQueryBuilder::new(
        Path::new(constants::EF_MODELS_DIR)
            .join(db_name)
            .to_str()
            .unwrap(),
    );

    let result = linq_query_builder.build_query(query);

    println!("Result: {}", result);

    if with_code_execution {
        let result = execute_csharp_code(
            constants::EF_PROJECT_DIR,
            &create_code_execution_code(
                &linq_query_builder.get_context_name(),
                db_name,
                query,
                &result,
            ),
        );

        println!("{:?}", result);
    }
}

fn main() {
    // run_tests();
    run_queries_sequentially();
    // run_queries_bulk();

    // debug_query("hr_1", r#"SELECT count(*) FROM scientists"#, false);
}
