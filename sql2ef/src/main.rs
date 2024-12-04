mod case_insensitive_hashmap;
mod case_insensitive_hashset;
mod case_insensitive_indexmap;
mod constants;
mod context_creator;
mod csharp;
mod dataset;
mod determine_join_order;
mod linq_query_builder;
mod manual_tests;
mod query_manager;
mod schema_mapping;

use std::fs::File;
use std::panic::catch_unwind;
use std::path::Path;
use std::process::exit;
use std::{collections::HashSet, io::Write};

use constants::SPIDER_DATASET_NAME;
use context_creator::extract_context_for_databases;
use csharp::{execute_csharp_code, BuildResultStatus, CodeResultStatus};
use dataset::{extract_queries, RawQuery};
use linq_query_builder::LinqQueryBuilder;
use query_manager::{Query, QueryManager, TestStatus};
use schema_mapping::extract_context_and_models;

fn create_code_execution_code(
    context_name: &str,
    dataset_name: &str,
    db_name: &str,
    query: &str,
    result: &str,
) -> String {
    let c_sharp_code = format!(
        r#"using entity_framework.Models.{}.{}; 
using Microsoft.EntityFrameworkCore;

class Program {{
public static void Main() {{
var context = new {}();

var sql = "{}";
var linq = {}

Tester.Test(linq, sql, context);
}}

}}"#,
        dataset_name,
        db_name,
        context_name,
        query.escape_debug(),
        result
    );

    return c_sharp_code;
}

fn execute_query_and_update_tests_file(
    context_name: &str,
    dataset_name: &str,
    db_name: &str,
    query: &RawQuery,
    result: &str,
    query_manager: &mut QueryManager,
) -> bool {
    let c_sharp_code =
        create_code_execution_code(context_name, dataset_name, db_name, &query.sql, result);

    let execution_result = execute_csharp_code(constants::EF_PROJECT_DIR, &c_sharp_code);

    match execution_result.build_result {
        BuildResultStatus::OK => {
            let code_result = execution_result.code_result.unwrap();

            match code_result {
                CodeResultStatus::OK => {
                    println!("Query executed successfully");

                    let test = Query::new(
                        dataset_name,
                        db_name,
                        &query.sql,
                        &query.question,
                        result,
                        None,
                        TestStatus::Passed,
                    );

                    query_manager.write_test_or_update(test).unwrap();

                    return true;
                }

                CodeResultStatus::WrongResults(exception_details) => {
                    println!("Query execution failed: {:?}", exception_details);

                    let test = Query::new(
                        dataset_name,
                        db_name,
                        &query.sql,
                        &query.question,
                        result,
                        Some(format!("{:?}", exception_details)),
                        TestStatus::CodeFailed,
                    );

                    query_manager.write_test_or_update(test).unwrap();

                    return false;
                }

                CodeResultStatus::UnhandledException(error) => {
                    println!("Query execution failed: {}", error);

                    let test = Query::new(
                        dataset_name,
                        db_name,
                        &query.sql,
                        &query.question,
                        result,
                        Some(error),
                        TestStatus::CodeFailed,
                    );

                    query_manager.write_test_or_update(test).unwrap();

                    return false;
                }
            }
        }
        BuildResultStatus::Fail(error_message) => {
            println!("Build failed: {}", error_message);

            let test = Query::new(
                dataset_name,
                db_name,
                &query.sql,
                &query.question,
                result,
                Some(error_message),
                TestStatus::BuildFailed,
            );

            query_manager.write_test_or_update(test).unwrap();

            return false;
        }
    }
}

// This is being used for when I modify something in the logic of testing the queries (in C#).
fn run_queries_bulk() {
    let test_manager = QueryManager::new(constants::QUERIES_JSON_FILE_PATH).unwrap();

    let mut c_sharp_code = String::new();
    let mut main_code = String::new();

    let db_names: HashSet<String> = test_manager
        .get_queries()
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
            .get_queries()
            .iter()
            .filter(|test| test.db_name == *db_name && test.status == TestStatus::Passed)
            .map(|test| (test.sql.as_str(), test.linq.as_str()))
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
    let dataset = extract_queries(
        constants::EF_MODELS_DIR,
        constants::SPIDER_DATASET_NAME,
        constants::SPIDER_DATASET_FILE_PATH,
    );

    let mut query_manager = QueryManager::new(constants::QUERIES_JSON_FILE_PATH).unwrap();

    for db_name in &dataset.db_names {
        let queries = if let Some(queries) = dataset.queries.get(db_name.as_str()) {
            queries
        } else {
            continue;
        };

        let linq_query_builder_result = catch_unwind(|| {
            LinqQueryBuilder::new(
                Path::new(constants::EF_MODELS_DIR)
                    .join(&dataset.dataset_name)
                    .join(db_name)
                    .to_str()
                    .unwrap(),
            )
        });

        let linq_query_builder = if let Ok(linq_query_builder) = linq_query_builder_result {
            linq_query_builder
        } else {
            if let Err(err) = linq_query_builder_result {
                let error_message = if let Some(s) = err.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = err.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "Unknown panic".to_string()
                };

                println!(
                    "Failed to create LinqQueryBuilder for {}, error: {}",
                    db_name, error_message
                );

                for query in queries.iter() {
                    query_manager
                        .write_test_or_update(Query::new(
                            &dataset.dataset_name,
                            db_name,
                            &query.sql,
                            &query.question,
                            "",
                            Some(error_message.to_string()),
                            TestStatus::SchemaMappingGenerationFailed,
                        ))
                        .unwrap();
                }

                continue;
            } else {
                panic!("Hm2");
            }
        };

        let context_name = linq_query_builder.get_context_name();

        for (index, query) in queries.iter().enumerate() {
            println!("Running query {} for {} - {}", index, db_name, query.sql);

            let build_query_result = catch_unwind(|| linq_query_builder.build_query(&query.sql));

            let result = if let Ok(res) = build_query_result {
                res
            } else {
                if let Err(err) = build_query_result {
                    println!("Failed to build query for {}", query.sql);

                    let error_message = if let Some(s) = err.downcast_ref::<&str>() {
                        s.to_string()
                    } else if let Some(s) = err.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        "Unknown panic".to_string()
                    };

                    query_manager
                        .write_test_or_update(Query::new(
                            &dataset.dataset_name,
                            db_name,
                            &query.sql,
                            &query.question,
                            "",
                            Some(error_message),
                            TestStatus::QueryBuildFailed,
                        ))
                        .unwrap();

                    continue;
                } else {
                    panic!("Hm");
                }
            };

            if let Some(test) = query_manager.get_query(&query.sql, db_name) {
                if test.linq == result && test.status == TestStatus::Passed {
                    continue;
                }

                if !test.should_retest {
                    continue;
                }

                if test.status != TestStatus::Passed {
                    println!(
                        "Query already exists in the tests file, but it's not passed. Retesting..."
                    );
                } else if test.linq != result {
                    println!("Query already exists in the tests file, but the result is different. Retesting...");
                }
            } else {
                println!("Query does not exist. Testing...")
            }

            let was_success = execute_query_and_update_tests_file(
                &context_name,
                &dataset.dataset_name,
                &db_name,
                query,
                &result,
                &mut query_manager,
            );

            if !was_success {
                exit(0);
            }
        }
    }
}

fn run_tests() {
    let test_manager = QueryManager::new(constants::QUERIES_JSON_FILE_PATH).unwrap();

    for (index, test) in test_manager.get_queries().iter().enumerate() {
        let linq_query_builder = LinqQueryBuilder::new(
            Path::new(constants::EF_MODELS_DIR)
                .join(&test.db_name)
                .to_str()
                .unwrap(),
        );

        let result = linq_query_builder.build_query(&test.sql);

        println!("{:?} - {:?}", test.db_name, test.sql);

        if result != test.linq {
            println!("Test {} failed: {}", index, test.sql);
            println!("Expected: {}", test.linq);
            println!("Got:      {}", result);
        } else {
            println!("Test {} passed", index);
        }
    }
}

fn debug_query(id: &str, with_code_execution: bool) {
    let mut query_manager = QueryManager::new(constants::QUERIES_JSON_FILE_PATH).unwrap();

    let query_option = query_manager.get_query_by_id(id);

    let query = if let Some(query) = query_option {
        query
    } else {
        panic!("Query not found");
    };

    let linq_query_builder = LinqQueryBuilder::new(
        Path::new(constants::EF_MODELS_DIR)
            .join(&query.dataset_name)
            .join(&query.db_name)
            .to_str()
            .unwrap(),
    );

    let result = linq_query_builder.build_query(&query.sql);

    println!("Result: {}", result);

    if with_code_execution {
        let result = execute_csharp_code(
            constants::EF_PROJECT_DIR,
            &create_code_execution_code(
                &linq_query_builder.get_context_name(),
                &query.dataset_name,
                &query.db_name,
                &query.sql,
                &result,
            ),
        );

        println!("{:?}", result);
    }
}

fn main() {
    // run_tests();
    // run_queries_sequentially();
    // run_queries_bulk();

    // debug_query(
    //     "07205c083677fef3773456b84f2ecaaaab97575390a6af2461a0741e437df9ec",
    //     false,
    // );

    // debug_query(
    //     "4a396cd1e8daae63224fa57d400c8fab67efc1ee4c461e452fff2fee02fe5f78",
    //     false
    // );
}
