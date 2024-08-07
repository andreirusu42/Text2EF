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
use std::{collections::HashSet, io::Write};

use context_creator::extract_context_for_databases;
use csharp::{execute_csharp_code, BuildResultStatus, CodeResultStatus};
use dataset::extract_queries;
use linq_query_builder::LinqQueryBuilder;
use query_manager::{Query, QueryManager, SplitType, TestStatus};
use schema_mapping::extract_context_and_models;

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
    split: &SplitType,
    test_manager: &mut QueryManager,
) {
    let c_sharp_code = create_code_execution_code(context_name, db_name, query, result);

    let execution_result = execute_csharp_code(constants::EF_PROJECT_DIR, &c_sharp_code);

    match execution_result.build_result {
        BuildResultStatus::OK => {
            let code_result = execution_result.code_result.unwrap();

            match code_result {
                CodeResultStatus::OK => {
                    println!("Query executed successfully");

                    let test = Query {
                        db_name: db_name.to_string(),
                        sql: query.to_string(),
                        linq: result.to_string(),
                        error: None,
                        status: TestStatus::Passed,
                        should_retest: false,
                        split: split.clone(),
                    };

                    test_manager.write_test_or_update(test).unwrap();
                }

                CodeResultStatus::Fail(exception_details) => {
                    println!("Query execution failed: {:?}", exception_details);

                    let test = Query {
                        db_name: db_name.to_string(),
                        sql: query.to_string(),
                        linq: result.to_string(),
                        error: Some(format!("{:?}", exception_details)),
                        status: TestStatus::CodeFailed,
                        should_retest: false,
                        split: split.clone(),
                    };

                    test_manager.write_test_or_update(test).unwrap();
                }
            }
        }
        BuildResultStatus::Fail(error_message) => {
            println!("Build failed: {}", error_message);

            let test = Query {
                db_name: db_name.to_string(),
                sql: query.to_string(),
                linq: result.to_string(),
                error: Some(error_message),
                status: TestStatus::BuildFailed,
                should_retest: false,
                split: split.clone(),
            };
            test_manager.write_test_or_update(test).unwrap();
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

fn run_queries_sequentially(split: SplitType) {
    let dataset_file_path = if split == SplitType::Train {
        constants::TRAIN_GOLD_DATASET_FILE_PATH
    } else if split == SplitType::Dev {
        constants::DEV_GOLD_DATASET_FILE_PATH
    } else {
        panic!("Invalid split: {:?}", split);
    };

    let dataset = extract_queries(constants::EF_MODELS_DIR, dataset_file_path);
    let mut query_manager = QueryManager::new(constants::QUERIES_JSON_FILE_PATH).unwrap();

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

            let build_query_result = catch_unwind(|| linq_query_builder.build_query(query));

            let result = if let Ok(res) = build_query_result {
                res
            } else {
                if let Err(err) = build_query_result {
                    println!("Failed to build query for {}", query);

                    let error_message = if let Some(s) = err.downcast_ref::<&str>() {
                        s.to_string()
                    } else if let Some(s) = err.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        "Unknown panic".to_string()
                    };

                    query_manager
                        .write_test_or_update(Query {
                            db_name: db_name.to_string(),
                            sql: query.to_string(),
                            linq: "".to_string(),
                            error: Some(error_message),
                            status: TestStatus::QueryBuildFailed,
                            should_retest: false,
                            split: split.clone(),
                        })
                        .unwrap();

                    continue;
                } else {
                    panic!("Hm");
                }
            };

            if let Some(test) = query_manager.get_query(query, db_name) {
                if test.linq == result && test.status == TestStatus::Passed {
                    println!("Query already exists in the tests file and it's passed.");
                    continue;
                }

                if test.status != TestStatus::Passed {
                    if test.should_retest {
                        println!("Query already exists in the tests file, but it's not passed. Retesting...");

                        execute_query_and_update_tests_file(
                            &context_name,
                            &db_name,
                            &query,
                            &result,
                            &split,
                            &mut query_manager,
                        );

                        continue;
                    } else {
                        println!("Query already exists in the tests file, but it's not passed and should not be retested.");
                        continue;
                    }
                }

                if test.should_retest {
                    println!(
                        "Query already exists in the tests file, but the result is different."
                    );
                    execute_query_and_update_tests_file(
                        &context_name,
                        &db_name,
                        &query,
                        &result,
                        &split,
                        &mut query_manager,
                    );
                    continue;
                } else {
                    println!("Query already exists in the tests file, but the result is different and should not be retested.");
                    continue;
                }
            }

            execute_query_and_update_tests_file(
                &context_name,
                &db_name,
                &query,
                &result,
                &split,
                &mut query_manager,
            );
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

        if result != test.linq {
            println!("Test {} failed: {}", index, test.sql);
            println!("Expected: {}", test.linq);
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
    run_queries_sequentially(SplitType::Train);
    // run_queries_bulk();

    // extract_context_for_databases();

    // debug_query(
    //     "college_2",
    //     r#"SELECT avg(T1.salary) , count(*) FROM instructor AS T1 JOIN department AS T2 ON T1.dept_name = T2.dept_name ORDER BY T2.budget DESC LIMIT 1"#,
    //     false,
    // );
}
