mod case_insensitive_hashmap;
mod case_insensitive_hashset;
mod constants;
mod csharp;
mod dataset;
mod determine_join_order;
mod linq_query_builder;
mod manual_tests;
mod schema_mapping;
mod tests;
use std::path::Path;

use csharp::{execute_csharp_code, BuildResultStatus, CodeResultStatus};
use dataset::extract_queries;
use linq_query_builder::LinqQueryBuilder;
use tests::{does_test_exist, read_tests, write_test, Test};

fn run_queries() {
    let mut successfully_executed_queries = 0;
    let dataset = extract_queries(constants::EF_MODELS_DIR, constants::GOLD_DATASET_FILE_PATH);

    for db_name in &dataset.db_names {
        // Sadly, this one has issues with the data, since I get
        // Unhandled exception. System.InvalidOperationException: Nullable object must have a value.
        // when trying to execute a simple context.idk.Count();
        if db_name == "college_2" {
            continue;
        }

        if db_name == "customers_and_addresses" {
            break;
        }

        let queries = if let Some(queries) = dataset.queries.get(db_name.as_str()) {
            queries
        } else {
            continue;
        };

        let linq_query_builder = LinqQueryBuilder::new(
            Path::new(constants::EF_MODELS_DIR)
                .join(db_name)
                .to_str()
                .unwrap(),
        );

        let context_name = linq_query_builder.get_context_name();

        for (index, query) in queries.iter().enumerate() {
            // wrong in the dataset, activity_1
            if query.to_lowercase().contains("t2.actid = t2.actid") {
                continue;
            }

            // wrong in the dataset, apartment_rentals
            if query
                .to_lowercase()
                .contains("t1.booking_start_date , t1.booking_start_date")
            {
                continue;
            }

            // wrong in the dataset, allergy_1
            if query.to_lowercase().contains("t2.allergytype") {
                // the field should be allergy_type
                continue;
            }

            // wrong in the dataset, assets_maintenance
            if query.to_lowercase().contains("ref_company_types") {
                continue;
            }

            println!("Processing query {} for {} - {}", index, db_name, query);

            if does_test_exist(query) {
                println!("Query already exists in the tests file, skipping.");
                continue;
            }

            let result = linq_query_builder.build_query(query);

            let mut c_sharp_code = format!(
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
                db_name, context_name, query, result
            );

            let test = Test {
                db_name: db_name.to_string(),
                query: query.to_string(),
                result,
            };

            let execution_result = execute_csharp_code(constants::EF_PROJECT_DIR, &c_sharp_code);

            match execution_result.build_result {
                BuildResultStatus::OK => {
                    let code_result = execution_result.code_result.unwrap();

                    match code_result {
                        CodeResultStatus::OK => {
                            successfully_executed_queries += 1;
                            println!("Query executed successfully");

                            write_test(test).unwrap();
                        }

                        CodeResultStatus::Fail(exception_details) => {
                            panic!("Query execution failed: {:?}", exception_details);
                        }
                    }
                }
                BuildResultStatus::Fail(error_message) => {
                    panic!("Build failed: {}", error_message);
                }
            }
        }
    }
}

fn run_tests() {
    let tests = read_tests(constants::TESTS_JSON_FILE_PATH).unwrap();

    for (index, test) in tests.iter().enumerate() {
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

fn main() {
    // run_tests();
    run_queries();
}
