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
use tests::{get_test, read_tests, update_test, write_test, Test};

fn run_test(context_name: &str, db_name: &str, query: &str, result: &str, update: bool) {
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

    let test = Test {
        db_name: db_name.to_string(),
        query: query.to_string(),
        result: result.to_string(),
    };

    let execution_result = execute_csharp_code(constants::EF_PROJECT_DIR, &c_sharp_code);

    match execution_result.build_result {
        BuildResultStatus::OK => {
            let code_result = execution_result.code_result.unwrap();

            match code_result {
                CodeResultStatus::OK => {
                    println!("Query executed successfully");

                    if (update) {
                        update_test(query, test).unwrap();
                    } else {
                        write_test(test).unwrap();
                    }
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

fn run_queries() {
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
            let lowercase_query = query.to_lowercase();

            let blacklist = vec![
                "t2.actid = t2.actid",                           // activity_1
                "t1.booking_start_date , t1.booking_start_date", // apartment_rentals
                "t2.allergytype",                                // allergy_1
                "ref_company_types",                             // assets_maintenance
                "tourist_attraction_features",                   // TODO: m2m tables
                "circulation_history",                           // TODO: m2m tables
            ];

            if blacklist.iter().any(|key| lowercase_query.contains(key)) {
                continue;
            }

            println!("Processing query {} for {} - {}", index, db_name, query);

            let result = linq_query_builder.build_query(query);

            if let Some(test) = get_test(query) {
                if test.result == result {
                    println!("Query already exists in the tests file, skipping.");
                    continue;
                }

                println!("Query already exists in the tests file, but the result is different.");
                run_test(&context_name, &db_name, &query, &result, true);
            }

            run_test(&context_name, &db_name, &query, &result, false);
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
