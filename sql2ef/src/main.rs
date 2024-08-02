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

use std::fs::File;
use std::path::Path;
use std::{collections::HashSet, io::Write};

use csharp::{execute_csharp_code, BuildResultStatus, CodeResultStatus};
use dataset::extract_queries;
use linq_query_builder::LinqQueryBuilder;
use tests::{get_test, read_tests, update_test, write_test, Test};

fn execute_query_and_update_tests_file(
    context_name: &str,
    db_name: &str,
    query: &str,
    result: &str,
    update: bool,
) {
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

                    if update {
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

// This is being used for when I modify something in the logic of testing the queries (in C#).
fn run_queries_bulk() {
    let tests = read_tests(constants::TESTS_JSON_FILE_PATH).unwrap();

    let mut c_sharp_code = String::new();
    let mut main_code = String::new();

    let db_names: HashSet<String> = tests.iter().map(|test| test.db_name.clone()).collect();

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

        let sql_and_results: Vec<(&str, &str)> = tests
            .iter()
            .filter(|test| test.db_name == *db_name)
            .map(|test| (test.query.as_str(), test.result.as_str()))
            .collect();

        let result = sql_and_results
            .iter()
            .map(|(a, b)| format!("({}, \"{}\"),", b.trim_end_matches(';'), a.escape_default()))
            .collect::<Vec<String>>()
            .join("\n");

        c_sharp_code.push_str(&result);

        c_sharp_code.push_str(
            "};\n\n for (int i = 0; i < tests.Length; ++i) { var (linq_query, sql_query) = tests[i];\n\n Console.WriteLine($\"Testing {sql_query}\");  Tester.Test(linq_query, sql_query, context); } }",
        );

        main_code.push_str(&format!(
            "Console.WriteLine(\"Running tests for {}\");\n Test{}();\n",
            context_name, context_name
        ));
    }

    c_sharp_code.push_str(&format!("\nstatic void Main() {{\n{}\n}}", main_code));
    c_sharp_code.push_str("\n}");

    let path = Path::new("./ef/Program.cs");
    let mut file = File::create(&path).expect("Unable to create file");
    file.write_all(c_sharp_code.as_bytes())
        .expect("Unable to write data");
}

fn run_queries_sequentially() {
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
                execute_query_and_update_tests_file(&context_name, &db_name, &query, &result, true);
            }

            execute_query_and_update_tests_file(&context_name, &db_name, &query, &result, false);
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
    // run_queries_sequentially();
    run_queries_bulk();
}
