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

use context_creator::extract_context_for_databases;
use csharp::{execute_csharp_code, BuildResultStatus, CodeResultStatus};
use dataset::{extract_queries, RawQuery};
use linq_query_builder::LinqQueryBuilder;
use query_manager::{Query, QueryManager, TestStatus};
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
    query: &RawQuery,
    result: &str,
    query_manager: &mut QueryManager,
) {
    let c_sharp_code = create_code_execution_code(context_name, db_name, &query.sql, result);

    let execution_result = execute_csharp_code(constants::EF_PROJECT_DIR, &c_sharp_code);

    match execution_result.build_result {
        BuildResultStatus::OK => {
            let code_result = execution_result.code_result.unwrap();

            match code_result {
                CodeResultStatus::OK => {
                    println!("Query executed successfully");

                    let test = Query::new(
                        db_name,
                        &query.sql,
                        &query.question,
                        result,
                        None,
                        TestStatus::Passed,
                    );

                    query_manager.write_test_or_update(test).unwrap();
                }

                CodeResultStatus::WrongResults(exception_details) => {
                    println!("Query execution failed: {:?}", exception_details);

                    let test = Query::new(
                        db_name,
                        &query.sql,
                        &query.question,
                        result,
                        Some(format!("{:?}", exception_details)),
                        TestStatus::CodeFailed,
                    );

                    query_manager.write_test_or_update(test).unwrap();
                    exit(0);
                }

                CodeResultStatus::UnhandledException(error) => {
                    println!("Query execution failed: {}", error);

                    let test = Query::new(
                        db_name,
                        &query.sql,
                        &query.question,
                        result,
                        Some(error),
                        TestStatus::CodeFailed,
                    );

                    query_manager.write_test_or_update(test).unwrap();
                    exit(0);
                }
            }
        }
        BuildResultStatus::Fail(error_message) => {
            println!("Build failed: {}", error_message);

            let test = Query::new(
                db_name,
                &query.sql,
                &query.question,
                result,
                Some(error_message),
                TestStatus::BuildFailed,
            );

            query_manager.write_test_or_update(test).unwrap();
            exit(0);
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
    let dataset = extract_queries(constants::EF_MODELS_DIR, constants::DATASET_FILE_PATH);
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

                    if !error_message.contains("set expression") {
                        println!("{:?}", error_message);
                        panic!();
                    }

                    query_manager
                        .write_test_or_update(Query::new(
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

                if test.status != TestStatus::Passed {
                    println!(
                        "Query already exists in the tests file, but it's not passed. Retesting..."
                    );

                    execute_query_and_update_tests_file(
                        &context_name,
                        &db_name,
                        query,
                        &result,
                        &mut query_manager,
                    );

                    continue;
                }
            }

            execute_query_and_update_tests_file(
                &context_name,
                &db_name,
                &query,
                &result,
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

    // debug_query(
    //     "game_injury",
    //     r#"SELECT name ,  average_attendance ,  total_attendance FROM stadium EXCEPT SELECT T2.name ,  T2.average_attendance ,  T2.total_attendance FROM game AS T1 JOIN stadium AS T2 ON T1.stadium_id  =  T2.id JOIN injury_accident AS T3 ON T1.id  =  T3.game_id"#,
    //     true,
    // );

    // debug_query(
    //     "station_weather",
    //     r#"SELECT count(*) ,  t1.network_name ,  t1.services FROM station AS t1 JOIN route AS t2 ON t1.id  =  t2.station_id GROUP BY t2.station_id"#,
    //     true,
    // );

    // TODO: wtoejaoiewjaewiojtyraeoiyae
    // debug_query("scientist_1", r#"SELECT count(*) FROM scientists"#, true);

    // nested set operations
    // debug_query(
    //     "dog_kennels",
    //     r#"SELECT first_name FROM Professionals UNION SELECT first_name FROM Owners EXCEPT SELECT name FROM Dogs"#,
    //     true,
    // );

    // extract_context_for_databases();

    // debug_query(
    //     "musical",
    //     r#"SELECT T2.Name FROM actor AS T1 JOIN musical AS T2 ON T1.Musical_ID  =  T2.Musical_ID GROUP BY T1.Musical_ID HAVING COUNT(*)  >=  3"#,
    //     true,
    // );

    // These 4 should work
    // debug_query(
    //     "cre_Doc_Control_Systems",
    //     r#"SELECT t2.employee_name FROM Circulation_History AS t1 JOIN Employees AS t2 ON t1.employee_id = t2.employee_id WHERE t1.document_id = 1;"#,
    //     true,
    // );
    // debug_query(
    //     "cre_Doc_Control_Systems",
    //     r#"SELECT Employees.employee_name FROM Circulation_History JOIN Employees ON Circulation_History.employee_id = Employees.employee_id WHERE Circulation_History.document_id = 1;"#,
    //     true,
    // );
    // debug_query(
    //     "cre_Doc_Control_Systems",
    //     r#"SELECT t2.employee_name FROM Employees AS t2 JOIN Circulation_History AS t1 ON t1.employee_id = t2.employee_id WHERE t1.document_id = 1;"#,
    //     true,
    // );
    // debug_query(
    //     "cre_Doc_Control_Systems",
    //     r#"SELECT Employees.employee_name FROM Employees JOIN Circulation_History ON Circulation_History.employee_id = Employees.employee_id WHERE Circulation_History.document_id = 1;"#,
    //     true,
    // );

    // All ways need to work!
    // debug_query(
    //     "cre_Theme_park",
    //     r#"SELECT T1.Name FROM Tourist_Attractions AS T1 JOIN Tourist_Attraction_Features AS T2 ON T1.tourist_attraction_id  =  T2.tourist_attraction_id JOIN Features AS T3 ON T2.Feature_ID  =  T3.Feature_ID WHERE T3.feature_Details  =  'park'"#,
    //     true,
    // );
    // debug_query(
    //     "cre_Theme_park",
    //     r#"SELECT T1.Name FROM Tourist_Attraction_Features AS T2 JOIN Tourist_Attractions AS T1 ON T1.tourist_attraction_id  =  T2.tourist_attraction_id JOIN Features AS T3 ON T2.Feature_ID  =  T3.Feature_ID WHERE T3.feature_Details  =  'park'"#,
    //     true,
    // );
    // debug_query(
    //     "cre_Theme_park",
    //     r#"SELECT T1.Name FROM Features AS T3 JOIN Tourist_Attraction_Features AS T2 ON T2.Feature_ID  =  T3.Feature_ID JOIN Tourist_Attractions AS T1 ON T1.tourist_attraction_id  =  T2.tourist_attraction_id WHERE T3.feature_Details  =  'park'"#,
    //     true,
    // );
    // debug_query(
    //     "cre_Theme_park",
    //     r#"SELECT T1.Name FROM Tourist_Attractions AS T1 JOIN Tourist_Attraction_Features AS T2 ON T1.tourist_attraction_id  =  T2.tourist_attraction_id"#,
    //     true,
    // );
    // debug_query(
    //     "cre_Theme_park",
    //     r#"SELECT T1.Name FROM Tourist_Attraction_Features as T2 JOIN Tourist_Attractions AS T1 ON T1.tourist_attraction_id  =  T2.tourist_attraction_id"#,
    //     true,
    // );

    // Both need to work!
    // debug_query(
    //     "network_1",
    //     r#"SELECT T2.name FROM Highschooler AS T2 JOIN Friend as T1 ON T2.id  =  T1.student_id GROUP BY T1.student_id HAVING count(*)  >=  3"#,
    //     true,
    // );
    // debug_query(
    //     "network_1",
    //     r#"SELECT T2.name FROM Friend AS T1 JOIN Highschooler as T2 ON T1.student_id  =  T2.id GROUP BY T1.student_id HAVING count(*)  >=  3"#,
    //     true,
    // );

    // wut is this
    // debug_query(
    //     "cre_Doc_Control_Systems",
    //     r#"SELECT draft_copies.document_id FROM Circulation_History JOIN draft_copies"#,
    //     true,
    // );

    // debug_query(
    //     "station_weather",
    //     r#"SELECT t3.name ,  t3.time FROM station AS t1 JOIN route AS t2 ON t1.id  =  t2.station_id JOIN train AS t3 ON t2.train_id  =  t3.id WHERE t1.local_authority  =  "Chiltern""#,
    //     true,
    // );

    // TODO: this goes on to infinity and i dunno why
    // debug_query(
    //     "station_weather",
    //     r#"SELECT t3.name ,  t3.time FROM route AS t2 JOIN station AS t1 ON t1.id  =  t2.station_id JOIN train AS t3 ON t2.train_id  =  t3.id WHERE t1.local_authority  =  "Chiltern""#,
    //     true,
    // );

    // debug_query(
    //     "flight_1",
    //     r#"SELECT T3.name FROM Employee AS T1 JOIN Certificate AS T2 ON T1.eid  =  T2.eid JOIN Aircraft AS T3 ON T3.aid  =  T2.aid WHERE T1.name  =  "John Williams""#,
    //     true,
    // );

    // TODO: MOST IMPORTANT 2
    // debug_query(
    //     "flight_1",
    //     r#"SELECT T3.name FROM Certificate AS T2 JOIN Employee AS T1 ON T1.eid  =  T2.eid JOIN Aircraft AS T3 ON T3.aid  =  T2.aid WHERE T1.name  =  "John Williams""#,
    //     true,
    // );

    // TODO: no clue so far xD
    // debug_query(
    //     "geo",
    //     r#"SELECT border FROM border_info WHERE state_name = "kentucky""#,
    //     true,
    // );

    // debug_query(
    //     "store_product",
    //     r#"SELECT t1.product FROM product AS t1 JOIN store_product AS t2 ON t1.product_id  =  t2.product_id JOIN store AS t3 ON t2.store_id  =  t3.store_id WHERE t3.store_name  =  "Miramichi""#,
    //     true,
    // );
}
