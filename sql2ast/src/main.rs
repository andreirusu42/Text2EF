use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, Write};
use std::path::Path;

mod linq_query_builder;
mod schema_mapping;

use linq_query_builder::LinqQueryBuilder;

fn tests() {
    let mut queries_and_results: Vec<(&str, &str)> = Vec::new();

    queries_and_results.push((
        r#"SELECT rank FROM Faculty"#,
        r#"context.Faculties.Select(row => new { row.Rank }).ToList();"#,
    ));

    queries_and_results.push((
        r#"SELECT DISTINCT rank FROM Faculty"#,
        r#"context.Faculties.Select(row => new { row.Rank }).Distinct().ToList();"#,
    ));

    queries_and_results.push((
        r#"SELECT T1.fname, T1.lname FROM Faculty AS T1 JOIN Student AS T2 ON T2.advisor = T1.FacID WHERE T2.fname = "Linda" AND T2.lname = "Smith""#,
        r#"context.Faculties.Join(context.Students, T1 => T1.FacId, T2 => T2.Advisor, (T1, T2) => new { T1, T2 }).Where(row => row.T2.Fname == "Linda" && row.T2.Lname == "Smith").Select(row => new { row.T1.Fname, row.T1.Lname }).ToList();"#,
    ));

    queries_and_results.push((
        r#"SELECT COUNT(*), rank FROM Faculty GROUP BY rank"#,
        r#"context.Faculties.GroupBy(row => new { row.Rank }).Select(group => new { Count = group.Count(), group.Key.Rank }).ToList();"#,
    ));

    queries_and_results.push((
        r#"SELECT T1.FacID FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID  =  T2.advisor GROUP BY T1.FacID"#,
        r#"context.Faculties.Join(context.Students, T1 => T1.FacId, T2 => T2.Advisor, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.FacId }).Select(group => new { group.Key.FacId }).ToList();"#,
    ));

    queries_and_results.push((
        r#"SELECT T1.activity_name FROM Activity AS T1 JOIN Faculty_participates_in AS T2 ON T1.actID  =  T2.actID GROUP BY T1.actID ORDER BY count(*) DESC LIMIT 1"#,
        "context.Activities.Join(context.FacultyParticipatesIns, T1 => T1.Actid, T2 => T2.Actid, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.Actid }).OrderByDescending(group => group.Count()).Select(group => new { group.First().T1.ActivityName }).Take(1).ToList();"
    ));

    queries_and_results.push((
        r#"SELECT T1.stuid FROM participates_in AS T1 JOIN activity AS T2 ON T1.actid  =  T2.actid WHERE T2.activity_name  =  'Canoeing' INTERSECT SELECT T1.stuid FROM participates_in AS T1 JOIN activity AS T2 ON T1.actid  =  T2.actid WHERE T2.activity_name  =  'Kayaking'"#,
        r#"context.ParticipatesIns.Join(context.Activities, T1 => T1.Actid, T2 => T2.Actid, (T1, T2) => new { T1, T2 }).Where(row => row.T2.ActivityName == "Canoeing").Select(row => row.T1.Stuid).Intersect(context.ParticipatesIns.Join(context.Activities, T1 => T1.Actid, T2 => T2.Actid, (T1, T2) => new { T1, T2 }).Where(row => row.T2.ActivityName == "Kayaking").Select(row => row.T1.Stuid)).ToList();"#
    ));

    queries_and_results.push((
        r#"SELECT T3.activity_name FROM Faculty AS T1 JOIN Faculty_participates_in AS T2 ON T2.facID  =  T1.facID JOIN Activity AS T3 ON T3.actid  =  T2.actid WHERE T1.fname  =  "Mark" AND T1.lname  =  "Giuliano""#,
        r#"context.Faculties.Join(context.FacultyParticipatesIns, T1 => T1.FacId, T2 => T2.FacId, (T1, T2) => new { T1, T2 }).Join(context.Activities, joined => joined.T2.Actid, T3 => T3.Actid, (joined, T3) => new { joined.T1, joined.T2, T3 }).Where(row => row.T1.Fname == "Mark" && row.T1.Lname == "Giuliano").Select(row => new { row.T3.ActivityName }).ToList();"#
    ));

    queries_and_results.push((
        r#"SELECT T1.rank ,  count(*) FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID  =  T2.advisor GROUP BY T1.rank"#,
        r#"context.Faculties.Join(context.Students, T1 => T1.FacId, T2 => T2.Advisor, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.Rank }).Select(group => new { group.Key.Rank, Count = group.Count() }).ToList();"#,
    ));

    queries_and_results.push((
        r#"SELECT T1.fname , T1.lname FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID = T2.advisor GROUP BY T1.FacID ORDER BY count(*) DESC LIMIT 1"#,
        r#"context.Faculties.Join(context.Students, T1 => T1.FacId, T2 => T2.Advisor, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.FacId }).OrderByDescending(group => group.Count()).Select(group => new { group.First().T1.Fname, group.First().T1.Lname }).Take(1).ToList();"#
    ));

    queries_and_results.push((
        r#"SELECT FacID FROM Faculty WHERE Sex = 'M'"#,
        r#"context.Faculties.Where(row => row.Sex == "M").Select(row => new { row.FacId }).ToList();"#,
    ));

    queries_and_results.push((
        r#"SELECT FacID FROM Faculty_participates_in INTERSECT SELECT advisor FROM Student"#,
        "context.FacultyParticipatesIns.Select(row => row.FacId).Intersect(context.Students.Select(row => row.Advisor)).ToList();",
    ));

    queries_and_results.push((
        r#"SELECT building FROM Faculty WHERE rank = 'Professor' GROUP BY building HAVING count(*) >= 10"#,
        r#"context.Faculties.Where(row => row.Rank == "Professor").GroupBy(row => new { row.Building }).Where(group => group.Count() >= 10).Select(group => new { group.Key.Building }).ToList();"#,
    ));

    queries_and_results.push((
        r#"SELECT T1.FacID FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID = T2.advisor GROUP BY T1.FacID HAVING count(*) >= 2"#,
        r#"context.Faculties.Join(context.Students, T1 => T1.FacId, T2 => T2.Advisor, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.FacId }).Where(group => group.Count() >= 2).Select(group => new { group.Key.FacId }).ToList();"#,
    ));

    queries_and_results.push((
        r#"SELECT count(DISTINCT FacID) FROM Faculty_participates_in"#,
        r#"context.FacultyParticipatesIns.Select(row => row.FacId).Distinct().Count();"#,
    ));

    // queries_and_results.push((
    //     r#"SELECT T1.stuid FROM participates_in AS T1 JOIN activity AS T2 ON T2.actid = T2.actid WHERE T2.activity_name = 'Canoeing' INTERSECT SELECT T1.stuid FROM participates_in AS T1 JOIN activity AS T2 ON T2.actid = T2.actid WHERE T2.activity_name = 'Kayaking'"#,
    //     r#""#
    // ));

    let linq_query_builder = LinqQueryBuilder::new("../entity-framework/Models/activity_1");

    for (index, (sql, expected_result)) in queries_and_results.iter().enumerate() {
        // if index != 14 {
        //     continue;
        // }

        let result = linq_query_builder.build_query(sql);

        if result == *expected_result {
            println!("Test {} passed", index + 1);
        } else {
            println!("Test {} failed", index + 1);
            println!("SQL: {}", sql);
            println!("Expected | Got");
            println!("{}\n{}", expected_result, result);
            return;
        }
    }
}

fn create_tests(sqls_and_results: &Vec<(String, String)>) {
    let mut c_sharp_code = r#"
using entity_framework.Models.activity_1;
    
    class Program {
    "#
    .to_string();

    for (index, (sql, result)) in sqls_and_results.iter().enumerate() {
        c_sharp_code.push_str(&format!(
            r#"
        static bool Test{}()
        {{
            var context = new Activity1Context();
            var linq_query = {}
            var sql_query = "{}";

            var test_passed = Tester.Test(linq_query, sql_query);

            return test_passed;
        }}
        "#,
            index,
            result,
            sql.replace("\"", "'"),
        ));
    }

    c_sharp_code.push_str(&format!(
        r#"
        static void Main()
        {{
    "#
    ));

    for index in 0..sqls_and_results.len() {
        c_sharp_code.push_str(&format!(
            r#"
            var test_passed_{} = Test{}();
            if (!test_passed_{}) {{
                Console.WriteLine("Test {} failed");
                return;
            }}
        "#,
            index, index, index, index
        ));
    }

    c_sharp_code.push_str("}}");

    println!("{}", c_sharp_code);

    let file_path: &str = "Program.cs";

    let mut file = File::create(file_path).expect("Could not create file");

    file.write_all(c_sharp_code.as_bytes())
        .expect("Could not write to file");
}

fn create_tests_to_file() {
    let path =
        Path::new("/Users/qfl1ck32/Stuff/Facultate/disertatie/Text2ORM/sql2ast/src/train_gold.sql");

    let file = File::open(&path).unwrap();
    let reader = io::BufReader::new(file);

    let mut queries: HashMap<String, Vec<String>> = HashMap::new();

    for line in reader.lines() {
        let line = line.unwrap();

        let parts: Vec<&str> = line.split_whitespace().collect();

        if parts.len() > 1 {
            // Extract the db_id which is the last part
            let db_id = parts.last().unwrap().to_string();

            // Join the query parts back into a single string (excluding the db_id)
            let query = parts[..parts.len() - 1].join(" ");

            // Insert the query into the hashmap
            queries.entry(db_id).or_insert(Vec::new()).push(query);
        }
    }

    let activity_1_queries = queries.get("activity_1").unwrap();

    let linq_query_builder = LinqQueryBuilder::new("../entity-framework/Models/activity_1");

    let mut sql_and_results: Vec<(String, String)> = Vec::new();

    for (index, query) in activity_1_queries.iter().enumerate() {
        println!("Query {}: {}", index + 1, query);

        // TODO: not handled
        if query.to_lowercase().contains("except") {
            continue;
        }

        // wrong in the dataset
        if query.to_lowercase().contains("t2.actid = t2.actid") {
            continue;
        }

        let result = linq_query_builder.build_query(query);

        println!("{}", result);

        sql_and_results.push((query.to_string(), result.to_string()));
    }

    create_tests(&sql_and_results);
}

fn main() {
    tests();
    // create_tests_to_file();
}

// TODO: handle HAVING and EXCEPT
