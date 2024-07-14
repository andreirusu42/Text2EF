use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, Write};
use std::path::Path;

mod linq_query_builder;
mod schema_mapping;

use linq_query_builder::LinqQueryBuilder;

fn tests() {
    let mut all_queries_and_results: HashMap<String, Vec<(&str, &str)>> = HashMap::new();

    all_queries_and_results.insert("activity_1".to_string(), vec!(
        (
            r#"SELECT rank FROM Faculty"#,
            r#"context.Faculties.Select(row => new { row.Rank }).ToList();"#,
        ),
    
        (
            r#"SELECT DISTINCT rank FROM Faculty"#,
            r#"context.Faculties.Select(row => new { row.Rank }).Distinct().ToList();"#,
        ),
    
        (
            r#"SELECT T1.fname, T1.lname FROM Faculty AS T1 JOIN Student AS T2 ON T2.advisor = T1.FacID WHERE T2.fname = "Linda" AND T2.lname = "Smith""#,
            r#"context.Faculties.Join(context.Students, T1 => T1.FacId, T2 => T2.Advisor, (T1, T2) => new { T1, T2 }).Where(row => row.T2.Fname == "Linda" && row.T2.Lname == "Smith").Select(row => new { row.T1.Fname, row.T1.Lname }).ToList();"#,
        ),
    
        (
            r#"SELECT COUNT(*), rank FROM Faculty GROUP BY rank"#,
            r#"context.Faculties.GroupBy(row => new { row.Rank }).Select(group => new { Count = group.Count(), group.Key.Rank }).ToList();"#,
        ),
    
        (
            r#"SELECT T1.FacID FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID  =  T2.advisor GROUP BY T1.FacID"#,
            r#"context.Faculties.Join(context.Students, T1 => T1.FacId, T2 => T2.Advisor, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.FacId }).Select(group => new { group.Key.FacId }).ToList();"#,
        ),
    
        (
            r#"SELECT T1.activity_name FROM Activity AS T1 JOIN Faculty_participates_in AS T2 ON T1.actID  =  T2.actID GROUP BY T1.actID ORDER BY count(*) DESC LIMIT 1"#,
            "context.Activities.Join(context.FacultyParticipatesIns, T1 => T1.Actid, T2 => T2.Actid, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.Actid }).OrderByDescending(group => group.Count()).Select(group => new { group.First().T1.ActivityName }).Take(1).ToList();"
        ),
    
        (
            r#"SELECT T1.stuid FROM participates_in AS T1 JOIN activity AS T2 ON T1.actid  =  T2.actid WHERE T2.activity_name  =  'Canoeing' INTERSECT SELECT T1.stuid FROM participates_in AS T1 JOIN activity AS T2 ON T1.actid  =  T2.actid WHERE T2.activity_name  =  'Kayaking'"#,
            r#"context.ParticipatesIns.Join(context.Activities, T1 => T1.Actid, T2 => T2.Actid, (T1, T2) => new { T1, T2 }).Where(row => row.T2.ActivityName == "Canoeing").Select(row => row.T1.Stuid).Intersect(context.ParticipatesIns.Join(context.Activities, T1 => T1.Actid, T2 => T2.Actid, (T1, T2) => new { T1, T2 }).Where(row => row.T2.ActivityName == "Kayaking").Select(row => row.T1.Stuid)).ToList();"#
        ),
    
        (
            r#"SELECT T3.activity_name FROM Faculty AS T1 JOIN Faculty_participates_in AS T2 ON T2.facID  =  T1.facID JOIN Activity AS T3 ON T3.actid  =  T2.actid WHERE T1.fname  =  "Mark" AND T1.lname  =  "Giuliano""#,
            r#"context.Faculties.Join(context.FacultyParticipatesIns, T1 => T1.FacId, T2 => T2.FacId, (T1, T2) => new { T1, T2 }).Join(context.Activities, joined => joined.T2.Actid, T3 => T3.Actid, (joined, T3) => new { joined.T1, joined.T2, T3 }).Where(row => row.T1.Fname == "Mark" && row.T1.Lname == "Giuliano").Select(row => new { row.T3.ActivityName }).ToList();"#
        ),
    
        (
            r#"SELECT T1.rank ,  count(*) FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID  =  T2.advisor GROUP BY T1.rank"#,
            r#"context.Faculties.Join(context.Students, T1 => T1.FacId, T2 => T2.Advisor, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.Rank }).Select(group => new { group.Key.Rank, Count = group.Count() }).ToList();"#,
        ),
    
    (
            r#"SELECT T1.fname , T1.lname FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID = T2.advisor GROUP BY T1.FacID ORDER BY count(*) DESC LIMIT 1"#,
            r#"context.Faculties.Join(context.Students, T1 => T1.FacId, T2 => T2.Advisor, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.FacId }).OrderByDescending(group => group.Count()).Select(group => new { group.First().T1.Fname, group.First().T1.Lname }).Take(1).ToList();"#
        ),
    
        (
            r#"SELECT FacID FROM Faculty WHERE Sex = 'M'"#,
            r#"context.Faculties.Where(row => row.Sex == "M").Select(row => new { row.FacId }).ToList();"#,
        ),
    
        (
            r#"SELECT FacID FROM Faculty_participates_in INTERSECT SELECT advisor FROM Student"#,
            "context.FacultyParticipatesIns.Select(row => row.FacId).Intersect(context.Students.Select(row => row.Advisor)).ToList();",
        ),

    
       (
            r#"SELECT building FROM Faculty WHERE rank = 'Professor' GROUP BY building HAVING count(*) >= 10"#,
            r#"context.Faculties.Where(row => row.Rank == "Professor").GroupBy(row => new { row.Building }).Where(group => group.Count() >= 10).Select(group => new { group.Key.Building }).ToList();"#,
        ),
    
        (
            r#"SELECT T1.FacID FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID = T2.advisor GROUP BY T1.FacID HAVING count(*) >= 2"#,
            r#"context.Faculties.Join(context.Students, T1 => T1.FacId, T2 => T2.Advisor, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.FacId }).Where(group => group.Count() >= 2).Select(group => new { group.Key.FacId }).ToList();"#,
        ),
    
(            r#"SELECT count(DISTINCT FacID) FROM Faculty_participates_in"#,
            r#"context.FacultyParticipatesIns.Select(row => row.FacId).Distinct().Count();"#,
        ),
    
        (
            r#"SELECT count(*) FROM Faculty_participates_in"#,
            r#"context.FacultyParticipatesIns.Count();"#,
        )
    ));

    all_queries_and_results.insert("apartment_rentals".to_string(), vec!(
        (
            r#"SELECT building_full_name FROM Apartment_Buildings WHERE building_full_name LIKE "%court%""#,
            r#"context.ApartmentBuildings.Where(row => EF.Functions.Like(row.BuildingFullName, "%court%")).Select(row => new { row.BuildingFullName }).ToList();"#,
        ),
        (
            r#"SELECT min(bathroom_count) , max(bathroom_count) FROM Apartments"#,
            r#"context.Apartments.GroupBy(row => 1).Select(group => new { MinBathroomCount = group.Min(row => row.BathroomCount), MaxBathroomCount = group.Max(row => row.BathroomCount) }).ToList();"#,
        )
    ));

    
    for (db_name, queries_and_results) in all_queries_and_results.iter() {
        // if db_name != "apartment_rentals" {
        //     continue
        // }

        let linq_query_builder = LinqQueryBuilder::new(&format!("../entity-framework/Models/{}", db_name));

        for (index, (sql, expected_result)) in queries_and_results.iter().enumerate() {
            // if index != 1 {
            //     continue;
            // }

            println!("Running test {} | DB: {} | SQL: {}", index + 1, db_name, sql);

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
}

fn create_tests(sqls_and_results: &Vec<(String, String)>, db_name: &str, context_name: &str) {
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

    // let db_name = "activity_1";
    // let context_name = "Activity1Context";
    let db_name = "apartment_rentals";

    let queries = queries.get(db_name).unwrap();

    let linq_query_builder =
        LinqQueryBuilder::new(&format!("../entity-framework/Models/{}", db_name));

    let mut sql_and_results: Vec<(String, String)> = Vec::new();

    for (index, query) in queries.iter().enumerate() {
        // wrong in the dataset
        if query.to_lowercase().contains("t2.actid = t2.actid") {
            continue;
        }

        println!("Processing query {}", index + 1);
        println!("{}", query);

        let result = linq_query_builder.build_query(query);

        sql_and_results.push((query.to_string(), result.to_string()));
    }

    // TODO: shouldn't get it from linq_query_builder
    create_tests(&sql_and_results, db_name, &linq_query_builder.schema_mapping.context);
}

fn main() {
    tests();
    // create_tests_to_file();
}
