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
        ),
        (
            r#"SELECT avg(bedroom_count) FROM Apartments"#,
            r#"context.Apartments.Select(row => row.BedroomCount).Average();"#,
        ),
        (
            r#"SELECT avg(room_count) FROM Apartment_Bookings AS T1 JOIN Apartments AS T2 ON T1.apt_id = T2.apt_id WHERE T1.booking_status_code = "Provisional""#,
            r#"context.ApartmentBookings.Join(context.Apartments, T1 => T1.AptId, T2 => T2.AptId, (T1, T2) => new { T1, T2 }).Where(row => row.T1.BookingStatusCode == "Provisional").Select(row => row.T2.RoomCount).Average();"#,
        ),
        (
            r#"SELECT sum(T2.room_count) FROM Apartment_Facilities AS T1 JOIN Apartments AS T2 ON T1.apt_id = T2.apt_id WHERE T1.facility_code = "Gym""#,
            r#"context.ApartmentFacilities.Join(context.Apartments, T1 => T1.AptId, T2 => T2.AptId, (T1, T2) => new { T1, T2 }).Where(row => row.T1.FacilityCode == "Gym").Select(row => row.T2.RoomCount).Sum();"#,
        ),
        (
            r#"SELECT apt_number FROM Apartments ORDER BY room_count ASC"#,
            r#"context.Apartments.OrderBy(row => row.RoomCount).Select(row => new { row.AptNumber }).ToList();"#,
        ),
        (
            r#"SELECT apt_type_code FROM Apartments GROUP BY apt_type_code ORDER BY avg(room_count) DESC LIMIT 3"#,
            r#"context.Apartments.GroupBy(row => new { row.AptTypeCode }).OrderByDescending(group => group.Average(row => row.RoomCount)).Select(group => new { group.Key.AptTypeCode }).Take(3).ToList();"#,
        ),
        (
            r#"SELECT count(*) FROM Apartments WHERE apt_id NOT IN (SELECT apt_id FROM Apartment_Facilities)"#,
            r#"context.Apartments.Where(row => !context.ApartmentFacilities.Select(row => row.AptId).Contains(row.AptId)).Count();"#,
        ),
        (
            r#"SELECT apt_type_code , bathroom_count , bedroom_count FROM Apartments GROUP BY apt_type_code ORDER BY sum(room_count) DESC LIMIT 1"#,
            r#"context.Apartments.GroupBy(row => new { row.AptTypeCode }).OrderByDescending(group => group.Sum(row => row.RoomCount)).Select(group => new { group.Key.AptTypeCode, group.First().BathroomCount, group.First().BedroomCount }).Take(1).ToList();"#,
        ),
        (
            r#"SELECT T1.apt_number FROM Apartments AS T1 JOIN View_Unit_Status AS T2 ON T1.apt_id = T2.apt_id WHERE T2.available_yn = 0 INTERSECT SELECT T1.apt_number FROM Apartments AS T1 JOIN View_Unit_Status AS T2 ON T1.apt_id = T2.apt_id WHERE T2.available_yn = 1"#,
            r#"context.Apartments.Join(context.ViewUnitStatuses, T1 => T1.AptId, T2 => T2.AptId, (T1, T2) => new { T1, T2 }).Where(row => row.T2.AvailableYn == false).Select(row => row.T1.AptNumber).Intersect(context.Apartments.Join(context.ViewUnitStatuses, T1 => T1.AptId, T2 => T2.AptId, (T1, T2) => new { T1, T2 }).Where(row => row.T2.AvailableYn == true).Select(row => row.T1.AptNumber)).ToList();"#,
        )
    ));

    all_queries_and_results.insert("allergy_1".to_string(), vec![
        (
            r#"SELECT LName FROM Student WHERE age = (SELECT min(age) FROM Student)"#,
            r#"context.Students.Where(row => row.Age == context.Students.Select(row => row.Age).Min()).Select(row => new { row.Lname }).ToList();"#,
        )
    ]);

    
    for (db_name, queries_and_results) in all_queries_and_results.iter() {
        if db_name != "allergy_1" {
            continue
        }

        let linq_query_builder = LinqQueryBuilder::new(&format!("../entity-framework/Models/{}", db_name));

        for (index, (sql, expected_result)) in queries_and_results.iter().enumerate() {
            if index != 0 {
                continue;
            }

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

fn create_tests_to_file() {
    let db_names = vec!["activity_1".to_string(), "apartment_rentals".to_string(), "allergy_1".to_string()];

        // TODO: EF might not be required tho. to simplify things we could simply run a lint at the end
        let mut c_sharp_code = String::new();
        let mut main_code = String :: new();

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

    for db_name in &db_names {
        let queries = queries.get(db_name.as_str()).unwrap();

        let linq_query_builder =
            LinqQueryBuilder::new(&format!("../entity-framework/Models/{}", db_name));

            // TODO: not from here pls
        let context_name = &linq_query_builder.schema_mapping.context;
    
        let mut sql_and_results: Vec<(String, String)> = Vec::new();
    
        for (index, query) in queries.iter().enumerate() {
            // wrong in the dataset, activity_1
            if query.to_lowercase().contains("t2.actid = t2.actid") {
                continue;
            }
    
            // wrong in the dataset, apartment_rentals
            if query.to_lowercase().contains("t1.booking_start_date , t1.booking_start_date") {
                continue;
            }
    
            println!("Processing query {}", index + 1);
            println!("{}", query);
    
            let result = linq_query_builder.build_query(query);
    
            sql_and_results.push((query.to_string(), result.to_string()));
        }

        c_sharp_code.push_str(&format!("\nstatic void Test{}() {{ var context = new {}(); \n var tests = new (object, string)[] {{", context_name, context_name));

        let result = sql_and_results
        .iter()
        .map(|(a, b)| format!("({}, \"{}\"),", b.trim_end_matches(";"), a.escape_debug()))
        .collect::<Vec<String>>()
        .join("\n");

        c_sharp_code.push_str(&result);

        c_sharp_code.push_str(&format!("}};\n\n for (int i = 0; i < tests.Length; ++i) {{ var (linq_query, sql_query) = tests[i];\n\nvar test_passed = Tester.Test(linq_query, sql_query, context); \n if (!test_passed) {{ Console.WriteLine($\"Test {{ i + 1 }} failed.\"); }} }} }}"));
        
        main_code.push_str(&format!("Console.WriteLine(\"Running tests for {}\");\n Test{}();\n", context_name, context_name));
    }

    c_sharp_code.push_str(&format!("\n\nstatic void Main() {{ {} }}", main_code));
   

    c_sharp_code.push_str("}");

    let file_path: &str = "Program.cs";

    let mut file = File::create(file_path).expect("Could not create file");

    file.write_all(c_sharp_code.as_bytes())
        .expect("Could not write to file");

}

fn main() {
    // tests();
    create_tests_to_file();
}
