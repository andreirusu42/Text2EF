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
    
        ( 
            r#"SELECT count(DISTINCT FacID) FROM Faculty_participates_in"#,
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
        ),
        (
            r#"SELECT lname , age FROM Student WHERE StuID IN (SELECT StuID FROM Has_allergy WHERE Allergy = "Milk" INTERSECT SELECT StuID FROM Has_allergy WHERE Allergy = "Cat")"#,
            r#"context.Students.Where(row => context.HasAllergies.Where(row => row.Allergy == "Milk").Select(row => row.StuId).Intersect(context.HasAllergies.Where(row => row.Allergy == "Cat").Select(row => row.StuId)).Contains(row.StuId)).Select(row => new { row.Lname, row.Age }).ToList();"#
        )
    ]);

    all_queries_and_results.insert("assets_maintenance".to_string(), vec![
        (
            r#"SELECT T1.company_id , T1.company_name FROM Third_Party_Companies AS T1 JOIN Maintenance_Engineers AS T2 ON T1.company_id = T2.company_id GROUP BY T1.company_id HAVING count(*) >= 2 UNION SELECT T3.company_id , T3.company_name FROM Third_Party_Companies AS T3 JOIN Maintenance_Contracts AS T4 ON T3.company_id = T4.maintenance_contract_company_id GROUP BY T3.company_id HAVING count(*) >= 2"#,
            r#"context.ThirdPartyCompanies.Join(context.MaintenanceEngineers, T1 => T1.CompanyId, T2 => T2.CompanyId, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.CompanyId }).Where(group => group.Count() >= 2).Select(group => new { group.Key.CompanyId, group.First().T1.CompanyName }).Union(context.ThirdPartyCompanies.Join(context.MaintenanceContracts, T3 => T3.CompanyId, T4 => T4.MaintenanceContractCompanyId, (T3, T4) => new { T3, T4 }).GroupBy(row => new { row.T3.CompanyId }).Where(group => group.Count() >= 2).Select(group => new { group.Key.CompanyId, group.First().T3.CompanyName })).ToList();"#,
        ),
        (
            r#"SELECT T1.staff_name , T1.staff_id FROM Staff AS T1 JOIN Fault_Log AS T2 ON T1.staff_id = T2.recorded_by_staff_id EXCEPT SELECT T3.staff_name , T3.staff_id FROM Staff AS T3 JOIN Engineer_Visits AS T4 ON T3.staff_id = T4.contact_staff_id"#,
            r#"context.Staff.Join(context.FaultLogs, T1 => T1.StaffId, T2 => T2.RecordedByStaffId, (T1, T2) => new { T1, T2 }).Select(row => new { row.T1.StaffName, row.T1.StaffId }).Except(context.Staff.Join(context.EngineerVisits, T3 => T3.StaffId, T4 => T4.ContactStaffId, (T3, T4) => new { T3, T4 }).Select(row => new { row.T3.StaffName, row.T3.StaffId })).ToList();"#,
        ),
        (
            r#"SELECT T1.engineer_id , T1.first_name , T1.last_name FROM Maintenance_Engineers AS T1 JOIN Engineer_Visits AS T2 GROUP BY T1.engineer_id ORDER BY count(*) DESC LIMIT 1"#,
            r#"context.MaintenanceEngineers.SelectMany(T1 => context.EngineerVisits, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.EngineerId }).OrderByDescending(group => group.Count()).Select(group => new { group.Key.EngineerId, group.First().T1.FirstName, group.First().T1.LastName }).Take(1).ToList();"#,
        ),
        (
            r#"SELECT T1.first_name , T1.last_name , T1.other_details , T3.skill_description FROM Maintenance_Engineers AS T1 JOIN Engineer_Skills AS T2 ON T1.engineer_id = T2.engineer_id JOIN Skills AS T3 ON T2.skill_id = T3.skill_id"#,
            r#"context.MaintenanceEngineers.Join(context.EngineerSkills, T1 => T1.EngineerId, T2 => T2.EngineerId, (T1, T2) => new { T1, T2 }).Join(context.Skills, joined => joined.T2.SkillId, T3 => T3.SkillId, (joined, T3) => new { joined.T1, joined.T2, T3 }).Select(row => new { row.T1.FirstName, row.T1.LastName, row.T1.OtherDetails, row.T3.SkillDescription }).ToList();"#,
        )
    ]);

    all_queries_and_results.insert("baseball_1".to_string(), vec![
        (
            r#"SELECT T1.name , T1.team_id , max(T2.salary) FROM team AS T1 JOIN salary AS T2 ON T1.team_id = T2.team_id GROUP BY T1.team_id;"#,
            r#"context.Teams.Join(context.Salaries, T1 => T1.TeamId, T2 => T2.TeamId, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.TeamId }).Select(group => new { group.First().T1.Name, group.Key.TeamId, MaxSalary1 = group.Max(row => row.T2.Salary1) }).ToList();"#,
        ),
        (
            r#"SELECT count(*) FROM ( SELECT * FROM postseason AS T1 JOIN team AS T2 ON T1.team_id_winner = T2.team_id_br WHERE T2.name = 'Boston Red Stockings' UNION SELECT * FROM postseason AS T1 JOIN team AS T2 ON T1.team_id_loser = T2.team_id_br WHERE T2.name = 'Boston Red Stockings' );"#,
            r#"context.Postseasons.Join(context.Teams, T1 => T1.TeamIdWinner, T2 => T2.TeamIdBr, (T1, T2) => new { T1, T2 }).Where(row => row.T2.Name == "Boston Red Stockings").Union(context.Postseasons.Join(context.Teams, T1 => T1.TeamIdLoser, T2 => T2.TeamIdBr, (T1, T2) => new { T1, T2 }).Where(row => row.T2.Name == "Boston Red Stockings")).Count();"#,
        ),
        (
            r#"SELECT sum(T1.attendance) FROM home_game AS T1 JOIN team AS T2 ON T1.team_id = T2.team_id_br WHERE T2.name = 'Boston Red Stockings' AND T1.year BETWEEN 2000 AND 2010;"#,
            r#"context.HomeGames.Join(context.Teams, T1 => T1.TeamId, T2 => T2.TeamIdBr, (T1, T2) => new { T1, T2 }).Where(row => row.T2.Name == "Boston Red Stockings" && row.T1.Year >= 2000 && row.T1.Year <= 2010).Select(row => row.T1.Attendance).Sum();"#,
        ),
        (
            r#"SELECT name_first , name_last FROM player AS T1 JOIN all_star AS T2 ON T1.player_id = T2.player_id WHERE YEAR = 1998"#,
            r#"context.Players.Join(context.AllStars, T1 => T1.PlayerId, T2 => T2.PlayerId, (T1, T2) => new { T1, T2 }).Where(row => row.T2.Year == 1998).Select(row => new { row.T1.NameFirst, row.T1.NameLast }).ToList();"#,
        )
    ]);

    all_queries_and_results.insert("behavior_monitoring".to_string(), vec![
        (
            r#"SELECT max(monthly_rental) , min(monthly_rental) FROM Student_Addresses"#,
            r#"context.StudentAddresses.GroupBy(row => 1).Select(group => new { MaxMonthlyRental = group.Max(row => (double) row.MonthlyRental), MinMonthlyRental = group.Min(row => (double) row.MonthlyRental) }).ToList();"#
        ),
        (
            r#"SELECT * FROM Student_Addresses ORDER BY monthly_rental DESC"#,
            r#"context.StudentAddresses.OrderByDescending(row => (double) row.MonthlyRental).ToList();"#
        ),
        (
            r#"SELECT T1.student_id , T2.first_name FROM Student_Addresses AS T1 JOIN Students AS T2 ON T1.student_id = T2.student_id GROUP BY T1.student_id ORDER BY AVG(monthly_rental) DESC LIMIT 1"#,
            r#"context.StudentAddresses.Join(context.Students, T1 => T1.StudentId, T2 => T2.StudentId, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.StudentId }).OrderByDescending(group => group.Average(row => (double) row.T1.MonthlyRental)).Select(group => new { group.Key.StudentId, group.First().T2.FirstName }).Take(1).ToList();"#
        )
    ]);

    all_queries_and_results.insert("bike_1".to_string(), vec![
        (
            r#"SELECT id FROM station WHERE city = "San Francisco" INTERSECT SELECT station_id FROM status GROUP BY station_id HAVING avg(bikes_available) > 10"#,
            r#"context.Stations.Where(row => row.City == "San Francisco").Select(row => row.Id).Intersect(context.Statuses.GroupBy(row => new { row.StationId }).Where(group => group.Average(row => row.BikesAvailable) > 10).Select(group => group.Key.StationId)).ToList();"#
        ),
        (
            r#"SELECT T1.name , T1.id FROM station AS T1 JOIN status AS T2 ON T1.id = T2.station_id GROUP BY T2.station_id HAVING avg(T2.bikes_available) > 14 UNION SELECT name , id FROM station WHERE installation_date LIKE "12/%""#,
            r#"context.Stations.Join(context.Statuses, T1 => T1.Id, T2 => T2.StationId, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T2.StationId }).Where(group => group.Average(row => row.T2.BikesAvailable) > 14).Select(group => new { group.First().T1.Name, group.First().T1.Id }).Union(context.Stations.Where(row => EF.Functions.Like(row.InstallationDate, "12/%")).Select(row => new { row.Name, row.Id })).ToList();"#
        )
    ]);

    
    for (db_name, queries_and_results) in all_queries_and_results.iter() {
       
        let linq_query_builder = LinqQueryBuilder::new(&format!("../entity-framework/Models/{}", db_name));

        for (index, (sql, expected_result)) in queries_and_results.iter().enumerate() {
            // if db_name != "bike_1" || index != 1 {
            //     continue;
            // }

            println!("Running test {} | DB: {} | SQL: {}", index, db_name, sql);

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
    let db_names = vec![
    // "activity_1".to_string(),
    // "apartment_rentals".to_string(),
    // "allergy_1".to_string(), 
    // "assets_maintenance".to_string(),
    // "baseball_1".to_string(),
    // "behavior_monitoring".to_string(),
    "bike_1".to_string()
    ];

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

            // wrong in the dataset, allergy_1
            if query.to_lowercase().contains("t2.allergytype") { // the field should be allergy_type
                continue;
            }

            // wrong in the dataset, assets_maintenance
            if query.to_lowercase().contains("ref_company_types") {
                continue;
            }
    
            println!("Processing query {} for {}", index, db_name);
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
    tests();
    // create_tests_to_file();
}
