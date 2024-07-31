use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufRead, Write};
use std::path::Path;

mod linq_query_builder;
mod schema_mapping;
mod case_insensitive_hashmap;
mod case_insensitive_hashset;
mod determine_join_order;

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
            r#"context.Apartments.GroupBy(row => 1).Select(group => new { MinBathroomCount = group.Select(row => row.BathroomCount).Min(), MaxBathroomCount = group.Select(row => row.BathroomCount).Max() }).ToList();"#,
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
            r#"context.Teams.Join(context.Salaries, T1 => T1.TeamId, T2 => T2.TeamId, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.TeamId }).Select(group => new { group.OrderByDescending(row => row.T2.Salary1).First().T1.Name, group.Key.TeamId, MaxSalary1 = group.Select(row => row.T2.Salary1).Max() }).ToList();"#,
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
            r#"context.StudentAddresses.GroupBy(row => 1).Select(group => new { MaxMonthlyRental = group.Select(row => (double) row.MonthlyRental).Max(), MinMonthlyRental = group.Select(row => (double) row.MonthlyRental).Min() }).ToList();"#
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
        ),
        ( 
            r#"SELECT date , max_temperature_f - min_temperature_f FROM weather ORDER BY max_temperature_f - min_temperature_f LIMIT 1"#,
            r#"context.Weathers.Select(row => new { row.Date, Diff = row.MaxTemperatureF - row.MinTemperatureF }).OrderBy(row => row.Diff).Take(1).ToList();"#
        ),
        (
            r#"SELECT count(*) FROM station AS T1 JOIN trip AS T2 JOIN trip AS T4 JOIN station AS T3 ON T1.id = T2.start_station_id AND T2.id = T4.id AND T3.id = T4.end_station_id WHERE T1.city = "Mountain View" AND T3.city = "Palo Alto""#,
            r#"context.Stations.Join(context.Trips, T1 => T1.Id, T2 => T2.StartStationId, (T1, T2) => new { T1, T2 }).Join(context.Trips, joined => joined.T2.Id, T4 => T4.Id, (joined, T4) => new { joined.T1, joined.T2, T4 }).Join(context.Stations, joined => joined.T4.EndStationId, T3 => T3.Id, (joined, T3) => new { joined.T1, joined.T2, joined.T4, T3 }).Where(row => row.T1.City == "Mountain View" && row.T3.City == "Palo Alto").Count();"#,
        )
    ]);

    all_queries_and_results.insert("browser_web".to_string(), vec![
        (
            r#"SELECT T2.name , T3.name FROM accelerator_compatible_browser AS T1 JOIN browser AS T2 ON T1.browser_id = T2.id JOIN web_client_accelerator AS T3 ON T1.accelerator_id = T3.id ORDER BY T1.compatible_since_year DESC"#,
            r#"context.AcceleratorCompatibleBrowsers.Join(context.Browsers, T1 => T1.BrowserId, T2 => T2.Id, (T1, T2) => new { T1, T2 }).Join(context.WebClientAccelerators, joined => joined.T1.AcceleratorId, T3 => T3.Id, (joined, T3) => new { joined.T1, joined.T2, T3 }).OrderByDescending(row => row.T1.CompatibleSinceYear).Select(row => new { T2Name = row.T2.Name, T3Name = row.T3.Name }).ToList();"#,
        )
    ]);

    all_queries_and_results.insert("candidate_poll".to_string(), vec![
        (
            r#"SELECT t1.name , t1.sex , min(oppose_rate) FROM people AS t1 JOIN candidate AS t2 ON t1.people_id = t2.people_id GROUP BY t1.sex"#,
            r#"context.People.Join(context.Candidates, t1 => t1.PeopleId, t2 => t2.PeopleId, (t1, t2) => new { t1, t2 }).GroupBy(row => new { row.t1.Sex }).Select(group => new { group.OrderBy(row => row.t2.OpposeRate).First().t1.Name, group.Key.Sex, MinOpposeRate = group.Select(row => row.t2.OpposeRate).Min() }).ToList();"#
        )
    ]);

    all_queries_and_results.insert("chinook_1".to_string(), vec![
        (
            r#"SELECT distinct(BillingCountry) FROM INVOICE"#,
            r#"context.Invoices.Select(row => new { row.BillingCountry }).Distinct().ToList();"#,
        ),
        (
            r#"SELECT T2.Name , T1.ArtistId FROM ALBUM AS T1 JOIN ARTIST AS T2 ON T1.ArtistId = T2.ArtistID WHERE T2.Name != "Hello" GROUP BY T1.ArtistId HAVING COUNT(*) >= 3 ORDER BY T2.Name"#,
            r#"context.Albums.Join(context.Artists, T1 => T1.ArtistId, T2 => T2.ArtistId, (T1, T2) => new { T1, T2 }).Where(row => row.T2.Name != "Hello").GroupBy(row => new { row.T1.ArtistId }).Where(group => group.Count() >= 3).OrderBy(group => group.First().T2.Name).Select(group => new { group.First().T2.Name, group.Key.ArtistId }).ToList();"#,
        ),
        (
            r#"SELECT AVG(UnitPrice) FROM TRACK"#,
            r#"context.Tracks.Select(row => (double) row.UnitPrice).Average();"#,
        )
    ]);

    all_queries_and_results.insert("college_1".to_string(), vec![
        (
            r#"SELECT count(DISTINCT dept_address) , school_code FROM department GROUP BY school_code"#,
            r#"context.Departments.GroupBy(row => new { row.SchoolCode }).Select(group => new { CountDistinctDeptAddress = group.Select(row => row.DeptAddress).Distinct().Count(), group.Key.SchoolCode }).ToList();"#,
        ),
        (
            r#"SELECT count(DISTINCT dept_name) , school_code FROM department GROUP BY school_code HAVING count(DISTINCT dept_name) < 5"#,
            r#"context.Departments.GroupBy(row => new { row.SchoolCode }).Select(group => new { CountDistinctDeptName = group.Select(row => row.DeptName).Distinct().Count(), group.Key.SchoolCode }).Where(group => group.CountDistinctDeptName < 5).ToList();"#,
        ),
        (
            r#"SELECT count(*) , dept_code FROM CLASS AS T1 JOIN course AS T2 ON T1.crs_code = T2.crs_code GROUP BY dept_code"#,
            r#"context.Classes.Join(context.Courses, T1 => T1.CrsCode, T2 => T2.CrsCode, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T2.DeptCode }).Select(group => new { Count = group.Count(), group.Key.DeptCode }).ToList();"#,
        )
    ]);

    all_queries_and_results.insert("cre_Theme_park".to_string(), vec![
        (
            r#"SELECT T1.Name FROM Tourist_Attractions AS T1 JOIN Tourist_Attraction_Features AS T2 ON T1.tourist_attraction_id = T2.tourist_attraction_id JOIN Features AS T3 ON T2.Feature_ID = T3.Feature_ID WHERE T3.feature_Details = 'park' UNION SELECT T1.Name FROM Tourist_Attractions AS T1 JOIN Tourist_Attraction_Features AS T2 ON T1.tourist_attraction_id = T2.tourist_attraction_id JOIN Features AS T3 ON T2.Feature_ID = T3.Feature_ID WHERE T3.feature_Details = 'shopping'"#,
            r#"context.TouristAttractions.Join(context.TouristAttractionFeature, T1 => T1.TouristAttractionId, T2 => T2.TouristAttractionId, (T1, T2) => new { T1, T2 }).Join(context.Features, joined => joined.T2.FeatureId, T3 => T3.FeatureId, (joined, T3) => new { joined.T1, joined.T2, T3 }).Where(row => row.T3.FeatureDetails == "park").Select(row => row.T1.Name).Union(context.TouristAttractions.Join(context.TouristAttractionFeature, T1 => T1.TouristAttractionId, T2 => T2.TouristAttractionId, (T1, T2) => new { T1, T2 }).Join(context.Features, joined => joined.T2.FeatureId, T3 => T3.FeatureId, (joined, T3) => new { joined.T1, joined.T2, T3 }).Where(row => row.T3.FeatureDetails == "shopping").Select(row => row.T1.Name)).ToList();"#,
        )
    ]);

    all_queries_and_results.insert("college_2".to_string(), vec![
        (
            r#"SELECT count(DISTINCT T2.id) , count(DISTINCT T3.id) , T3.dept_name FROM department AS T1 JOIN student AS T2 ON T1.dept_name = T2.dept_name JOIN instructor AS T3 ON T1.dept_name = T3.dept_name GROUP BY T3.dept_name"#,
            r#"context.Departments.Join(context.Students, T1 => T1.DeptName, T2 => T2.DeptName, (T1, T2) => new { T1, T2 }).Join(context.Instructors, joined => joined.T1.DeptName, T3 => T3.DeptName, (joined, T3) => new { joined.T1, joined.T2, T3 }).GroupBy(row => new { row.T3.DeptName }).Select(group => new { CountDistinctIdT2 = group.Select(row => row.T2.Id).Distinct().Count(), CountDistinctIdT3 = group.Select(row => row.T3.Id).Distinct().Count(), group.Key.DeptName }).ToList();"#,
        ),
        (
            r#"SELECT T1.name FROM student AS T1 JOIN takes AS T2 ON T1.id = T2.id WHERE T2.course_id IN (SELECT T4.prereq_id FROM course AS T3 JOIN prereq AS T4 ON T3.course_id = T4.course_id WHERE T3.title = 'International Finance')"#,
            r#"context.Students.Join(context.Takes, T1 => T1.Id, T2 => T2.Id, (T1, T2) => new { T1, T2 }).Where(row => context.Courses.Join(context.Prereq, T3 => T3.CourseId, T4 => T4.CourseId, (T3, T4) => new { T3, T4 }).Where(row => row.T3.Title == "International Finance").Select(row => row.T4.PrereqId).Contains(row.T2.CourseId)).Select(row => new { row.T1.Name }).ToList();"#,
        ),
        (
            r#"SELECT min(salary) , dept_name FROM instructor GROUP BY dept_name HAVING avg(salary) > (SELECT avg(salary) FROM instructor)"#,
            r#"context.Instructors.GroupBy(row => new { row.DeptName }).Where(group => group.Average(row => row.Salary) > context.Instructors.Select(row => row.Salary).Average()).Select(group => new { MinSalary = group.Select(row => row.Salary).Min(), group.Key.DeptName }).ToList();"#,
        )
    ]);

    all_queries_and_results.insert("cre_Doc_Control_Systems".to_string(), vec![
        (
            r#"SELECT Roles.role_description , count(Employees.employee_id) FROM ROLES JOIN Employees ON Employees.role_code = Roles.role_code GROUP BY Employees.role_code HAVING count(Employees.employee_id) > 1;"#,
            r#"context.Roles.Join(context.Employees, Roles => Roles.RoleCode, Employees => Employees.RoleCode, (Roles, Employees) => new { Roles, Employees }).GroupBy(row => new { row.Employees.RoleCode }).Select(group => new { group.First().Roles.RoleDescription, CountEmployeeId = group.Select(row => row.Employees.EmployeeId).Count() }).Where(group => group.CountEmployeeId > 1).ToList();"#,
        ),
        (
            r#"SELECT Ref_Document_Status.document_status_description FROM Ref_Document_Status JOIN Documents ON Documents.document_status_code = Ref_Document_Status.document_status_code WHERE Documents.document_id = 1;"#,
            r#"context.RefDocumentStatuses.Join(context.Documents, RefDocumentStatuses => RefDocumentStatuses.DocumentStatusCode, Documents => Documents.DocumentStatusCode, (RefDocumentStatuses, Documents) => new { RefDocumentStatuses, Documents }).Where(row => row.Documents.DocumentId == 1).Select(row => new { row.RefDocumentStatuses.DocumentStatusDescription }).ToList();"#,
        ),
        (
            r#"SELECT Addresses.address_details FROM Addresses JOIN Documents_Mailed ON Documents_Mailed.mailed_to_address_id = Addresses.address_id WHERE document_id = 4;"#,
            r#"context.Addresses.Join(context.DocumentsMaileds, Addresses => Addresses.AddressId, DocumentsMaileds => DocumentsMaileds.MailedToAddressId, (Addresses, DocumentsMaileds) => new { Addresses, DocumentsMaileds }).Where(row => row.DocumentsMaileds.DocumentId == 4).Select(row => new { row.Addresses.AddressDetails }).ToList();"#,
        ),
        (
            r#"SELECT Employees.employee_name FROM Employees JOIN Circulation_History ON Circulation_History.employee_id = Employees.employee_id WHERE Circulation_History.document_id = 1;"#,
            r#"context.Employees.Join(context.CirculationHistory, Employees => Employees.EmployeeId, CirculationHistory => CirculationHistory.EmployeeId, (Employees, CirculationHistory) => new { Employees, CirculationHistory }).Where(row => row.CirculationHistory.DocumentId == 1).Select(row => new { row.Employees.EmployeeName }).ToList();"#,
        ),
        (
            r#"SELECT document_id , count(copy_number) FROM Draft_Copies GROUP BY document_id ORDER BY count(copy_number) DESC LIMIT 1;"#,
            r#"context.DraftCopies.GroupBy(row => new { row.DocumentId }).Select(group => new { group.Key.DocumentId, CountCopyNumber = group.Select(row => row.CopyNumber).Count() }).OrderByDescending(group => group.CountCopyNumber).Take(1).ToList();"#,
        ),
        (
            r#"SELECT document_id , count(T1.copy_number) FROM Draft_Copies AS T1 GROUP BY document_id ORDER BY count(T1.copy_number) DESC LIMIT 1;"#,
            r#"context.DraftCopies.GroupBy(row => new { row.T1.DocumentId }).Select(group => new { group.Key.DocumentId, CountCopyNumber = group.Select(row => row.T1.CopyNumber).Count() }).OrderByDescending(group => group.CountCopyNumber).Take(1).ToList();"#,
        )
    ]);

    all_queries_and_results.insert("csu_1".to_string(), vec![
        (
            r#"SELECT degrees FROM campuses AS T1 JOIN degrees AS T2 ON t1.id = t2.campus WHERE t1.campus = "San Jose State University" AND t2.year = 2000"#,
            r#"context.Campuses.Join(context.Degrees, T1 => T1.Id, T2 => T2.Campus, (T1, T2) => new { T1, T2 }).Where(row => row.T1.Campus1 == "San Jose State University" && row.T2.Year == 2000).Select(row => new { row.T2.Degrees }).ToList();"#,
        ),
        (
            r#"SELECT T2.faculty FROM campuses AS T1 JOIN faculty AS T2 ON T1.id = t2.campus JOIN degrees AS T3 ON t1.id = t3.campus AND t3.year = t2.year WHERE t2.year = 2002 ORDER BY t3.degrees DESC LIMIT 1"#,
            r#"context.Campuses.Join(context.Faculties, T1 => T1.Id, T2 => T2.Campus, (T1, T2) => new { T1, T2 }).Join(context.Degrees, joined => new { Pair1 = joined.T1.Id, Pair2 = joined.T2.Year }, T3 => new { Pair1 = T3.Campus, Pair2 = T3.Year }, (joined, T3) => new { joined.T1, joined.T2, T3 }).Where(row => row.T2.Year == 2002).OrderByDescending(row => row.T3.Degrees).Select(row => new { row.T2.Faculty1 }).Take(1).ToList();"#,
        )
    ]);

    all_queries_and_results.insert("county_public_safety".to_string(), vec![
        (
            r#"SELECT name FROM city WHERE county_ID = (SELECT county_ID FROM county_public_safety ORDER BY Police_officers DESC LIMIT 1)"#,
            r#"context.Cities.Where(row => row.CountyId == context.CountyPublicSafeties.OrderByDescending(row => row.PoliceOfficers).Select(row => row.CountyId).Take(1).First()).Select(row => new { row.Name }).ToList();"#,
        )
    ]);

    all_queries_and_results.insert("cre_docs_and_epenses".to_string(), vec![
        (
            r#"SELECT max(Account_details) FROM Accounts UNION SELECT Account_details FROM Accounts WHERE Account_details LIKE "%5%""#,
            r#"new List<string> { context.Accounts.Select(row => row.AccountDetails.ToString()).Max() }.Union(context.Accounts.Where(row => EF.Functions.Like(row.AccountDetails.ToString(), "%5%")).Select(row => row.AccountDetails.ToString())).ToList();"#
        )
    ]);

    all_queries_and_results.insert("cre_theme_park".to_string(), vec![
        (
            r#"SELECT avg(price_range) FROM HOTELS WHERE star_rating_code = "5" AND pets_allowed_yn = 1"#,
            r#"context.Hotels.Where(row => row.StarRatingCode == "5" && row.PetsAllowedYn == true).Select(row => row.PriceRange).Average();"#,
        )
    ]);

    all_queries_and_results.insert("college_3".to_string(), vec![
        (
            r#"SELECT CName FROM COURSE WHERE Credits = 3 UNION SELECT CName FROM COURSE WHERE Credits = 1 AND Hours = 4"#,
            r#"context.Courses.Where(row => row.Credits == 3).Select(row => row.Cname).Union(context.Courses.Where(row => row.Credits == 1 && row.Hours == 4).Select(row => row.Cname)).ToList();"#,
        )
    ]);

    all_queries_and_results.insert("customer_complaints".to_string(), vec![
        (
            r#"SELECT email_address , phone_number FROM customers ORDER BY email_address , phone_number"#,
            r#"context.Customers.OrderBy(row => row.EmailAddress).ThenBy(row => row.PhoneNumber).Select(row => new { row.EmailAddress, row.PhoneNumber }).ToList();"#,
        )
    ]);

    all_queries_and_results.insert("college_3".to_string(), vec![
        (
            r#"SELECT T2.Lname FROM DEPARTMENT AS T1 JOIN FACULTY AS T2 ON T1.DNO = T3.DNO JOIN MEMBER_OF AS T3 ON T2.FacID = T3.FacID WHERE T1.DName = "Computer Science""#,
            r#"context.Departments.Join(context.MemberOfs, T1 => T1.Dno, T3 => T3.Dno, (T1, T3) => new { T1, T3 }).Join(context.Faculties, joined => joined.T3.FacId, T2 => T2.FacId, (joined, T2) => new { joined.T1, joined.T3, T2 }).Where(row => row.T1.Dname == "Computer Science").Select(row => new { row.T2.Lname }).ToList();"#,
        )
    ]);

    for (db_name, queries_and_results) in all_queries_and_results.iter() {
       
        let linq_query_builder = LinqQueryBuilder::new(&format!("../entity-framework/Models/{}", db_name));

        for (index, (sql, expected_result)) in queries_and_results.iter().enumerate() {
            // if db_name != "college_2" || index != 0 {
            //     continue;
            // }

            // if db_name == "college_3" && index == 0 {
            //     continue;
            // }

            if db_name != "csu_1" || index != 1 {
                continue;
            }
            
            println!("Running test {} | DB: {} | SQL: {}", index + 1, db_name, sql);

            let result = linq_query_builder.build_query(sql);

            if result == *expected_result {
                println!("Test {} passed", index + 1);
            } else {
                println!("Test {} failed", index + 1);
                println!("Expected | Got");
                println!("{}\n{}", expected_result, result);
                return;
            }
        }
    }
}

fn create_tests_to_file() {
    // let db_names = vec![
    // "activity_1".to_string(),
    // "apartment_rentals".to_string(),
    // "allergy_1".to_string(), 
    // "assets_maintenance".to_string(),
    // "baseball_1".to_string(),
    // "behavior_monitoring".to_string(),
    // "bike_1".to_string(),
    // "body_builder".to_string(),
    // "book_2".to_string(),
    // "browser_web".to_string(),
    // "candidate_poll".to_string(),
    // "chinook_1".to_string(),
    // "cinema".to_string(),
    // "climbing".to_string(),
    // "club_1".to_string(),
    // "coffee_shop".to_string(),
    // "college_1".to_string(),
    // // "college_2".to_string(),
    // // "college_3".to_string(),
    // "company_1".to_string()
    // ];

    let mut db_names: Vec<String> = Vec::new();
    for entry in fs::read_dir("../entity-framework/Models").unwrap() {
        let entry = entry.unwrap();

        let path = entry.path();

        if path.is_dir() {
            let file_name = path.file_name().unwrap().to_str().unwrap().to_string();

            let mut files = fs::read_dir(path).unwrap();

            if files.next().is_some() {
                db_names.push(file_name);
            }

        }

    }


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

    let mut successfully_executed_queries = 0;

    for db_name in &db_names {
        // Sadly, this one has issues with the data, since I get
        // Unhandled exception. System.InvalidOperationException: Nullable object must have a value.
        // when trying to execute a simple context.idk.Count();
        if db_name == "college_2" {
            continue;
        }

        if db_name == "customers_and_addresses" {
            break;
        }

        let queries = if let Some(queries) = queries.get(db_name.as_str()) {
            queries
        } else {
            continue;
        };

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

            // wrong in the dataset, college_2
            // if query.to_lowercase().contains("join prereq") {
            //     continue;
            // }
    
            println!("Processing query {} for {} | SUCCESS SO FAR: {}", index, db_name, successfully_executed_queries);
            println!("{}", query);
    
            let result = linq_query_builder.build_query(query);
    
            sql_and_results.push((query.to_string(), result.to_string()));

            successfully_executed_queries += 1;   
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
