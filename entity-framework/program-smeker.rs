using System.Net.Mime;
using entity_framework.Models.college_2;
using entity_framework.Models.cre_Doc_Control_Systems;
using entity_framework.Models.cre_Theme_park;
using entity_framework.Models.flight_1;
using entity_framework.Models.geo;
using entity_framework.Models.network_1;
using entity_framework.Models.station_weather;
using entity_framework.Models.store_product;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Update.Internal;

class Program
{
    // public static void Main()
    // {
    //     var context = new Network1Context();

    //     var sql = "SELECT T2.name FROM Friend AS T1 JOIN Highschooler AS T2 ON T1.student_id  =  T2.id GROUP BY T1.student_id HAVING count(*)  >=  3";
    //     var linq = context.Highschoolers
    //         .SelectMany(row => row.Friends, (T2, T1) => new { T1, T2 })
    //         .GroupBy(joined => joined.T2.Id)
    //         .Where(grouped => grouped.Count() >= 3)
    //         .Select(grouped => grouped.First().T2.Name);

    //     Tester.Test(linq, sql, context);
    // }

    // public static void Main()
    // {
    //     var context = new Flight1Context();

    //     var sql = "SELECT T3.name FROM Employee AS T1 JOIN Certificate AS T2 ON T1.eid  =  T2.eid JOIN Aircraft AS T3 ON T3.aid  =  T2.aid WHERE T1.name  =  \"John Williams\"";
    //     var linq = context.Employees
    //         .SelectMany(row => row.Aids, (T1, T3) => new { T1, T3 })
    //         .Where(joined => joined.T1.Name == "John Williams")
    //         .Select(joined => joined.T3.Name);

    //     Tester.Test(linq, sql, context);
    // }

    // public static void Main()
    // {
    //     var context = new StationWeatherContext();

    //     var sql = "SELECT t3.name ,  t3.time FROM station AS t1 JOIN route AS t2 ON t1.id  =  t2.station_id JOIN train AS t3 ON t2.train_id  =  t3.id WHERE t1.local_authority  =  \"Chiltern\"";

    //     var linq = context.Stations
    //         .SelectMany(row => row.Trains, (T1, T3) => new { T1, T3 })
    //         .Where(joined => joined.T1.LocalAuthority == "Chiltern")
    //         .Select(joined => new
    //         {
    //             joined.T3.Name,
    //             joined.T3.Time
    //         });

    //     Tester.Test(linq, sql, context);
    // }

    // public static void Main()
    // {
    //     var context = new CreThemeParkContext();

    //     var sql = "SELECT T1.Name FROM Tourist_Attractions AS T1 JOIN Tourist_Attraction_Features AS T2 ON T1.tourist_attraction_id  =  T2.tourist_attraction_id JOIN Features AS T3 ON T2.Feature_ID  =  T3.Feature_ID WHERE T3.feature_Details  =  'park'";
    //     var linq = context.TouristAttractions
    //         .SelectMany(row => row.Features, (T1, T3) => new { T1, T3 })
    //         .Where(joined => joined.T3.FeatureDetails == "park")
    //         .Select(joined => joined.T1.Name);


    //     Tester.Test(linq, sql, context);
    // }

    // public static void Main()
    // {
    //     var context = new StoreProductContext();

    //     var sql = "SELECT t1.product FROM product AS t1 JOIN store_product AS t2 ON t1.product_id  =  t2.product_id JOIN store AS t3 ON t2.store_id  =  t3.store_id WHERE t3.store_name  =  \"Miramichi\"";

    //     var linq = context.Products
    //         .SelectMany(row => row.Stores, (T1, T3) => new { T1, T3 })
    //         .Where(joined => joined.T3.StoreName == "Miramichi")
    //         .Select(joined => joined.T1.Product1);

    //     Tester.Test(linq, sql, context);
    // }


    // public static void Main()
    // {
    //     var context = new GeoContext();

    //     var sql = "SELECT border FROM border_info";
    //     var linq = context.States
    //     .SelectMany(state => state.Borders)
    //     .Select(row => row.StateName).ToArray();

    //     Console.WriteLine(linq.Length);

    //     Tester.Test(linq, sql, context);
    // }


    // I could know, if only... it would work X((((
    // public static void Main()
    // {
    //     var context = new College2Context();

    //     var sql = "SELECT T1.title FROM course AS T1 JOIN prereq AS T2 ON T1.course_id  =  T2.course_id GROUP BY T2.course_id HAVING count(*)  =  2";
    //     var linq = context.Courses
    //         .SelectMany(row => row.Prereqs, (T1, T2) => new { T1, T2 })
    //         .GroupBy(joined => joined.T2.CourseId)
    //         .Where(grouped => grouped.Count() == 2)
    //         .Select(grouped => grouped.First().T1.Title);

    //     Tester.Test(linq, sql, context);
    // }

    public static void Main()
    {
        var context = new CreDocControlSystemsContext();

        var sql = "SELECT Employees.employee_name FROM Circulation_History JOIN Employees ON Circulation_History.employee_id = Employees.employee_id WHERE Circulation_History.document_id = 1;";
        var linq = context.DraftCopies
            .SelectMany(row => row.Employees, (Circulation_History, Employees) => new { Employees, Circulation_History })
            .Where(row => row.Circulation_History.DocumentId == 1)
            .Select(row => row.Employees.EmployeeName);

        Tester.Test(linq, sql, context);
    }
}

/*
    Certificate, real "certificate"
        Aircraft with FK Aid, real "aid"
        Employee with FK Eid, real "eid"
    
    Has many "Aids" and many "Eids", 


    SELECT T3.name FROM Employee AS T1 JOIN Certificate AS T2 ON T1.eid  =  T2.eid
        -> This is essentially context.Employees.SelectMany(row => row.Aids)



    If you literally join the m2m table, it's like you're doing nothing

    If you don't have the usual equality on the ids on foreign keys in the on clauses, then what do you do?
        Is this a real case?
*/