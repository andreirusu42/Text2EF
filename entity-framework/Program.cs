using entity_framework.Models.activity_1;
using Microsoft.EntityFrameworkCore;

class Program
{
    static void Main(string[] args)
    {
        var context = new Activity1Context();

        var tests_with_linq_query_and_raw_sql_query_array = new (IQueryable<dynamic>, string)[] {
            (context.Faculties.Select(row => new { row.Rank }), "SELECT rank FROM Faculty"),
            (context.Faculties.Select(row => new { row.Rank }).Distinct(), "SELECT DISTINCT rank FROM Faculty"),
            (context.Faculties.Join(context.Students, T1 => T1.FacId, T2 => T2.Advisor, (T1, T2) => new { T1, T2 }).Where(row => row.T2.Fname == "Linda" && row.T2.Lname == "Smith").Select(row => new { row.T1.Fname, row.T1.Lname }), "SELECT T1.fname, T1.lname FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID = T2.advisor WHERE T2.fname = \"Linda\" AND T2.lname = \"Smith\""),
            (context.Faculties.GroupBy(row => new { row.Rank }).Select(group => new { Count = group.Count(), group.Key.Rank }),  "SELECT COUNT(*), rank FROM Faculty GROUP BY rank"),
            (context.Faculties.Join(context.Students, T1 => T1.FacId, T2 => T2.Advisor, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.FacId }).Select(group => new { group.Key.FacId }), "SELECT T1.FacID FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID  =  T2.advisor GROUP BY T1.FacID")
        };

        for (int i = 0; i < tests_with_linq_query_and_raw_sql_query_array.Length; i++)
        {
            var (linq_query, raw_sql_query) = tests_with_linq_query_and_raw_sql_query_array[i];

            var test_passed = Tester.Test(linq_query, raw_sql_query);

            Console.WriteLine($"Test {i + 1} passed: {test_passed}");
        }
    }
}
