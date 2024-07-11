using System.Security.Cryptography.X509Certificates;
using entity_framework.Models.activity_1;
using Microsoft.EntityFrameworkCore;

class Program
{
    static void Main2(string[] args)
    {
        var context = new Activity1Context();

        var result = context.Activities.Join(context.FacultyParticipatesIns, T1 => T1.Actid, T2 => T2.Actid, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.Actid, row.T1.ActivityName }).OrderByDescending(g => g.Count()).Select(g => new { g.Key.ActivityName }).ToList();

        foreach (var row in result)
        {
            Console.WriteLine(row);
        }
    }
    static void Main(string[] args)
    {
        var context = new Activity1Context();

        var tests_with_linq_query_and_raw_sql_query_array = new (object?, string)[] {
            (context.Faculties.Select(row => new { row.Rank }).ToList(), "SELECT rank FROM Faculty"),
            (context.Faculties.Select(row => new { row.Rank }).Distinct().ToList(), "SELECT DISTINCT rank FROM Faculty"),
            (context.Faculties.Join(context.Students, T1 => T1.FacId, T2 => T2.Advisor, (T1, T2) => new { T1, T2 }).Where(row => row.T2.Fname == "Linda" && row.T2.Lname == "Smith").Select(row => new { row.T1.Fname, row.T1.Lname }).ToList(), "SELECT T1.fname, T1.lname FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID = T2.advisor WHERE T2.fname = \"Linda\" AND T2.lname = \"Smith\""),
            (context.Faculties.GroupBy(row => new { row.Rank }).Select(group => new { Count = group.Count(), group.Key.Rank }).ToList(),  "SELECT COUNT(*), rank FROM Faculty GROUP BY rank"),
            (context.Faculties.Join(context.Students, T1 => T1.FacId, T2 => T2.Advisor, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.FacId }).Select(group => new { group.Key.FacId }).ToList(), "SELECT T1.FacID FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID  =  T2.advisor GROUP BY T1.FacID"),
            (context.Activities.Join(context.FacultyParticipatesIns, T1 => T1.Actid, T2 => T2.Actid, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.Actid }).OrderByDescending(group => group.Count()).Select(group => new { group.First().T1.ActivityName }).Take(1).ToList(), "SELECT T1.activity_name FROM Activity AS T1 JOIN Faculty_participates_in AS T2 ON T1.actID  =  T2.actID GROUP BY T1.actID ORDER BY count(*) DESC LIMIT 1"),
            (context.ParticipatesIns.Join(context.Activities, T1 => T1.Actid, T2 => T2.Actid, (T1, T2) => new { T1, T2 }).Where(row => row.T2.ActivityName == "Canoeing").Select(row => new { row.T1.Stuid }).Intersect(context.ParticipatesIns.Join(context.Activities, T1 => T1.Actid, T2 => T2.Actid, (T1, T2) => new { T1, T2 }).Where(row => row.T2.ActivityName == "Kayaking").Select(row => new { row.T1.Stuid })).ToList(), "SELECT T1.stuid FROM participates_in AS T1 JOIN activity AS T2 ON T1.actid  =  T2.actid WHERE T2.activity_name  =  'Canoeing' INTERSECT SELECT T1.stuid FROM participates_in AS T1 JOIN activity AS T2 ON T1.actid  =  T2.actid WHERE T2.activity_name  =  'Kayaking'"),
            (context.Faculties.Join(context.FacultyParticipatesIns, T1 => T1.FacId, T2 => T2.FacId, (T1, T2) => new { T1, T2 }).Join(context.Activities, joined => joined.T2.Actid, T3 => T3.Actid, (joined, T3) => new { joined.T1, joined.T2, T3 }).Where(row => row.T1.Fname == "Mark" && row.T1.Lname == "Giuliano").Select(row => new { row.T3.ActivityName }).ToList(), "SELECT T3.activity_name FROM Faculty AS T1 JOIN Faculty_participates_in AS T2 ON T2.facID  =  T1.facID JOIN Activity AS T3 ON T3.actid  =  T2.actid WHERE T1.fname  =  'Mark' AND T1.lname  =  'Giuliano'")
    };

        for (int i = 0; i < tests_with_linq_query_and_raw_sql_query_array.Length; i++)
        {
            var (linq_query, raw_sql_query) = tests_with_linq_query_and_raw_sql_query_array[i];

            var test_passed = Tester.Test(linq_query, raw_sql_query);

            Console.WriteLine($"Test {i + 1} passed: {test_passed}");
        }
    }
}
