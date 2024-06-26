using entity_framework.Models.activity_1;
using Microsoft.EntityFrameworkCore;

class Program
{
    static void Main(string[] args)
    {
        using var context = new Activity1Context();

        var query = "SELECT T1.fname, T1.lname FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID = T2.advisor WHERE T2.fname = \"Linda\" AND T2.lname = \"Smith\"";

        var rawSqlResult = context.Faculties
            .FromSqlRaw(query)
            .Select(row => new
            {
                row.Fname,
                row.Lname
            }).ToList();

        var methodSyntaxResult = context.Faculties.Join(context.Students, T1 => T1.FacId, T2 => T2.Advisor, (T1, T2) => new { T1, T2 }).Where(row => row.T2.Fname == "Linda" && row.T2.Lname == "Smith").Select(row => new { row.T1.Fname, row.T1.Lname }).ToList();

        for (int i = 0; i < rawSqlResult.Count; i++)
        {
            Console.WriteLine($"Raw SQL: {rawSqlResult[i].Fname} {rawSqlResult[i].Lname}");
            Console.WriteLine($"Method Syntax: {methodSyntaxResult[i].Fname} {methodSyntaxResult[i].Lname}");
        }

        var areResultsEqual = methodSyntaxResult.SequenceEqual(rawSqlResult);

        if (areResultsEqual)
        {
            Console.WriteLine("Results are equal");
        }
        else
        {
            Console.WriteLine("Results are not equal");
        }
    }
}
