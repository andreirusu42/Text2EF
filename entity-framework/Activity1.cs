using entity_framework.Models.activity_1;
using Microsoft.EntityFrameworkCore;

class Activity1Test
{
    public static void Test()
    {
        using var context = new Activity1Context();

        String sqlQuery = "SELECT T1.fname ,  T1.lname FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID  =  T2.advisor WHERE T2.fname  =  \"Linda\" AND T2.lname  =  \"Smith\"";

        var resultSql = context.Faculties
            .FromSqlRaw(sqlQuery)
            .Select(x => new { x.Fname, x.Lname })
            .ToList();

        var resultLinq = context.Faculties
            .Join(context.Students,
                T1 => T1.FacId,
                T2 => T2.Advisor,
                (T1, T2) => new { T1, T2 })
            .Where(x => x.T1.Fname == "Linda" && x.T2.Lname == "Smith")
            .Select(x => new { x.T1.Fname, x.T2.Lname })
            .ToList();

        Boolean equal = resultSql.SequenceEqual(resultLinq);

        for (int i = 0; i < resultSql.Count; i++)
        {
            System.Console.WriteLine($"Sql: {resultSql[i]}");
            System.Console.WriteLine($"Linq: {resultLinq[i]}");
        }

        if (equal)
        {
            System.Console.WriteLine("Results are the same");
        }
        else
        {
            System.Console.WriteLine("Results are different");
        }
    }
}