using entity_framework.Models.activity_1;
using Microsoft.EntityFrameworkCore;

class Activity1Test
{
    public static void Test()
    {
        using var context = new Activity1Context();

        String sqlQuery = "SELECT TOP 50 PERCENT fname, lname FROM Faculty WHERE Rank =  \"Instructor\"";

        var resultSql = context.Faculties
                              .FromSqlRaw(sqlQuery)
                              .Select(f => new
                              {
                                  Fname = f.Fname,
                                  Lname = f.Lname
                              })
                              .ToArray();

        for (int i = 0; i < resultSql.Length; i++)
        {
            System.Console.WriteLine(resultSql[i]);
        }

        var resultLinq = context.Faculties
                               .Where(f => f.Rank == "Instructor")
                               .Select(f => new
                               {
                                   Fname = f.Fname,
                                   Lname = f.Lname
                               })
                               .ToArray();


        // // Check if results are the same

        Boolean equal = resultSql.SequenceEqual(resultLinq);

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