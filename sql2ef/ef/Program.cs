using entity_framework.Models.csu_1;
using Microsoft.EntityFrameworkCore;

class Program
{
    public static void Main()
    {
        var context = new Csu1Context();
        // Execute raw SQL query and get the list of campuses
        var rawSqlQuery = context.Campuses.FromSqlRaw("SELECT * FROM campuses").ToList();

        // Execute LINQ query and get the list of campuses
        var linqQuery = context.Campuses.ToList();

        // Compare the results
        bool areEqual = rawSqlQuery.SequenceEqual(linqQuery);

        if (areEqual)
        {
            Console.WriteLine("The results are equal.");
        }
        else
        {
            Console.WriteLine("The results are not equal.");
        }

    }

}