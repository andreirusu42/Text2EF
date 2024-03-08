using entity_framework.Models.activity_1;
using Microsoft.EntityFrameworkCore;

class Activity1Test
{
    public static void Test()
    {
        using var context = new Activity1Context();

        var methodSyntaxResult = context.Faculties.GroupBy(row => new { row.Building }).Select(row => new { row.Key });



        // var methodSyntaxResult = context.Faculties.Where(row => row.Sex == "F").Select(row => new { row.Fname, row.Lname, row.Phone });

        // var areEqual = methodSyntaxResult == rawSqlResult;

        // Console.WriteLine($"Are equal: {areEqual}");
        // using var context = new world_1Context();

        // var methodSyntaxResult = context.country.Join(context.city, T1 => T1.Code, T2 => T2.CountryCode, (T1, T2) => new { T1, T2 }).Where(row => row.T2.Name == "Kabul").Select(row => new { row.T1.Region });
        // var rawSqlResult = context.country.FromSqlRaw(@"SELECT Region FROM country AS T1 JOIN city AS T2 ON T1.Code  =  T2.CountryCode WHERE T2.Name  =  'Kabul'").Select(row => new { row.Region });

        // var areEqual = methodSyntaxResult.ToList().SequenceEqual(rawSqlResult.ToList());

        // Console.WriteLine($"Are equal: {areEqual}");

        // QUERY 1
        //         var europeanCitiesMS = context.Cities
        //             .Join(
        //                 context.Countries,
        //                 city => city.CountryCode,
        //                 country => country.Code,
        //                 (city, country) => new { City = city, Country = country }
        //             )
        //             .Where(joined => joined.Country.Continent == "Europe")
        //             .Where(joined => !context.Countrylanguages
        //                 .Any(cl => cl.CountryCode == joined.Country.Code && cl.IsOfficial == "T" && cl.Language == "English"))
        //             .Select(joined => joined.City.Name)
        //             .Distinct()
        //             .ToList();

        //         System.Console.WriteLine(europeanCitiesMS.Count);

        //         var sql = @"SELECT DISTINCT T2.Name
        // FROM country AS T1
        // JOIN city AS T2 ON T2.CountryCode  =  T1.Code
        // WHERE T1.Continent  =  'Europe'
        // AND T1.Name NOT IN (SELECT T3.Name
        //                     FROM country AS T3
        //                     JOIN countrylanguage AS T4 ON T3.Code  =  T4.CountryCode WHERE T4.IsOfficial  =  'T'
        //                     AND T4.Language  =  'English')";

        //         var europeanCitiesSQL = context.Cities
        //             .FromSqlRaw(sql)
        //             .Select(city => city.Name)
        //             .ToList();

        //         System.Console.WriteLine(europeanCitiesSQL.Count);

        //         var areEqual = europeanCitiesMS.SequenceEqual(europeanCitiesSQL);

        //         System.Console.WriteLine(areEqual);
    }
}