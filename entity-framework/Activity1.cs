using entity_framework.Models.allergy_1;
using Microsoft.EntityFrameworkCore;

class Activity1Test
{
    public static void Test()
    {
        using var context = new Allergy1Context();

        // var sql = "SELECT count(*) FROM Has_allergy AS T1 JOIN Allergy_type AS T2 ON T1.allergy  =  T2.allergy WHERE T2.allergytype  =  'food'";

        // var resultSql = context.Has_Allergy.FromSqlRaw(sql)
        // .FirstOrDefault();

        var resultLinq = context.HasAllergies.Join(context.AllergyTypes, T1 => T1.Allergy, T2 => T2.Allergy, (T1, T2) => new { T1, T2 })
    .Where(row => row.T2.AllergyType1 == "food")
    .GroupBy(row => new { row.T1.Allergy, row.T2.AllergyType1 })
    .Select(group => new
    {
        Count = group.Count(),
        x = group.Key.AllergyType1
    })
    .Count();

        Console.WriteLine(resultLinq);

        // if (resultSql != resultLinq)
        // {
        //     throw new Exception("Test failed");
        // }
    }
}