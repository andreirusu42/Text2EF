using entity_framework.Models.allergy_1;
using Microsoft.EntityFrameworkCore;

class Activity1Test
{
    public static void Test()
    {
        using var context = new allergy_1Context();

        // var sql = "SELECT count(*) FROM Has_allergy AS T1 JOIN Allergy_type AS T2 ON T1.allergy  =  T2.allergy WHERE T2.allergytype  =  'food'";

        // var resultSql = context.Has_Allergy.FromSqlRaw(sql)
        // .FirstOrDefault();

        var resultLinq = context.Has_Allergy.Join(context.Allergy_Type, T1 => T1.Allergy, T2 => T2.Allergy, (T1, T2) => new { T1, T2 })
    .Where(row => row.T2.AllergyType == "food")
    .Count();

        Console.WriteLine(resultLinq);

        // if (resultSql != resultLinq)
        // {
        //     throw new Exception("Test failed");
        // }
    }
}