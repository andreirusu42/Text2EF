using entity_framework.Models.activity_1;
using entity_framework.Models.baseball_1;
using Microsoft.EntityFrameworkCore;

class Activity1Test
{
    public static void Test()
    {
        using var context = new baseball_1Context();

        var sql = "SELECT yearid ,  count(*) FROM hall_of_fame GROUP BY yearid;";

        var result = context.hall_of_fame.FromSqlRaw(sql)
        .Select(row => new
        {
            yearid = row.yearid,
        })
        .ToList();

        var linq = context.hall_of_fame
        .GroupBy(row => row.yearid)
        .Select(group => new
        {
            yearid = group.Key,
            count = group.Count()
        });

        foreach (var item in result)
        {
            System.Console.WriteLine(item);
        }

        foreach (var item in linq)
        {
            System.Console.WriteLine(item);
        }

    }
}