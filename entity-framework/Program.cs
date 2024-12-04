using entity_framework.Models.Spider.flight_1;
using Microsoft.EntityFrameworkCore;

class Program
{
    public static void Main()
    {
        var context = new Flight1Context();

        var sql = "SELECT T2.name FROM Certificate AS T1 JOIN Aircraft AS T2 ON T2.aid  =  T1.aid GROUP BY T1.aid ORDER BY count(*) DESC LIMIT 1";
        var linq = context.Employees.SelectMany(row => row.Aids, (T1, T2) => new { T1, T2 }).GroupBy(row => new { row.T1.Id }).OrderByDescending(group => group.Count()).Select(group => new { group.First().T2.Name }).Take(1).ToList();

        Tester.Test(linq, sql, context);
    }

}