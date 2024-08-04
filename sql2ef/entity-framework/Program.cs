using entity_framework.Models.manufactory_1; 
using Microsoft.EntityFrameworkCore;

class Program {
public static void Main() {
var context = new Manufactory1Context();

var sql = "SELECT avg(revenue) , max(revenue) , sum(revenue) FROM manufacturers";
var linq = context.Manufacturers.GroupBy(row => 1).Select(group => new { AverageRevenue = group.Select(row => row.Revenue).Average(), MaxRevenue = group.Select(row => row.Revenue).Max(), SumRevenue = group.Select(row => row.Revenue).Sum() }).ToList();

Tester.Test(linq, sql, context);
}

}