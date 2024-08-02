using entity_framework.Models.csu_1; 
using Microsoft.EntityFrameworkCore;

class Program {
public static void Main() {
var context = new Csu1Context();

var sql = "SELECT avg(campusfee) FROM csu_fees WHERE YEAR = 1996";
var linq = context.CsuFees.Where(row => row.Year == 1996).Select(row => row.CampusFee).Average();

Tester.Test(linq, sql, context);
}

}