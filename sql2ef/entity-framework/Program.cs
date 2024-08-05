using entity_framework.Models.insurance_fnol; 
using Microsoft.EntityFrameworkCore;

class Program {
public static void Main() {
var context = new InsuranceFnolContext();

var sql = "SELECT t2.date_opened , t2.date_closed FROM customers AS t1 JOIN customers_policies AS t2 ON t1.customer_id = t2.customer_id WHERE t1.customer_name LIKE \"%Diana%\"";
var linq = context.Customers.Join(context.CustomersPolicies, t1 => t1.CustomerId, t2 => t2.CustomerId, (t1, t2) => new { t1, t2 }).Where(row => EF.Functions.Like(row.t1.CustomerName, "%Diana%")).Select(row => new { row.t2.DateOpened, row.t2.DateClosed }).ToList();

Tester.Test(linq, sql, context);
}

}