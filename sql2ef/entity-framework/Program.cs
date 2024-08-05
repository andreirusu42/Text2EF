using entity_framework.Models.manufactory_1; 
using Microsoft.EntityFrameworkCore;

class Program {
public static void Main() {
var context = new Manufactory1Context();

var sql = "SELECT * FROM products WHERE price BETWEEN 60 AND 120";
var linq = context.Products.Where(row => row.Price >= 60 && row.Price <= 120).Select(row => new { row..Code, row..Name, row..Price, row..Manufacturer }).ToList();

Tester.Test(linq, sql, context);
}

}