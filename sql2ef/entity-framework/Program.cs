using entity_framework.Models.browser_web; 
using Microsoft.EntityFrameworkCore;

class Program {
public static void Main() {
var context = new BrowserWebContext();

var sql = "SELECT count(*) FROM browser WHERE market_share >= 5";
var linq = context.Browsers.Where(row => row.MarketShare >= 5).Count();

Tester.Test(linq, sql, context);
}

}