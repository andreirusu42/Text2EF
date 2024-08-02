using entity_framework.Models.browser_web; 
using Microsoft.EntityFrameworkCore;

class Program {
    public static void Main() {
        var context = new BrowserWebContext();

        var sql = "SELECT DISTINCT T1.name FROM web_client_accelerator AS T1 JOIN accelerator_compatible_browser AS T2 ON T2.accelerator_id = T1.id JOIN browser AS T3 ON T2.browser_id = T3.id WHERE T3.market_share > 15;";
        var linq = context.WebClientAccelerators.Join(context.AcceleratorCompatibleBrowsers, T1 => T1.Id, T2 => T2.AcceleratorId, (T1, T2) => new { T1, T2 }).Join(context.Browsers, joined => joined.T2.BrowserId, T3 => T3.Id, (joined, T3) => new { joined.T1, joined.T2, T3 }).Where(row => row.T3.MarketShare > 15).Select(row => new { row.T1.Name }).Distinct().ToList();

        Tester.Test(linq, sql, context);
    }

}