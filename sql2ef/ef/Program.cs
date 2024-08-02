using entity_framework.Models.browser_web; 
using Microsoft.EntityFrameworkCore;

class Program {
    public static void Main() {
        var context = new BrowserWebContext();

        var sql = "SELECT name , operating_system FROM web_client_accelerator WHERE CONNECTION != 'Broadband'";
        var linq = context.WebClientAccelerators.Where(row => row.Connection != "Broadband").Select(row => new { row.Name, row.OperatingSystem }).ToList();

        Tester.Test(linq, sql, context);
    }

}