using entity_framework.Models.cre_Doc_Control_Systems; 
using Microsoft.EntityFrameworkCore;

class Program {
public static void Main() {
var context = new CreDocControlSystemsContext();

var sql = "SELECT Employees.employee_name FROM Employees JOIN Circulation_History ON Circulation_History.employee_id = Employees.employee_id WHERE Circulation_History.document_id = 1;";
var linq = context.Employees.Join(context.CirculationHistory, Employees => Employees.EmployeeId, CirculationHistory => CirculationHistory.EmployeeId, (Employees, CirculationHistory) => new { Employees, CirculationHistory }).Where(row => row.CirculationHistory.DocumentId == 1).Select(row => new { row.Employees.EmployeeName }).ToList();

Tester.Test(linq, sql, context);
}

}