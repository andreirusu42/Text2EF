using entity_framework.Models.hospital_1; 
using Microsoft.EntityFrameworkCore;

class Program {
public static void Main() {
var context = new Hospital1Context();

var sql = "SELECT appointmentid FROM appointment ORDER BY START DESC LIMIT 1";
var linq = context.Appointments.OrderByDescending(row => row.Start).Select(row => new { row.AppointmentId }).Take(1).ToList();

Tester.Test(linq, sql, context);
}

}