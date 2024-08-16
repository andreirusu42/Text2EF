using entity_framework.Models.student_transcripts_tracking;
using Microsoft.EntityFrameworkCore;

class Program
{
    public static void Main()
    {
        var context = new StudentTranscriptsTrackingContext();

        var sql = "select t1.first_name from students as t1 join addresses as t2 on t1.permanent_address_id  =  t2.address_id where t2.country  =  \'haiti\' or t1.cell_mobile_number  =  \'09700166582\'";
        var linq = context.Students.Join(context.Addresses, t1 => t1.PermanentAddressId, t2 => t2.AddressId, (t1, t2) => new { t1, t2 }).Where(row => row.t2.Country == "haiti" || row.t1.CellMobileNumber == "09700166582").Select(row => new { row.t1.FirstName }).ToList();

        Tester.Test(linq, sql, context);
    }
}