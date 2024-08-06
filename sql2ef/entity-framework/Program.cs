using entity_framework.Models.apartment_rentals; 
using Microsoft.EntityFrameworkCore;

class Program {
public static void Main() {
var context = new ApartmentRentalsContext();

var sql = "SELECT sum(T2.room_count) FROM Apartment_Buildings AS T1 JOIN Apartments AS T2 ON T1.building_id = T2.building_id WHERE T1.building_short_name = \"Columbus Square\"";
var linq = context.ApartmentBuildings.Join(context.Apartments, T1 => T1.BuildingId, T2 => T2.BuildingId, (T1, T2) => new { T1, T2 }).Where(row => row.T1.BuildingShortName == "Columbus Square").Select(row => row.T2.RoomCount).ToList().Select(value => double.Parse(value)).Sum();

Tester.Test(linq, sql, context);
}

}