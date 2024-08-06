using entity_framework.Models.bike_1; 
using Microsoft.EntityFrameworkCore;

class Program {
public static void Main() {
var context = new Bike1Context();

var sql = "SELECT id FROM station WHERE city = \"San Francisco\" INTERSECT SELECT station_id FROM status GROUP BY station_id HAVING avg(bikes_available) > 10";
var linq = context.Stations.Where(row => row.City == "San Francisco").Select(row => row.Id).Intersect(context.Statuses.GroupBy(row => new { row.StationId }).Where(group => group.Average(row => row.BikesAvailable) > 10).Where(group => group.Key.StationId.HasValue).Select(group => group.Key.StationId.Value)).ToList();

Tester.Test(linq, sql, context);
}

}