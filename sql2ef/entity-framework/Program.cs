using entity_framework.Models.station_weather; 
using Microsoft.EntityFrameworkCore;

class Program {
public static void Main() {
var context = new StationWeatherContext();

var sql = "SELECT count(*) , t1.network_name , t1.services FROM station AS t1 JOIN route AS t2 ON t1.id = t2.station_id GROUP BY t2.station_id";
var linq = context.Stations.Join(context.Route, t1 => t1.Id, t2 => t2.StationId, (t1, t2) => new { t1, t2 }).GroupBy(row => new { row.t2.StationId }).Select(group => new { Count = group.Count(), group.First().t1.NetworkName, group.First().t1.Services }).ToList();

Tester.Test(linq, sql, context);
}

}