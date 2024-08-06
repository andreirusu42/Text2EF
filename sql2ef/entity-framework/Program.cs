using entity_framework.Models.mountain_photos; 
using Microsoft.EntityFrameworkCore;

class Program {
public static void Main() {
var context = new MountainPhotosContext();

var sql = "SELECT name , prominence FROM mountain EXCEPT SELECT T1.name , T1.prominence FROM mountain AS T1 JOIN photos AS T2 ON T1.id = T2.mountain_id JOIN camera_lens AS T3 ON T2.camera_lens_id = T3.id WHERE T3.brand = \'Sigma\'";
var linq = context.Mountains.Where(row => row.Prominence.HasValue).Select(row => new { row.Name, row.Prominence.Value }).Except(context.Mountains.Join(context.Photos, T1 => T1.Id, T2 => T2.MountainId, (T1, T2) => new { T1, T2 }).Join(context.CameraLens, joined => joined.T2.CameraLensId, T3 => T3.Id, (joined, T3) => new { joined.T1, joined.T2, T3 }).Where(row => row.T3.Brand == "Sigma").Where(row => row.T1.Prominence.HasValue).Select(row => new { row.T1.Name, row.T1.Prominence.Value })).ToList();

Tester.Test(linq, sql, context);
}

}