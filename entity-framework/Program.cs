using entity_framework.Models.twitter_1; 
using Microsoft.EntityFrameworkCore;

class Program {
public static void Main() {
var context = new Twitter1Context();

var sql = "SELECT max(followers) ,  sum(followers) FROM user_profiles";
var linq = context.UserProfiles.GroupBy(row => 1).Select(group => new { MaxFollowers = group.Select(row => row.Followers).Max(), SumFollowers = group.Select(row => row.Followers).Sum() }).ToList();

Tester.Test(linq, sql, context);
}

}