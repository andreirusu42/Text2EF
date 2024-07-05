using entity_framework.Models.activity_1;
using Microsoft.EntityFrameworkCore;

class Tester
{
    private static string GenerateRawResultString(string query)
    {
        using var context = new Activity1Context();
        var connection = context.Database.GetDbConnection();

        var sqlQueryResults = new List<Dictionary<string, object>>();

        try
        {
            connection.Open();
            var command = connection.CreateCommand();
            command.CommandText = query;

            using (var reader = command.ExecuteReader())
            {
                var columnCount = reader.FieldCount;

                while (reader.Read())
                {
                    var row = new Dictionary<string, object>();
                    for (int i = 0; i < columnCount; i++)
                    {
                        row[reader.GetName(i).ToLower()] = reader.GetValue(i);
                    }
                    sqlQueryResults.Add(row);
                }
            }
        }
        finally
        {
            connection.Close();
        }

        var sqlStrings = sqlQueryResults.Select(dict => string.Join(", ", dict.Select(kv => $"{kv.Key}={kv.Value}"))).ToList();

        return string.Join("\n", sqlStrings);
    }

    private static List<Dictionary<string, object>> ExecuteSqlQuery(string query)
    {
        using var context = new Activity1Context();
        var connection = context.Database.GetDbConnection();

        var sqlQueryResults = new List<Dictionary<string, object>>();

        try
        {
            connection.Open();
            var command = connection.CreateCommand();
            command.CommandText = query;

            using (var reader = command.ExecuteReader())
            {
                var columnCount = reader.FieldCount;

                while (reader.Read())
                {
                    var row = new Dictionary<string, object>();
                    for (int i = 0; i < columnCount; i++)
                    {
                        var columnName = reader.GetName(i).ToLower();
                        if (columnName == "count(*)") columnName = "count";
                        row[columnName] = reader.GetValue(i);
                    }
                    sqlQueryResults.Add(row);
                }
            }
        }
        finally
        {
            connection.Close();
        }

        return sqlQueryResults;
    }

    private static List<Dictionary<string, object>> ExecuteLinqQuery<TResult>(IQueryable<TResult> linqQuery)
    {
        var linqResults = linqQuery
            .ToList()
            .Select(item => item.GetType().GetProperties().ToDictionary(p => p.Name.ToLower(), p => p.GetValue(item)))
            .ToList();

        return linqResults;
    }

    private static bool CompareResults(List<Dictionary<string, object>> linqResults, List<Dictionary<string, object>> sqlResults)
    {
        var linqStrings = linqResults.Select(dict => string.Join(", ", dict.Select(kv => $"{kv.Key}={kv.Value}"))).ToList();
        var sqlStrings = sqlResults.Select(dict => string.Join(", ", dict.Select(kv => $"{kv.Key}={kv.Value}"))).ToList();

        // Console.WriteLine("Linq results:");
        // Console.WriteLine(string.Join("\n", linqStrings));
        // Console.WriteLine("Sql results:");
        // Console.WriteLine(string.Join("\n", sqlStrings));

        bool areEqual = linqStrings.Count == sqlStrings.Count && !linqStrings.Except(sqlStrings).Any();

        return areEqual;
    }

    public static bool Test(IQueryable<dynamic> linqQuery, string sqlQuery)
    {
        var linqResults = ExecuteLinqQuery(linqQuery);
        var sqlResults = ExecuteSqlQuery(sqlQuery);

        return CompareResults(linqResults, sqlResults);
    }
}
