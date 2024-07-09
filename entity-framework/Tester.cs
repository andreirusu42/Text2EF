using entity_framework.Models.activity_1;
using Microsoft.EntityFrameworkCore;

class Tester
{
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
                        var columnName = NormalizeColumnName(reader.GetName(i));
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

    private static List<Dictionary<string, object>> ExecuteLinqQuery<TResult>(object linqQuery)
    {

        if (linqQuery is IEnumerable<TResult> query)
        {
            var linqResults = query
             .Select(item => item.GetType().GetProperties().ToDictionary(p => NormalizeColumnName(p.Name), p => p.GetValue(item)))
             .ToList();

            return linqResults;
        }

        var results = new List<Dictionary<string, object>>
        {
            linqQuery.GetType().GetProperties().ToDictionary(p => NormalizeColumnName(p.Name), p => p.GetValue(linqQuery))
        };

        return results;
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

    public static bool Test(object linqQuery, string sqlQuery)
    {
        var linqResults = ExecuteLinqQuery<object>(linqQuery);
        var sqlResults = ExecuteSqlQuery(sqlQuery);


        // for (int i = 0; i < sqlResults.Count; i++)
        // {
        //     var row = sqlResults[i];
        //     Console.WriteLine($"Row {i + 1}: {string.Join(", ", row.Select(kv => $"{kv.Key}={kv.Value}"))}");
        // }

        // for (int i = 0; i < linqResults.Count; i++)
        // {
        //     var row = linqResults[i];
        //     Console.WriteLine($"Row {i + 1}: {string.Join(", ", row.Select(kv => $"{kv.Key}={kv.Value}"))}");
        // }

        return CompareResults(linqResults, sqlResults);
    }

    private static string NormalizeColumnName(string columnName)
    {
        // Normalize the column name by converting it to lower case
        // and replacing camelCase or PascalCase with snake_case
        if (string.IsNullOrEmpty(columnName)) return columnName;

        var normalizedColumnName = System.Text.RegularExpressions.Regex
            .Replace(columnName, "([a-z])([A-Z])", "$1_$2")
            .ToLower();

        return normalizedColumnName;
    }
}
