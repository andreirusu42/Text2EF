using Microsoft.EntityFrameworkCore;

class Tester
{
    private static List<Dictionary<string, object>> ExecuteSqlQuery(string query, DbContext context)
    {
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
                        var columnName = $"{i}";
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
        if (linqQuery is System.Collections.IEnumerable query)
        {
            var enumerator = query.GetEnumerator();
            if (!enumerator.MoveNext())
            {
                return new List<Dictionary<string, object>>();
            }

            var firstItem = enumerator.Current;

            if (firstItem == null)
            {
                return new List<Dictionary<string, object>>();
            }

            var isPrimitive = firstItem.GetType().IsPrimitive || firstItem is string || firstItem is int;

            if (isPrimitive)
            {
                var primitiveResults = query.Cast<object>().Select(item =>
                    new Dictionary<string, object> { { "Value", item } }).ToList();
                return primitiveResults;
            }
            else
            {
                var complexResults = query.Cast<object>().Select(item =>
                    {
                        var key = 0;

                        var result = item.GetType()
                        .GetProperties()
                        .ToDictionary(p => $"{key}", p =>
                        {
                            var value = p.GetValue(item);

                            key += 1;

                            return value is DateTime dateTime ? dateTime.ToString("yyyy-MM-dd HH:mm:ss") : value;
                        });


                        return result;
                    }
                ).ToList();

                return complexResults;
            }
        }

        var results = new List<Dictionary<string, object>> { };

        if (linqQuery is int || linqQuery is double)
        {
            results.Add(new Dictionary<string, object> { { "Value", linqQuery } });
        }
        // else
        // {
        //     results.Add(linqQuery.GetType().GetProperties().ToDictionary(p => NormalizeColumnName(p.Name), p =>
        //     {
        //         var value = p.GetValue(linqQuery);
        //         return value is DateTime dateTime ? dateTime.ToString("yyyy-MM-dd HH:mm:ss") : value;
        //     }));
        // }

        return results;
    }

    private static bool CompareResults(List<Dictionary<string, object>> linqResults, List<Dictionary<string, object>> sqlResults)
    {
        // there is this one case where we only compare one field. in this case,
        // we will compare the values directly

        if (linqResults.Count > 0 && linqResults[0].Count == 1)
        {
            var linqStringsWithoutKeys = linqResults.Select(dict => dict.Values.First().ToString()).ToList();
            var sqlStringsWithoutKeys = sqlResults.Select(dict => dict.Values.First().ToString()).ToList();

            bool areEqualWithoutKeys = linqStringsWithoutKeys.Count == sqlStringsWithoutKeys.Count && !linqStringsWithoutKeys.Except(sqlStringsWithoutKeys).Any();

            // Console.WriteLine("Linq results:");
            // Console.WriteLine(string.Join("\n", linqStringsWithoutKeys));
            // Console.WriteLine("Sql results:");
            // Console.WriteLine(string.Join("\n", sqlStringsWithoutKeys));

            return areEqualWithoutKeys;
        }

        var linqStrings = linqResults.Select(dict => string.Join(", ", dict.Select(kv => $"{kv.Key}={kv.Value}"))).ToList();
        var sqlStrings = sqlResults.Select(dict => string.Join(", ", dict.Select(kv => $"{kv.Key}={kv.Value}"))).ToList();

        // Console.WriteLine("Linq results:");
        // Console.WriteLine(string.Join("\n", linqStrings));
        // Console.WriteLine("Sql results:");
        // Console.WriteLine(string.Join("\n", sqlStrings));

        bool areEqual = linqStrings.Count == sqlStrings.Count && !linqStrings.Except(sqlStrings).Any();

        return areEqual;
    }

    public static bool Test(object linqQuery, string sqlQuery, DbContext context)
    {
        var linqResults = ExecuteLinqQuery<object>(linqQuery);
        var sqlResults = ExecuteSqlQuery(sqlQuery, context);


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

}
