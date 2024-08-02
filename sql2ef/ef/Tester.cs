using Microsoft.EntityFrameworkCore;

class Tester
{
    public static List<Dictionary<string, object>> ExecuteSqlQuery(string query, DbContext context)
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
                        var value = reader.GetValue(i);

                        if (value is System.DBNull)
                        {
                            continue;
                        }

                        var columnName = $"{i}";
                        row[columnName] = value;
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
        if (linqQuery is String stringResult)
        {
            return new List<Dictionary<string, object>> { new Dictionary<string, object> { { "Value", stringResult } } };
        }
        else if (linqQuery is System.Collections.IEnumerable query)
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


            var isPrimitive = firstItem.GetType().IsPrimitive || firstItem is string || firstItem is int || firstItem is double;

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

                        var result = new Dictionary<string, object>();
                        foreach (var p in item.GetType().GetProperties())
                        {
                            var value = p.GetValue(item);

                            var isList = value != null && value is IEnumerable<object>;

                            if (value != null && !isList)
                            {
                                result[$"{key}"] = value is DateTime dateTime ? dateTime.ToString("yyyy-MM-dd HH:mm:ss") : value;
                            }
                            key += 1;
                        }

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
        //     results.Add(linqQuery.GetType().GetProperties().ToDictionary(p => p.Name, p =>
        //     {
        //         var value = p.GetValue(linqQuery);
        //         return value is DateTime dateTime ? dateTime.ToString("yyyy-MM-dd HH:mm:ss") : value;
        //     }));
        // }


        return results;
    }

    private static void CompareResults(List<Dictionary<string, object>> linqResults, List<Dictionary<string, object>> sqlResults)
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

            if (!areEqualWithoutKeys)
            {
                throw new ResultsAreNotEqualException(sqlResults, linqResults);
            }

            return;
        }

        var linqStrings = linqResults.Select(dict => string.Join(", ", dict.Select(kv => $"{kv.Key}={kv.Value}"))).ToList();
        var sqlStrings = sqlResults.Select(dict => string.Join(", ", dict.Select(kv => $"{kv.Key}={kv.Value}"))).ToList();

        // for (int i = 0; i < linqStrings.Count; i++)
        // {
        //     Console.WriteLine($"LINQ: {linqStrings[i]}");
        //     Console.WriteLine($"SQL-: {sqlStrings[i]}");
        // }

        // Check where linqStrings[i] differs to sqlStrings[i]

        // for (int i = 0; i < linqStrings.Count; i++)
        // {
        //     if (linqStrings[i] != sqlStrings[i])
        //     {
        //         Console.WriteLine($"LINQ: {linqStrings[i]}");
        //         Console.WriteLine($"SQL-: {sqlStrings[i]}");
        //         Console.WriteLine($"for {i}");
        //     }
        // }

        bool areEqual = linqStrings.Count == sqlStrings.Count && !linqStrings.Except(sqlStrings).Any();

        if (!areEqual)
        {
            throw new ResultsAreNotEqualException(sqlResults, linqResults);
        }
    }

    public static void Test(object linqQuery, string sqlQuery, DbContext context)
    {
        var linqResults = ExecuteLinqQuery<object>(linqQuery);
        var sqlResults = ExecuteSqlQuery(sqlQuery, context);

        // TODO: Issue. You can not simply compare these.
        // Imagine when you have a VARCHAR(7) in sql, which is translated to a Decimal / int in linq.
        // I know this is stupid, but when you scaffold the model, this is how it's generated :/

        // Console.WriteLine("SQL Results:");
        // for (int i = 0; i < sqlResults.Count; i++)
        // {
        //     var row = sqlResults[i];
        //     Console.WriteLine($"Row {i + 1}: {string.Join(", ", row.Select(kv => $"{kv.Key}={kv.Value}"))}");
        // }

        // Console.WriteLine("LINQ Results:");
        // for (int i = 0; i < linqResults.Count; i++)
        // {
        //     var row = linqResults[i];
        //     Console.WriteLine($"Row {i + 1}: {string.Join(", ", row.Select(kv => $"{kv.Key}={kv.Value}"))}");
        // }

        CompareResults(linqResults, sqlResults);
    }

}
