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

                        if (value is DBNull)
                        {
                            continue;
                        }

                        var columnName = $"{i}";
                        row[columnName] = value;
                    }

                    if (row.Count == 0)
                    {
                        continue;
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
            return new List<Dictionary<string, object>> { new Dictionary<string, object> { { "0", stringResult } } };
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
                    new Dictionary<string, object> { { "0", item } }).ToList();
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

                        return result.Count > 0 ? result : null;
                    }
                ).Where(rez => rez != null)
                .ToList();

                return complexResults;
            }
        }

        var results = new List<Dictionary<string, object>> { };

        if (linqQuery is int || linqQuery is double)
        {
            results.Add(new Dictionary<string, object> { { "0", linqQuery } });
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

        // if (sqlResults.Count != linqResults.Count)
        // {
        //     Console.WriteLine($"SQL Results: {sqlResults.Count}");
        //     Console.WriteLine($"LINQ Results: {linqResults.Count}");
        // }



        // This is some kind of normalising step, even though, if you ask me, it'd be better to check the types of the columns and compare based on that :/
        if (sqlResults.Count == linqResults.Count)
        {
            for (int i = 0; i < sqlResults.Count; ++i)
            {
                var sql = sqlResults[i];
                var linq = linqResults[i];

                for (int j = 0; j < sql.Count; ++j)
                {
                    var sqlKey = sql.Keys.ElementAt(j);
                    var linqKey = linq.Keys.ElementAt(j);

                    var sqlValue = sql[sqlKey];
                    var linqValue = linq[linqKey];

                    // Console.WriteLine($"SQL Key: {sqlKey} ({sqlValue.GetType()})");
                    // Console.WriteLine($"LINQ Key: {linqKey} ({linqValue.GetType()})");
                    // Console.WriteLine($"SQL Value: {sqlValue}");
                    // Console.WriteLine($"LINQ Value: {linqValue}");

                    /*
                        Also, this is incredible too. When running in Rust, you can get this:

                         "SQL Results:\nRow 1: 0=7, 1=495.063\nRow 2: 0=61, 1=930.14\nRow 3: 0=98, 1=6035.84\nRow 4: 0=136, 1=199.52\nRow 5: 0=164, 1=12223.93\nRow 6: 0=209, 1=11130.23\nRow 7: 0=211, 1=1230.454\nRow 8: 0=240, 1=6352.31\nRow 9: 0=262, 1=147.96\nRow 10: 0=280, 1=187.14\nRow 11: 0=321, 1=745.817\nRow 12: 0=346, 1=127.9\nRow 13: 0=414, 1=25.41\nRow 14: 0=427, 1=1168.32\nRow 15: 0=451, 1=658.26\n
                         LINQ Results:\nRow 1: 0=7, 1=495,063\nRow 2: 0=61, 1=930,14\nRow 3: 0=98, 1=6035,84\nRow 4: 0=136, 1=199,52\nRow 5: 0=164, 1=12223,93\nRow 6: 0=209, 1=11130,23\nRow 7: 0=211, 1=1230,454\nRow 8: 0=240, 1=6352,31\nRow 9: 0=262, 1=147,96\nRow 10: 0=280, 1=187,14\nRow 11: 0=321, 1=745,817\nRow 12: 0=346, 1=127,9\nRow 13: 0=414, 1=25,41\nRow 14: 0=427, 1=1168,32\nRow 15: 0=451, 1=658,26\n"

                        For some reason, the decimal point is a comma in the LINQ results
                    */
                    if (linqValue is decimal linqDecimal)
                    {
                        linqResults[i][linqKey] = linqDecimal.ToString().Replace(",", ".");
                    }

                    if (sqlValue is string sqlString)
                    {
                        if (linqValue is double linqDouble)
                        {
                            if (sqlString == "" && linqDouble == 0)
                            {
                                sqlResults[i][sqlKey] = "0";
                            }
                        }
                        else if (linqValue is Boolean linqBoolean)
                        {
                            if (sqlString == "")
                            {
                                sqlResults[i][sqlKey] = false;
                            }
                            else if (sqlString == "1")
                            {
                                sqlResults[i][sqlKey] = true;
                            }
                        }
                    }

                    else if (sqlValue is DBNull)
                    {
                        if (linqValue is double linqDouble)
                        {
                            sqlResults[i][sqlKey] = 0;
                        }
                        else if (linqValue is Boolean linqBoolean)
                        {
                            sqlResults[i][sqlKey] = false;
                        }
                    }

                    // Console.WriteLine($"SQL Value: {sqlValue} ({sqlValue.GetType()})");
                    // Console.WriteLine($"LINQ Value: {linqValue} ({linqValue.GetType()})");
                }
            }
        }
        else
        {
            if (linqResults.Count == 1 && sqlResults.Count == 0)
            {
                var linq = linqResults[0];


                if (linq.Count != 1)
                {
                    throw new Exception("sad face");
                }

                var linqValue = linq.Values.First();

                // Console.WriteLine($"LINQ Value: {linqValue} ({linqValue.GetType()})");

                if (linqValue is int linqInt && linqInt == 0 || linqValue is double linqDouble && linqDouble == 0)
                {
                    linqResults.RemoveAt(0);
                }
            }
        }



        CompareResults(linqResults, sqlResults);
    }

}
