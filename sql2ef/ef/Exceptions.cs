public class ResultsAreNotEqualException : Exception
{
    public List<Dictionary<string, object>> SqlResults { get; }
    public List<Dictionary<string, object>> LinqResults { get; }

    public ResultsAreNotEqualException(
        List<Dictionary<string, object>> sqlResults,
        List<Dictionary<string, object>> linqResults
    ) : base("Results are not equal.")
    {
        SqlResults = sqlResults;
        LinqResults = linqResults;
    }

    public override string ToString()
    {
        return $"{base.ToString()}\nSQL Results: {SerializeResults(SqlResults)}\nLINQ Results: {SerializeResults(LinqResults)}";
    }


    // TODO: use Newtonsoft.Json
    private string SerializeResults(List<Dictionary<string, object>> results)
    {
        var serialized = new System.Text.StringBuilder();
        serialized.Append("[");

        for (int i = 0; i < results.Count; i++)
        {
            var row = results[i];
            serialized.Append("{");

            int j = 0;
            foreach (var kvp in row)
            {
                serialized.Append($"{kvp.Key}: {kvp.Value}");
                if (j < row.Count - 1)
                {
                    serialized.Append(", ");
                }
                j++;
            }

            serialized.Append("}");
            if (i < results.Count - 1)
            {
                serialized.Append(", ");
            }
        }

        serialized.Append("]");
        return serialized.ToString();
    }

}
