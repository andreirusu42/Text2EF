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

    private string SerializeResults(List<Dictionary<string, object>> results)
    {
        var serialized = new System.Text.StringBuilder();
        foreach (var row in results)
        {
            foreach (var kvp in row)
            {
                serialized.Append($"{kvp.Key}: {kvp.Value}, ");
            }
            serialized.AppendLine();
        }
        return serialized.ToString();
    }
}
