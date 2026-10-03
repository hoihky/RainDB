using RainDB.Execution;

namespace RainDB.Query.Results;

/// <summary>Textual EXPLAIN output (not columnar).</summary>
public sealed class ExplainTextQueryResult : IQueryResult
{
    public ExplainTextQueryResult(string text) => Text = text ?? throw new ArgumentNullException(nameof(text));

    public string Text { get; }

    public long RowCount => 1;

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
