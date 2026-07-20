using RainDB.Execution;

namespace RainDB.Query.Results;

internal sealed class EmptyQueryResult : IQueryResult
{
    public EmptyQueryResult(long rowCount = 0) => RowCount = rowCount;

    public long RowCount { get; }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
