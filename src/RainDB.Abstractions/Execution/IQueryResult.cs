namespace RainDB.Execution;

/// <summary>Materialized or streaming result surface (LSP: alternate result types later).</summary>
public interface IQueryResult : IAsyncDisposable
{
    long RowCount { get; }
}
