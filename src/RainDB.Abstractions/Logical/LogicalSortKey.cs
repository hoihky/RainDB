namespace RainDB.Logical;

/// <summary>One key in <c>ORDER BY</c> (column reference + sort direction).</summary>
public sealed class LogicalSortKey
{
    /// <summary>Sort by column when <see cref="SortExpression"/> is null.</summary>
    public LogicalColumnProjection? Column { get; init; }

    /// <summary>Expression sort key (non-grouped queries).</summary>
    public LogicalScalarExpression? SortExpression { get; init; }

    /// <summary><see langword="false"/> = ascending (default).</summary>
    public bool Descending { get; init; }
}
