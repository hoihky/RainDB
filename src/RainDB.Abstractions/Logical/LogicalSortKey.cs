namespace RainDB.Logical;

/// <summary>One key in <c>ORDER BY</c> (column reference + sort direction).</summary>
public sealed class LogicalSortKey
{
    public required LogicalColumnProjection Column { get; init; }

    /// <summary><see langword="false"/> = ascending (default).</summary>
    public bool Descending { get; init; }
}
