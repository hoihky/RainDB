using RainDB.Execution;

namespace RainDB.Logical;

/// <summary>Single-column global aggregate (no GROUP BY).</summary>
public sealed class LogicalAggregate
{
    public required AggregateKind Kind { get; init; }

    /// <summary>Target column; <see langword="null"/> only for <c>COUNT(*)</c>.</summary>
    public string? ColumnName { get; init; }
}
