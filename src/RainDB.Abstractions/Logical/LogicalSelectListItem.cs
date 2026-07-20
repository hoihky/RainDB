using RainDB.Execution;

namespace RainDB.Logical;

/// <summary>Single entry in a SELECT list (row projection or grouped query).</summary>
public abstract class LogicalSelectListItem
{
}

/// <summary>Non-aggregated column reference.</summary>
public sealed class LogicalColumnProjection : LogicalSelectListItem
{
    /// <summary>Optional table qualifier (<c>table.column</c>).</summary>
    public string? QualifierTableName { get; init; }

    public required string ColumnName { get; init; }
}

/// <summary>Aggregate function call (including <c>COUNT(*)</c>).</summary>
public sealed class LogicalAggregationCall : LogicalSelectListItem
{
    public required AggregateKind Kind { get; init; }

    /// <summary>Optional table qualifier for the aggregate argument (<c>SUM(t.col)</c>).</summary>
    public string? ArgumentQualifierTableName { get; init; }

    /// <summary>Target column for <c>SUM</c>/<c>MIN</c>/<c>MAX</c>/<c>COUNT(col)</c>; <see langword="null"/> for <c>COUNT(*)</c>.</summary>
    public string? ArgumentColumnName { get; init; }
}
