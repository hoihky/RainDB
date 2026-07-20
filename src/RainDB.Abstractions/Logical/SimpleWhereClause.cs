using RainDB.Execution;

namespace RainDB.Logical;

/// <summary>Single-column predicate (strict SQL subset).</summary>
public sealed class SimpleWhereClause
{
    /// <summary>When set, <see cref="ColumnName"/> is on this table (<c>table.column</c> form).</summary>
    public string? QualifierTableName { get; init; }

    public required string ColumnName { get; init; }

    public required ScalarCompareOp Operator { get; init; }

    public required SqlLiteral Literal { get; init; }
}
