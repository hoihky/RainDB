using RainDB.Execution;

namespace RainDB.Logical;

/// <summary>Single-column predicate (strict SQL subset).</summary>
public sealed class SimpleWhereClause
{
    /// <summary>When set, <see cref="ColumnName"/> is on this table (<c>table.column</c> form).</summary>
    public string? QualifierTableName { get; init; }

    public required string ColumnName { get; init; }

    public required ScalarCompareOp Operator { get; init; }

    /// <summary>Literal compare value when the predicate is not parameterized.</summary>
    public SqlLiteral? Literal { get; init; }

    /// <summary>Parameter name (without <c>@</c>) when the predicate uses <c>@name</c> instead of a literal.</summary>
    public string? ParameterName { get; init; }

    public bool UsesParameter => !string.IsNullOrEmpty(ParameterName);
}
