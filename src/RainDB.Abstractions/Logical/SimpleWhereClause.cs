using RainDB.Execution;

namespace RainDB.Logical;

/// <summary>Single-column predicate (strict SQL subset).</summary>
public sealed class SimpleWhereClause
{
    /// <summary>When set, <see cref="ColumnName"/> is on this table (<c>table.column</c> form).</summary>
    public string? QualifierTableName { get; init; }

    /// <summary>Single-column compare when <see cref="LeftExpression"/> is null.</summary>
    public string ColumnName { get; init; } = "";

    /// <summary>Int32 arithmetic/compare expression on the left-hand side of the predicate.</summary>
    public LogicalScalarExpression? LeftExpression { get; init; }

    public required ScalarCompareOp Operator { get; init; }

    /// <summary>Literal compare value when the predicate is not parameterized.</summary>
    public SqlLiteral? Literal { get; init; }

    /// <summary>Column compare (<c>col = other.col</c>) when set instead of <see cref="Literal"/>.</summary>
    public LogicalColumnScalarRef? CompareColumn { get; init; }

    /// <summary>Parameter name (without <c>@</c>) when the predicate uses <c>@name</c> instead of a literal.</summary>
    public string? ParameterName { get; init; }

    public bool UsesParameter => !string.IsNullOrEmpty(ParameterName);
}
