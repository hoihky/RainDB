using RainDB.Execution;
using RainDB.Schema;

namespace RainDB.Query.Plans;

/// <summary>Uncorrelated <c>IN</c> / <c>NOT IN</c>: membership tested against one subquery result column.</summary>
public readonly record struct SubqueryInPhysicalSpec(
    int ColumnIndex,
    RainDbType ColumnType,
    bool Negated,
    IPhysicalPlan Subquery,
    int SubqueryResultColumnIndex,
    CorrelatedEqualityBinding[]? Correlations = null);

/// <summary>Which row supplies the outer side of a correlation binding.</summary>
public enum CorrelatedOuterColumnSource : byte
{
    SingleTable,
    JoinProbe,
    JoinBuild,
}

/// <summary>Equality between one outer column and one inner column (correlated execution).</summary>
public readonly record struct CorrelatedEqualityBinding(
    int OuterColumnIndex,
    int InnerColumnIndex,
    RainDbType CompareType,
    CorrelatedOuterColumnSource OuterSource = CorrelatedOuterColumnSource.SingleTable);

/// <summary><c>EXISTS</c> / <c>NOT EXISTS</c> (uncorrelated or correlated).</summary>
public sealed class SubqueryExistsPhysicalSpec
{
    public required bool Negated { get; init; }

    public required IPhysicalPlan Subquery { get; init; }

    public CorrelatedEqualityBinding[]? Correlations { get; init; }
}
