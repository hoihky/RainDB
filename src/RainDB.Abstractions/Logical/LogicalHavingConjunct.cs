using RainDB.Execution;

namespace RainDB.Logical;

/// <summary>Post-aggregation filter: group key column or aggregate compared to a literal.</summary>
public sealed class LogicalHavingConjunct
{
    public LogicalColumnProjection? GroupKeyColumn { get; init; }

    public LogicalAggregationCall? Aggregate { get; init; }

    public required ScalarCompareOp Operator { get; init; }

    public required SqlLiteral Literal { get; init; }
}
