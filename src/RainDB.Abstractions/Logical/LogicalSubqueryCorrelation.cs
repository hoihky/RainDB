namespace RainDB.Logical;

/// <summary>Equality link between an outer row column and an inner subquery column (correlated execution).</summary>
public sealed class LogicalSubqueryCorrelation
{
    public required LogicalColumnScalarRef OuterColumn { get; init; }

    public required LogicalColumnScalarRef InnerColumn { get; init; }
}
