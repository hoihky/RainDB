namespace RainDB.Logical;

/// <summary>Uncorrelated subquery predicate in <c>WHERE</c> (<c>IN</c> / <c>EXISTS</c>).</summary>
public sealed class LogicalUncorrelatedSubqueryPredicate
{
    public enum Kind
    {
        In,
        NotIn,
        Exists,
        NotExists,
    }

    public required Kind PredicateKind { get; init; }

    /// <summary>Column compared for <see cref="Kind.In"/> / <see cref="Kind.NotIn"/>.</summary>
    public LogicalColumnScalarRef? Column { get; init; }

    public required LogicalSubquery Subquery { get; init; }

    /// <summary>Non-empty when the subquery is correlated to the outer query.</summary>
    public IReadOnlyList<LogicalSubqueryCorrelation>? Correlations { get; init; }
}
