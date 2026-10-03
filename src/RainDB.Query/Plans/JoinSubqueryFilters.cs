namespace RainDB.Query.Plans;

/// <summary>Uncorrelated subquery predicates bound to probe/build sides of a join.</summary>
public sealed class JoinSubqueryPhysicalSpecs
{
    public SubqueryInPhysicalSpec[]? ProbeInSubqueries { get; init; }

    public SubqueryInPhysicalSpec[]? BuildInSubqueries { get; init; }

    public SubqueryExistsPhysicalSpec[]? ExistsSubqueries { get; init; }

    public bool HasAny =>
        ProbeInSubqueries is { Length: > 0 }
        || BuildInSubqueries is { Length: > 0 }
        || ExistsSubqueries is { Length: > 0 };
}
