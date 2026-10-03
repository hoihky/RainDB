using System.Text;
using RainDB.Execution;

namespace RainDB.Logical;

/// <summary>Query over a derived table: <c>FROM (SELECT …) alias</c>.</summary>
public sealed class LogicalDerivedTableScan : ILogicalRoot
{
    public required string Alias { get; init; }

    public required LogicalSubquery Subquery { get; init; }

    public IReadOnlyList<LogicalColumnProjection>? Projection { get; init; }

    /// <summary>Explicit SELECT list when it includes scalar expressions (column-only lists use <see cref="Projection"/>).</summary>
    public IReadOnlyList<LogicalSelectListItem>? SelectList { get; init; }

    public IReadOnlyList<SimpleWhereClause>? WhereConjuncts { get; init; }

    public IReadOnlyList<LogicalUncorrelatedSubqueryPredicate>? SubqueryPredicates { get; init; }

    public IReadOnlyList<LogicalSortKey>? OrderBy { get; init; }

    public int? Limit { get; init; }

    public string Explain(string indent = "")
    {
        var sb = new StringBuilder();
        sb.Append(indent).Append("LogicalDerivedTableScan(").Append(Alias).Append(") SUBQUERY ");
        sb.AppendLine().Append(Subquery.Root.Explain(indent + "  "));
        if (WhereConjuncts is { Count: > 0 } wc)
            sb.Append(indent).Append(" WHERE …");
        if (OrderBy is { Count: > 0 })
            sb.Append(" ORDER BY …");
        if (Limit is { } lim)
            sb.Append(" LIMIT ").Append(lim);
        return sb.ToString();
    }
}
