using System.Text;
using RainDB.Execution;

namespace RainDB.Logical;

/// <summary>Single-table scan: row projection, global aggregate, or GROUP BY grouped aggregation.</summary>
public sealed class LogicalTableScan : ILogicalRoot
{
    public required string TableName { get; init; }

    /// <summary>
    /// Explicit column projection for non-aggregate queries; <see langword="null"/> means <c>SELECT *</c>.
    /// </summary>
    public IReadOnlyList<LogicalColumnProjection>? Projection { get; init; }

    /// <summary>
    /// Non-null for <c>GROUP BY</c>: ordered grouping columns (qualified when needed). Requires <see cref="SelectList"/>.
    /// </summary>
    public IReadOnlyList<LogicalColumnProjection>? GroupByColumns { get; init; }

    /// <summary>
    /// SELECT list items for grouped queries (mix of column refs and aggregates), in output order.
    /// </summary>
    public IReadOnlyList<LogicalSelectListItem>? SelectList { get; init; }

    /// <summary>AND conjunction of single-column predicates.</summary>
    public IReadOnlyList<SimpleWhereClause>? WhereConjuncts { get; init; }

    /// <summary>Single-result aggregate without GROUP BY (mutually exclusive with grouped fields).</summary>
    public LogicalAggregate? Aggregate { get; init; }

    /// <summary>Optional <c>ORDER BY</c> (non-grouped, non-global-aggregate queries only).</summary>
    public IReadOnlyList<LogicalSortKey>? OrderBy { get; init; }

    /// <summary>Optional <c>LIMIT</c> (same applicability as <see cref="OrderBy"/>).</summary>
    public int? Limit { get; init; }

    public string Explain(string indent = "")
    {
        var sb = new StringBuilder();
        sb.Append(indent).Append("LogicalTableScan(").Append(TableName).Append(')');
        if (GroupByColumns is { Count: > 0 } gb)
        {
            sb.Append(" GROUP BY(");
            for (var i = 0; i < gb.Count; i++)
            {
                if (i > 0)
                    sb.Append(", ");
                sb.Append(ExplainProjectionCol(gb[i]));
            }

            sb.Append(')');
            if (SelectList is { Count: > 0 } sl)
            {
                sb.Append(" SELECT[");
                for (var i = 0; i < sl.Count; i++)
                {
                    if (i > 0)
                        sb.Append(", ");
                    sb.Append(ExplainSelectItem(sl[i]));
                }

                sb.Append(']');
            }
        }
        else if (Aggregate is { } a)
        {
            sb.Append(" AGG(").Append(a.Kind).Append(' ');
            sb.Append(a.ColumnName ?? "*").Append(')');
        }
        else if (Projection is null)
            sb.Append(" PROJECT(*)");
        else
        {
            sb.Append(" PROJECT(");
            for (var i = 0; i < Projection.Count; i++)
            {
                if (i > 0)
                    sb.Append(", ");
                sb.Append(ExplainProjectionCol(Projection[i]));
            }

            sb.Append(')');
        }

        if (WhereConjuncts is { Count: > 0 } wc)
        {
            sb.Append(" WHERE ");
            for (var i = 0; i < wc.Count; i++)
            {
                if (i > 0)
                    sb.Append(" AND ");
                var w = wc[i];
                if (w.QualifierTableName is { } qt)
                    sb.Append(qt).Append('.');
                sb.Append(w.ColumnName).Append(' ').Append(w.Operator).Append(' ').Append(w.Literal.Text);
            }
        }

        if (OrderBy is { Count: > 0 } ob)
        {
            sb.Append(" ORDER BY ");
            for (var i = 0; i < ob.Count; i++)
            {
                if (i > 0)
                    sb.Append(", ");
                var k = ob[i];
                sb.Append(ExplainProjectionCol(k.Column));
                if (k.Descending)
                    sb.Append(" DESC");
                else
                    sb.Append(" ASC");
            }
        }

        if (Limit is { } lim)
            sb.Append(" LIMIT ").Append(lim);

        return sb.ToString();
    }

    private static string ExplainProjectionCol(LogicalColumnProjection p)
    {
        if (p.QualifierTableName is { } q)
            return $"{q}.{p.ColumnName}";
        return p.ColumnName;
    }

    private static string ExplainSelectItem(LogicalSelectListItem item) =>
        item switch
        {
            LogicalColumnProjection c => ExplainProjectionCol(c),
            LogicalAggregationCall a when a.Kind == AggregateKind.Count && a.ArgumentColumnName is null => "COUNT(*)",
            LogicalAggregationCall a =>
                $"{a.Kind}({(a.ArgumentQualifierTableName is { } aq ? $"{aq}." : "")}{a.ArgumentColumnName})",
            _ => item.ToString() ?? "?",
        };
}

