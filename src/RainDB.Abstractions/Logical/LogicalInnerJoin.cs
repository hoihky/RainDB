using System.Text;
using RainDB.Execution;

namespace RainDB.Logical;

/// <summary>Inner join of two base tables with one or more AND-ed equi-predicates (<c>tbl.col = tbl.col</c>).</summary>
public sealed class LogicalInnerJoin : ILogicalRoot
{
    public required string LeftTableName { get; init; }

    public required string RightTableName { get; init; }

    /// <summary>Key columns on the left table (same length as <see cref="RightKeyColumns"/>).</summary>
    public required IReadOnlyList<LogicalQualifiedColumn> LeftKeyColumns { get; init; }

    /// <summary>Key columns on the right table.</summary>
    public required IReadOnlyList<LogicalQualifiedColumn> RightKeyColumns { get; init; }

    /// <summary><see langword="null"/> means <c>SELECT *</c> (all left columns then all right).</summary>
    public IReadOnlyList<LogicalColumnProjection>? SelectProjection { get; init; }

    /// <summary>AND conjunction of predicates (fixed-width or UTF-8 with string literals).</summary>
    public IReadOnlyList<SimpleWhereClause>? WhereConjuncts { get; init; }

    /// <summary>
    /// When set with <see cref="SelectList"/>, runs grouped aggregation over the join result (<c>SELECT … GROUP BY</c>).
    /// <see cref="SelectProjection"/> must be <see langword="null"/> for this shape.
    /// </summary>
    public IReadOnlyList<LogicalColumnProjection>? GroupByColumns { get; init; }

    /// <summary>Grouped <c>SELECT</c> list (keys + aggregates). Used only when <see cref="GroupByColumns"/> is set.</summary>
    public IReadOnlyList<LogicalSelectListItem>? SelectList { get; init; }

    /// <summary>Optional row ordering on the join result. Not supported with <see cref="GroupByColumns"/>.</summary>
    public IReadOnlyList<LogicalSortKey>? OrderBy { get; init; }

    /// <summary>Optional <c>LIMIT</c> after sort (or join row order if no <see cref="OrderBy"/>).</summary>
    public int? Limit { get; init; }

    public string Explain(string indent = "")
    {
        var sb = new StringBuilder();
        sb.Append(indent).Append("LogicalInnerJoin(").Append(LeftTableName).Append(", ").Append(RightTableName).Append(") ON ");
        for (var i = 0; i < LeftKeyColumns.Count; i++)
        {
            if (i > 0)
                sb.Append(" AND ");
            var l = LeftKeyColumns[i];
            var r = RightKeyColumns[i];
            sb.Append(l.TableName).Append('.').Append(l.ColumnName).Append('=')
                .Append(r.TableName).Append('.').Append(r.ColumnName);
        }

        if (SelectProjection is { Count: > 0 } sp)
        {
            sb.Append(" PROJECT(");
            for (var i = 0; i < sp.Count; i++)
            {
                if (i > 0)
                    sb.Append(", ");
                var p = sp[i];
                if (p.QualifierTableName is { } qt)
                    sb.Append(qt).Append('.');
                sb.Append(p.ColumnName);
            }

            sb.Append(')');
        }
        else if (GroupByColumns is { Count: > 0 } gb)
        {
            sb.Append(" GROUP BY(");
            for (var i = 0; i < gb.Count; i++)
            {
                if (i > 0)
                    sb.Append(", ");
                var g = gb[i];
                if (g.QualifierTableName is { } gq)
                    sb.Append(gq).Append('.');
                sb.Append(g.ColumnName);
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
        else
            sb.Append(" PROJECT(*)");

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
                if (k.Column.QualifierTableName is { } oq)
                    sb.Append(oq).Append('.');
                sb.Append(k.Column.ColumnName);
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

    private static string ExplainSelectItem(LogicalSelectListItem item) =>
        item switch
        {
            LogicalColumnProjection c => c.QualifierTableName is { } q ? $"{q}.{c.ColumnName}" : c.ColumnName,
            LogicalAggregationCall a when a.Kind == AggregateKind.Count && a.ArgumentColumnName is null => "COUNT(*)",
            LogicalAggregationCall a =>
                $"{a.Kind}({(a.ArgumentQualifierTableName is { } aq ? $"{aq}." : "")}{a.ArgumentColumnName})",
            _ => item.ToString() ?? "?",
        };
}

