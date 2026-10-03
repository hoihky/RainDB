using RainDB.Logical;

namespace RainDB.Sql.Optimization.Rules;

/// <summary>Trims explicit projections to columns referenced by SELECT and ORDER BY (filters stay on scan).</summary>
internal sealed class TableScanProjectionPruningRule : ILogicalRewriteRule
{
    private readonly LogicalPlanCloner _cloner = new();

    public string Name => "table_scan_projection_prune";

    public LogicalPlan Apply(LogicalPlan plan)
    {
        if (plan.Root is not LogicalTableScan scan)
            return plan;
        if (scan.GroupByColumns is { Count: > 0 } || scan.Aggregate is not null)
            return plan;
        if (scan.Projection is not { Count: > 0 })
            return plan;

        var needed = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var p in scan.Projection)
            needed.Add(p.ColumnName);

        if (scan.OrderBy is { Count: > 0 } ob)
        {
            foreach (var k in ob)
            {
                if (k.Column is null)
                    continue;
                if (k.Column.QualifierTableName is not null
                    && !k.Column.QualifierTableName.Equals(scan.TableName, StringComparison.OrdinalIgnoreCase))
                    continue;
                needed.Add(k.Column.ColumnName);
            }
        }

        if (needed.Count == scan.Projection.Count)
            return plan;

        var pruned = new List<LogicalColumnProjection>();
        foreach (var p in scan.Projection)
        {
            if (needed.Contains(p.ColumnName))
                pruned.Add(p);
        }

        return new LogicalPlan(_cloner.CloneTableScan(scan, pruned), plan.ExplainLevel);
    }
}
