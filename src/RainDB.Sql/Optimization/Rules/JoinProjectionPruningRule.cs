using RainDB.Logical;

namespace RainDB.Sql.Optimization.Rules;

/// <summary>Trims join SELECT projections to columns referenced by ORDER BY when both are explicit lists.</summary>
internal sealed class JoinProjectionPruningRule : ILogicalRewriteRule
{
    private readonly LogicalPlanCloner _cloner = new();

    public string Name => "join_projection_prune";

    public LogicalPlan Apply(LogicalPlan plan)
    {
        if (plan.Root is not LogicalInnerJoin join)
            return plan;
        if (join.GroupByColumns is { Count: > 0 })
            return plan;
        if (join.SelectProjection is not { Count: > 0 } projection)
            return plan;
        if (join.OrderBy is not { Count: > 0 } orderBy)
            return plan;

        var needed = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var p in projection)
            needed.Add(JoinColumnKey(p));

        foreach (var k in orderBy)
            needed.Add(JoinColumnKey(k.Column));

        if (needed.Count == projection.Count)
            return plan;

        var pruned = new List<LogicalColumnProjection>();
        foreach (var p in projection)
        {
            if (needed.Contains(JoinColumnKey(p)))
                pruned.Add(p);
        }

        return new LogicalPlan(_cloner.CloneJoin(join, selectProjectionOverride: pruned), plan.ExplainLevel);
    }

    private static string JoinColumnKey(LogicalColumnProjection p) =>
        $"{p.QualifierTableName ?? ""}\u001f{p.ColumnName}";
}
