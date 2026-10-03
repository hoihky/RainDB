using RainDB.Logical;

namespace RainDB.Sql.Optimization.Rules;

/// <summary>Partitions join WHERE conjuncts onto probe/build sides when table qualifiers are known.</summary>
internal sealed class JoinPredicatePartitionRule : ILogicalRewriteRule
{
    private readonly LogicalPlanCloner _cloner = new();

    public string Name => "join_predicate_partition";

    public LogicalPlan Apply(LogicalPlan plan)
    {
        if (plan.Root is not LogicalInnerJoin join)
            return plan;
        if (join.ProbeSideWhereConjuncts is not null || join.BuildSideWhereConjuncts is not null)
            return plan;
        if (join.WhereConjuncts is not { Count: > 0 })
            return plan;

        var probe = new List<SimpleWhereClause>();
        var build = new List<SimpleWhereClause>();
        var residual = new List<SimpleWhereClause>();

        foreach (var w in join.WhereConjuncts)
        {
            if (w.QualifierTableName is { } qt)
            {
                if (TableEq(qt, join.LeftTableName))
                    probe.Add(w);
                else if (TableEq(qt, join.RightTableName))
                    build.Add(w);
                else
                    residual.Add(w);
            }
            else
                residual.Add(w);
        }

        var copy = _cloner.CloneJoin(
            join,
            whereOverride: residual,
            probeWhereOverride: probe,
            buildWhereOverride: build);
        return new LogicalPlan(copy, plan.ExplainLevel);
    }

    private static bool TableEq(string a, string b) => a.Equals(b, StringComparison.OrdinalIgnoreCase);
}
