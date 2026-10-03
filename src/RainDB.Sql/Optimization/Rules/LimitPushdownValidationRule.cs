using RainDB.Logical;

namespace RainDB.Sql.Optimization.Rules;

/// <summary>Ensures LIMIT without ORDER BY remains on the logical root (identity check for future push rules).</summary>
internal sealed class LimitPushdownValidationRule : ILogicalRewriteRule
{
    public string Name => "limit_pushdown_validate";

    public LogicalPlan Apply(LogicalPlan plan) => plan;
}
