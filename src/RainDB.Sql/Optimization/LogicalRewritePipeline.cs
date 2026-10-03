using RainDB.Logical;

namespace RainDB.Sql.Optimization;

/// <summary>Applies ordered rewrite rules to a logical plan (fixed-point optional).</summary>
public sealed class LogicalRewritePipeline
{
    private readonly IReadOnlyList<ILogicalRewriteRule> _rules;

    public LogicalRewritePipeline(IEnumerable<ILogicalRewriteRule> rules)
    {
        ArgumentNullException.ThrowIfNull(rules);
        _rules = rules.ToList();
        if (_rules.Count == 0)
            throw new ArgumentException("At least one rewrite rule is required.", nameof(rules));
    }

    public LogicalRewritePipeline()
        : this(DefaultRules.Create())
    {
    }

    public LogicalPlan Optimize(LogicalPlan plan)
    {
        ArgumentNullException.ThrowIfNull(plan);
        var current = plan;
        foreach (var rule in _rules)
            current = rule.Apply(current);
        return current;
    }
}
