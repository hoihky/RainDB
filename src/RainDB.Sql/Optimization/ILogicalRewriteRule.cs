using RainDB.Logical;

namespace RainDB.Sql.Optimization;

/// <summary>Single logical rewrite (strategy pattern).</summary>
public interface ILogicalRewriteRule
{
    string Name { get; }

    LogicalPlan Apply(LogicalPlan plan);
}
