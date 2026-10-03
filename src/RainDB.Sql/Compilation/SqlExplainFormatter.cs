using RainDB.Execution;
using RainDB.Logical;
using RainDB.Query.Plans;

namespace RainDB.Sql.Compilation;

/// <summary>Formats logical and physical explain trees for EXPLAIN output.</summary>
public sealed class SqlExplainFormatter
{
    public string FormatLogical(LogicalPlan plan) => "LOGICAL:\n  " + plan.Explain();

    public string FormatPhysical(IPhysicalPlan plan) => "PHYSICAL:\n  " + plan.Explain();

    public string FormatBundle(LogicalPlan logical, IPhysicalPlan physical, SqlExplainLevel level) =>
        level switch
        {
            SqlExplainLevel.Logical => FormatLogical(logical),
            SqlExplainLevel.Physical => FormatPhysical(physical),
            _ => FormatLogical(logical) + "\n" + FormatPhysical(physical),
        };
}
