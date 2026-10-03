using RainDB.Execution;
using RainDB.Logical;

namespace RainDB.Query.Plans;

/// <summary>Carries logical + physical explain text; execution returns formatted output only.</summary>
public sealed class ExplainBundlePhysicalPlan : IPhysicalPlan
{
    public ExplainBundlePhysicalPlan(string logicalText, string physicalText, SqlExplainLevel level)
    {
        LogicalText = logicalText ?? throw new ArgumentNullException(nameof(logicalText));
        PhysicalText = physicalText ?? throw new ArgumentNullException(nameof(physicalText));
        Level = level;
    }

    public string LogicalText { get; }

    public string PhysicalText { get; }

    public SqlExplainLevel Level { get; }

    public string Explain(string indent = "") =>
        Level switch
        {
            SqlExplainLevel.Logical => $"{indent}{LogicalText}",
            SqlExplainLevel.Physical => $"{indent}{PhysicalText}",
            _ => $"{indent}{LogicalText}\n{indent}{PhysicalText}",
        };
}
