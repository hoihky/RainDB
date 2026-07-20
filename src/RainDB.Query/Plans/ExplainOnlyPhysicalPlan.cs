using RainDB.Execution;

namespace RainDB.Query.Plans;

/// <summary>Placeholder physical plan until operator pipeline lands (OCP: replace with real tree).</summary>
public sealed class ExplainOnlyPhysicalPlan : IPhysicalPlan
{
    public ExplainOnlyPhysicalPlan(string label) => Label = label;

    public string Label { get; }

    public string Explain(string indent = "") => $"{indent}{Label}";
}
