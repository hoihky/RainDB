using RainDB.Execution;
using RainDB.Schema;

namespace RainDB.Query.Plans;

/// <summary>Deduplicates rows from a child plan (full-row distinct on all output columns).</summary>
public sealed class DistinctPhysicalPlan : IPhysicalPlan
{
    public DistinctPhysicalPlan(IPhysicalPlan input, TableSchema outputSchema)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(outputSchema);
        Input = input;
        OutputSchema = outputSchema;
    }

    public IPhysicalPlan Input { get; }

    public TableSchema OutputSchema { get; }

    public string Explain(string indent = "") =>
        $"{indent}Distinct\n{Input.Explain(indent + "  ")}";
}
