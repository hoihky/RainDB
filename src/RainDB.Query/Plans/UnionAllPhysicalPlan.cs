using RainDB.Execution;
using RainDB.Schema;

namespace RainDB.Query.Plans;

/// <summary>Concatenates output batches from child plans with identical column layout.</summary>
public sealed class UnionAllPhysicalPlan : IPhysicalPlan
{
    public UnionAllPhysicalPlan(IPhysicalPlan[] inputs, TableSchema outputSchema)
    {
        ArgumentNullException.ThrowIfNull(inputs);
        ArgumentNullException.ThrowIfNull(outputSchema);
        if (inputs.Length < 2)
            throw new ArgumentException("UNION ALL requires at least two branches.", nameof(inputs));

        Inputs = (IPhysicalPlan[])inputs.Clone();
        OutputSchema = outputSchema;
    }

    public IPhysicalPlan[] Inputs { get; }

    public TableSchema OutputSchema { get; }

    public string Explain(string indent = "") =>
        $"{indent}UnionAll[{Inputs.Length}] SCHEMA({OutputSchema.Columns.Count} cols)";
}
