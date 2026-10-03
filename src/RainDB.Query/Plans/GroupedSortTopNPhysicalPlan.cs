using RainDB.Execution;
using RainDB.Schema;

namespace RainDB.Query.Plans;

/// <summary>Hash aggregate followed by in-memory sort / limit on grouped output.</summary>
public sealed class GroupedSortTopNPhysicalPlan : IPhysicalPlan
{
    public GroupedSortTopNPhysicalPlan(
        HashAggregatePhysicalPlan aggregate,
        TableSchema outputSchema,
        SortKeyPhysicalSpec[] sortKeys,
        int? limit,
        VectorizedScanExecutionOptions options = default)
    {
        ArgumentNullException.ThrowIfNull(aggregate);
        ArgumentNullException.ThrowIfNull(sortKeys);
        Aggregate = aggregate;
        OutputSchema = outputSchema ?? throw new ArgumentNullException(nameof(outputSchema));
        SortKeys = (SortKeyPhysicalSpec[])sortKeys.Clone();
        Limit = limit;
        Options = options;
        if (limit is < 1)
            throw new ArgumentOutOfRangeException(nameof(limit), "LIMIT must be a positive integer.");
    }

    public HashAggregatePhysicalPlan Aggregate { get; }

    public TableSchema OutputSchema { get; }

    public SortKeyPhysicalSpec[] SortKeys { get; }

    public int? Limit { get; }

    public VectorizedScanExecutionOptions Options { get; }

    public string Explain(string indent = "") =>
        $"{indent}GroupedSortTopN{(Limit is { } n ? $" LIMIT({n})" : "")}\n{Aggregate.Explain(indent + "  ")}";
}
