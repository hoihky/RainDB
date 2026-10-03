using RainDB.Execution;
using RainDB.Schema;

namespace RainDB.Query.Plans;

/// <summary>Grouped inner join followed by in-memory sort / limit on aggregate output.</summary>
public sealed class GroupedJoinSortTopNPhysicalPlan : IPhysicalPlan
{
    public GroupedJoinSortTopNPhysicalPlan(
        GroupedJoinPhysicalPlan groupedJoin,
        TableSchema outputSchema,
        SortKeyPhysicalSpec[] sortKeys,
        int? limit,
        VectorizedScanExecutionOptions options = default)
    {
        ArgumentNullException.ThrowIfNull(groupedJoin);
        ArgumentNullException.ThrowIfNull(sortKeys);
        GroupedJoin = groupedJoin;
        OutputSchema = outputSchema ?? throw new ArgumentNullException(nameof(outputSchema));
        SortKeys = (SortKeyPhysicalSpec[])sortKeys.Clone();
        Limit = limit;
        Options = options;
        if (limit is < 1)
            throw new ArgumentOutOfRangeException(nameof(limit), "LIMIT must be a positive integer.");
    }

    public GroupedJoinPhysicalPlan GroupedJoin { get; }

    public TableSchema OutputSchema { get; }

    public SortKeyPhysicalSpec[] SortKeys { get; }

    public int? Limit { get; }

    public VectorizedScanExecutionOptions Options { get; }

    public string Explain(string indent = "") =>
        $"{indent}GroupedJoinSortTopN{(Limit is { } n ? $" LIMIT({n})" : "")}\n{GroupedJoin.Explain(indent + "  ")}";
}
