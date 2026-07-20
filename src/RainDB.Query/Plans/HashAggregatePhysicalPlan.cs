using RainDB.Catalog;
using RainDB.Execution;

namespace RainDB.Query.Plans;

/// <summary>Maps each SELECT output column (after GROUP BY) to a key component or aggregate output.</summary>
public enum HashAggregateOutputColumnKind
{
    GroupKey,
    Aggregate,
}

/// <summary>
/// <see cref="Ordinal"/> indexes into either <see cref="HashAggregatePhysicalPlan.GroupKeyColumnIndices"/> or <see cref="HashAggregatePhysicalPlan.Aggregates"/>.
/// </summary>
public readonly record struct HashAggregateOutputSlot(HashAggregateOutputColumnKind Kind, int Ordinal);

/// <summary>
/// Hash-based grouped aggregation over <see cref="IColumnarTableSource"/> (fixed-width and UTF-8 group keys).
/// </summary>
public sealed class HashAggregatePhysicalPlan : IPhysicalPlan
{
    public HashAggregatePhysicalPlan(
        TableId tableId,
        int[] groupKeyColumnIndices,
        AggregateSpec[] aggregates,
        HashAggregateOutputSlot[]? outputColumns = null,
        ColumnCompareFilter[]? filters = null,
        VectorizedScanExecutionOptions options = default,
        int spillPartialEntryThreshold = 0)
    {
        ArgumentNullException.ThrowIfNull(groupKeyColumnIndices);
        ArgumentNullException.ThrowIfNull(aggregates);
        if (groupKeyColumnIndices.Length == 0)
            throw new ArgumentException("At least one group key column is required.", nameof(groupKeyColumnIndices));
        if (aggregates.Length == 0)
            throw new ArgumentException("At least one aggregate is required.", nameof(aggregates));

        TableId = tableId;
        GroupKeyColumnIndices = (int[])groupKeyColumnIndices.Clone();
        Aggregates = (AggregateSpec[])aggregates.Clone();
        OutputColumns = outputColumns is { Length: > 0 }
            ? (HashAggregateOutputSlot[])outputColumns.Clone()
            : CreateDefaultOutputLayout(groupKeyColumnIndices.Length, aggregates.Length);
        ValidateOutputLayout(GroupKeyColumnIndices.Length, Aggregates.Length, OutputColumns);
        Filters = filters is { Length: > 0 } ? (ColumnCompareFilter[])filters.Clone() : null;
        Options = options;
        SpillPartialEntryThreshold = spillPartialEntryThreshold;
    }

    public TableId TableId { get; }

    public int[] GroupKeyColumnIndices { get; }

    public AggregateSpec[] Aggregates { get; }

    public HashAggregateOutputSlot[] OutputColumns { get; }

    /// <summary>AND conjunction of predicates on input rows.</summary>
    public ColumnCompareFilter[]? Filters { get; }

    public VectorizedScanExecutionOptions Options { get; }

    /// <summary>
    /// When positive and <see cref="IExecutionContext.SpillWriter"/> is enabled, partial maps with at least this many
    /// groups invoke <see cref="ISpillWriter.SpillChunkAsync"/> with a UTF-8 metrics payload (operator still completes in-memory).
    /// </summary>
    public int SpillPartialEntryThreshold { get; }

    public string Explain(string indent = "")
    {
        var f = Filters is { Length: > 0 } fl
            ? $" FILTER[{string.Join(" AND ", Array.ConvertAll(fl, x => $"col{x.ColumnIndex}{x.Op}"))}]"
            : "";
        var ag = string.Join(",", Array.ConvertAll(Aggregates, static a =>
            a.Kind == AggregateKind.Count && a.SourceColumnIndex < 0
                ? "Count(*)"
                : $"{a.Kind}({a.SourceColumnIndex})"));
        return $"{indent}HashAggregate(table={TableId}) KEYS[{string.Join(",", GroupKeyColumnIndices)}] AGGS[{ag}]{f}";
    }

    private static HashAggregateOutputSlot[] CreateDefaultOutputLayout(int keyCount, int aggCount)
    {
        var slots = new HashAggregateOutputSlot[keyCount + aggCount];
        for (var i = 0; i < keyCount; i++)
            slots[i] = new HashAggregateOutputSlot(HashAggregateOutputColumnKind.GroupKey, i);
        for (var j = 0; j < aggCount; j++)
            slots[keyCount + j] = new HashAggregateOutputSlot(HashAggregateOutputColumnKind.Aggregate, j);
        return slots;
    }

    private static void ValidateOutputLayout(int keyCount, int aggCount, HashAggregateOutputSlot[] slots)
    {
        if (slots.Length == 0)
            throw new ArgumentException("Output column layout must not be empty.", nameof(slots));
        foreach (var s in slots)
        {
            switch (s.Kind)
            {
                case HashAggregateOutputColumnKind.GroupKey when (uint)s.Ordinal >= (uint)keyCount:
                    throw new ArgumentException($"Group key output ordinal {s.Ordinal} is out of range.");
                case HashAggregateOutputColumnKind.Aggregate when (uint)s.Ordinal >= (uint)aggCount:
                    throw new ArgumentException($"Aggregate output ordinal {s.Ordinal} is out of range.");
            }
        }
    }
}
