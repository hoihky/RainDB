using RainDB.Catalog;
using RainDB.Execution;

namespace RainDB.Query.Plans;

/// <summary>
/// Physical scan over <see cref="IColumnarTableSource"/> with optional scalar filter, projection, aggregate, and morsel scheduling.
/// </summary>
public sealed class VectorizedScanPhysicalPlan : IPhysicalPlan
{
    public VectorizedScanPhysicalPlan(
        TableId tableId,
        int[] outputColumnIndices,
        ColumnCompareFilter[]? filters = null,
        AggregateSpec? aggregate = null,
        VectorizedScanExecutionOptions options = default)
    {
        ArgumentNullException.ThrowIfNull(outputColumnIndices);
        TableId = tableId;
        OutputColumnIndices = (int[])outputColumnIndices.Clone();
        Filters = filters is { Length: > 0 } ? (ColumnCompareFilter[])filters.Clone() : null;
        Aggregate = aggregate;
        Options = options;
    }

    public TableId TableId { get; }

    /// <summary>Indices into the table schema / batch column list (deduplicated projection).</summary>
    public int[] OutputColumnIndices { get; }

    /// <summary>AND conjunction of predicates (same row must satisfy all).</summary>
    public ColumnCompareFilter[]? Filters { get; }

    public AggregateSpec? Aggregate { get; }

    public VectorizedScanExecutionOptions Options { get; }

    public string Explain(string indent = "")
    {
        var agg = Aggregate is { } a ? $" AGG({a.SourceColumnIndex},{a.Kind})" : "";
        var f = Filters is { Length: > 0 } fl
            ? $" FILTER[{string.Join(" AND ", Array.ConvertAll(fl, x => $"col{x.ColumnIndex}{x.Op}"))}]"
            : "";
        return $"{indent}VectorizedScan(table={TableId}) PROJECT[{string.Join(",", OutputColumnIndices)}]{f}{agg}";
    }
}

/// <summary>
/// Compares a column to an immediate: fixed-width uses <see cref="ImmediateBits"/>; UTF-8 uses <see cref="Utf8LiteralBytes"/> with only Eq/Ne.
/// </summary>
public readonly record struct ColumnCompareFilter(
    int ColumnIndex,
    ScalarCompareOp Op,
    long ImmediateBits,
    byte[]? Utf8LiteralBytes = null);

/// <summary>Single-column aggregate over filtered rows (P1: Float64 sum/min/max; Int32/Int64 sum uses Int64Value).</summary>
public readonly record struct AggregateSpec(int SourceColumnIndex, AggregateKind Kind);

public readonly record struct VectorizedScanExecutionOptions
{
    /// <summary>-1 means <see cref="Environment.ProcessorCount"/>; 1 forces serial per-batch processing.</summary>
    public int MaxDegreeOfParallelism { get; init; }

    /// <summary>When true, batch indices are scheduled through a <see cref="System.Threading.Channels.Channel{T}"/> worker pool (deterministic merge by batch order).</summary>
    public bool UseChannelScheduler { get; init; }

    /// <summary>Use AVX2 horizontal reduction for <see cref="RainDB.Schema.RainDbType.Float64"/> <see cref="AggregateKind.Sum"/> when available.</summary>
    public bool UseAvx2DoubleSum { get; init; }
}
