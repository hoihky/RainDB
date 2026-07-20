using RainDB.Catalog;
using RainDB.Execution;

namespace RainDB.Query.Plans;

/// <summary>Sort key: index into the row's column list (table schema for scans; join output schema for join+sort).</summary>
public readonly record struct SortKeyPhysicalSpec(int ColumnIndex, bool Descending);

/// <summary>
/// Single-table scan output: optional row filter, projection, then global sort (optional keys) and <see cref="Limit"/> rows.
/// When <see cref="SortKeys"/> is empty, rows keep deterministic batch/row order and only <see cref="Limit"/> is applied.
/// </summary>
public sealed class SortTopNPhysicalPlan : IPhysicalPlan
{
    public SortTopNPhysicalPlan(
        TableId tableId,
        int[] outputColumnIndices,
        ColumnCompareFilter[]? filters,
        SortKeyPhysicalSpec[] sortKeys,
        int? limit,
        VectorizedScanExecutionOptions options = default)
    {
        ArgumentNullException.ThrowIfNull(outputColumnIndices);
        ArgumentNullException.ThrowIfNull(sortKeys);
        TableId = tableId;
        OutputColumnIndices = (int[])outputColumnIndices.Clone();
        Filters = filters is { Length: > 0 } ? (ColumnCompareFilter[])filters.Clone() : null;
        SortKeys = (SortKeyPhysicalSpec[])sortKeys.Clone();
        Limit = limit;
        Options = options;
        if (limit is < 1)
            throw new ArgumentOutOfRangeException(nameof(limit), "LIMIT must be a positive integer.");
    }

    public TableId TableId { get; }

    public int[] OutputColumnIndices { get; }

    public ColumnCompareFilter[]? Filters { get; }

    public SortKeyPhysicalSpec[] SortKeys { get; }

    public int? Limit { get; }

    public VectorizedScanExecutionOptions Options { get; }

    public string Explain(string indent = "")
    {
        var sk = SortKeys.Length == 0
            ? "NONE"
            : string.Join(",", Array.ConvertAll(SortKeys, x => $"{x.ColumnIndex}{(x.Descending ? "D" : "A")}"));
        var f = Filters is { Length: > 0 } fl
            ? $" FILTER[{string.Join(" AND ", Array.ConvertAll(fl, x => $"col{x.ColumnIndex}{x.Op}"))}]"
            : "";
        var lim = Limit is { } n ? $" LIMIT({n})" : "";
        return $"{indent}SortTopN(table={TableId}) PROJECT[{string.Join(",", OutputColumnIndices)}] KEYS[{sk}]{f}{lim}";
    }
}
