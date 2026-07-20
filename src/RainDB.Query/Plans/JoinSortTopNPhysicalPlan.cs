using System;
using RainDB.Execution;

namespace RainDB.Query.Plans;

/// <summary>Inner join followed by optional sort and <see cref="Limit"/> on the join output rowset (in-memory).</summary>
public sealed class JoinSortTopNPhysicalPlan : IPhysicalPlan
{
    public JoinSortTopNPhysicalPlan(
        JoinPhysicalPlan join,
        SortKeyPhysicalSpec[] sortKeys,
        int? limit,
        VectorizedScanExecutionOptions options = default)
    {
        ArgumentNullException.ThrowIfNull(join);
        ArgumentNullException.ThrowIfNull(sortKeys);
        Join = join;
        SortKeys = (SortKeyPhysicalSpec[])sortKeys.Clone();
        Limit = limit;
        Options = options;
        if (limit is < 1)
            throw new ArgumentOutOfRangeException(nameof(limit), "LIMIT must be a positive integer.");
    }

    public JoinPhysicalPlan Join { get; }

    public SortKeyPhysicalSpec[] SortKeys { get; }

    public int? Limit { get; }

    public VectorizedScanExecutionOptions Options { get; }

    public string Explain(string indent = "") =>
        $"{indent}JoinSortTopN{(Limit is { } n ? $" LIMIT({n})" : "")}\n{indent}  {Join.Explain()}\n{indent}  SORT_KEYS[{string.Join(",", Array.ConvertAll(SortKeys, x => $"{x.ColumnIndex}{(x.Descending ? "D" : "A")}"))}]";
}
