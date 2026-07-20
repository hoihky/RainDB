using RainDB.Catalog;
using RainDB.Execution;
using RainDB.Schema;

namespace RainDB.Query.Plans;

/// <summary>Physical join implementation variant (same logical inner equi-join).</summary>
public enum PhysicalJoinAlgorithm
{
    Hash,
    SortMerge,
}

/// <summary>One selected output column from probe (left) or build (right) schema.</summary>
public readonly record struct JoinOutputColumnRef(bool IsProbe, int ColumnIndex);

/// <summary>
/// Inner equi-join: probe rows (left table) against a hash/sorted index on the build side (right table).
/// </summary>
public sealed class JoinPhysicalPlan : IPhysicalPlan
{
    public JoinPhysicalPlan(
        PhysicalJoinAlgorithm algorithm,
        TableId probeTableId,
        TableId buildTableId,
        int[] probeKeyColumnIndices,
        int[] buildKeyColumnIndices,
        TableSchema outputSchema,
        JoinOutputColumnRef[]? outputColumnOrder = null,
        ColumnCompareFilter[]? probeSideFilters = null,
        ColumnCompareFilter[]? buildSideFilters = null)
    {
        ArgumentNullException.ThrowIfNull(probeKeyColumnIndices);
        ArgumentNullException.ThrowIfNull(buildKeyColumnIndices);
        ArgumentNullException.ThrowIfNull(outputSchema);
        if (probeKeyColumnIndices.Length != buildKeyColumnIndices.Length || probeKeyColumnIndices.Length == 0)
            throw new ArgumentException("Join requires at least one key column and matching key arity.");
        Algorithm = algorithm;
        ProbeTableId = probeTableId;
        BuildTableId = buildTableId;
        ProbeKeyColumnIndices = (int[])probeKeyColumnIndices.Clone();
        BuildKeyColumnIndices = (int[])buildKeyColumnIndices.Clone();
        OutputSchema = outputSchema;
        OutputColumnOrder = outputColumnOrder is { Length: > 0 } ? (JoinOutputColumnRef[])outputColumnOrder.Clone() : null;
        ProbeSideFilters = probeSideFilters is { Length: > 0 } ? (ColumnCompareFilter[])probeSideFilters.Clone() : null;
        BuildSideFilters = buildSideFilters is { Length: > 0 } ? (ColumnCompareFilter[])buildSideFilters.Clone() : null;
    }

    public PhysicalJoinAlgorithm Algorithm { get; }

    /// <summary>Left / probe table.</summary>
    public TableId ProbeTableId { get; }

    /// <summary>Right / build table.</summary>
    public TableId BuildTableId { get; }

    public int[] ProbeKeyColumnIndices { get; }

    public int[] BuildKeyColumnIndices { get; }

    /// <summary>Final column layout (names/types match <see cref="OutputSchema"/>).</summary>
    public TableSchema OutputSchema { get; }

    /// <summary>When non-null, materialize these columns in order; otherwise all probe columns then all build columns.</summary>
    public JoinOutputColumnRef[]? OutputColumnOrder { get; }

    /// <summary>AND predicates on probe rows.</summary>
    public ColumnCompareFilter[]? ProbeSideFilters { get; }

    /// <summary>AND predicates on build rows.</summary>
    public ColumnCompareFilter[]? BuildSideFilters { get; }

    public string Explain(string indent = "")
    {
        var algo = Algorithm == PhysicalJoinAlgorithm.Hash ? "HashJoin" : "SortMergeJoin";
        var proj = OutputColumnOrder is { Length: > 0 } o ? $" OUT[{o.Length}]" : "";
        var pf = ProbeSideFilters is { Length: > 0 } pl
            ? $" PROBE_FILTER[{string.Join(" AND ", Array.ConvertAll(pl, x => $"col{x.ColumnIndex}{x.Op}"))}]"
            : "";
        var bf = BuildSideFilters is { Length: > 0 } bl
            ? $" BUILD_FILTER[{string.Join(" AND ", Array.ConvertAll(bl, x => $"col{x.ColumnIndex}{x.Op}"))}]"
            : "";
        return $"{indent}{algo}(probe={ProbeTableId}, build={BuildTableId}) KEYS[{string.Join(",", ProbeKeyColumnIndices)}]=[{string.Join(",", BuildKeyColumnIndices)}]{proj}{pf}{bf}";
    }
}
