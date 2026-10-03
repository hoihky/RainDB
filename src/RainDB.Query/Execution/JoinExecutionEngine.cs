using RainDB.Catalog;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Execution;
using RainDB.Query.Execution.Joining;
using RainDB.Query.Execution.Operators;
using RainDB.Query.Plans;
using RainDB.Query.Results;
using RainDB.Query.Runtime;
using RainDB.Query.Vectorized;
using RainDB.Logical;
using RainDB.Schema;

namespace RainDB.Query.Execution;

/// <summary>Phase 2 inner equi-join: hash build on the right, probe from the left; or sort-merge on join keys.</summary>
public sealed class JoinOperator : Operators.IJoinOperator
{
    private readonly QueryOperatorDependencies _deps;

    public JoinOperator()
        : this(new QueryOperatorDependencies())
    {
    }

    internal JoinOperator(QueryOperatorDependencies dependencies) =>
        _deps = dependencies ?? throw new ArgumentNullException(nameof(dependencies));

    private ResolvedJoinSubqueryFilters _resolvedJoinSubqueries = ResolvedJoinSubqueryFilters.Empty;

    private readonly record struct RowRef(int BatchIdx, int RowIdx);

    private sealed class SortEntryFixed
    {
        public required GroupKey Key { get; init; }

        public int BatchIdx { get; init; }

        public int RowIdx { get; init; }
    }

    /// <summary>Probe-driven join: emits output in fixed-size chunks (no full <c>List&lt;match&gt;</c>).</summary>
    public void ExecuteStreaming(
        JoinPhysicalPlan plan,
        IColumnarTableSource probeTable,
        IColumnarTableSource buildTable,
        IExecutionContext context,
        Action<ColumnarBatch> emitBatch,
        int matchChunkRowCount = JoinMatchChunkEmitter.DefaultChunkRowCount)
    {
        ExecuteStreaming(plan, probeTable, buildTable, context, emitBatch, matchChunkRowCount, ResolvedJoinSubqueryFilters.Empty);
    }

    internal void ExecuteStreaming(
        JoinPhysicalPlan plan,
        IColumnarTableSource probeTable,
        IColumnarTableSource buildTable,
        IExecutionContext context,
        Action<ColumnarBatch> emitBatch,
        int matchChunkRowCount,
        ResolvedJoinSubqueryFilters subqueryFilters)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(probeTable);
        ArgumentNullException.ThrowIfNull(buildTable);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(emitBatch);
        Validate(plan, probeTable, buildTable);

        if (subqueryFilters.IsDenyAll)
            return;

        _resolvedJoinSubqueries = subqueryFilters;
        try
        {
            RunJoinCore(plan, probeTable, buildTable, context, emitBatch, matchChunkRowCount);
        }
        finally
        {
            _resolvedJoinSubqueries = ResolvedJoinSubqueryFilters.Empty;
        }
    }

    private void RunJoinCore(
        JoinPhysicalPlan plan,
        IColumnarTableSource probeTable,
        IColumnarTableSource buildTable,
        IExecutionContext context,
        Action<ColumnarBatch> emitBatch,
        int matchChunkRowCount)
    {
        var probeSchema = probeTable.Schema;
        var buildSchema = buildTable.Schema;
        var probeBatches = probeTable.Batches;
        var buildBatches = buildTable.Batches;
        var ct = context.CancellationToken;

        var emitter = new JoinMatchChunkEmitter(
            plan,
            probeBatches,
            buildBatches,
            probeSchema,
            buildSchema,
            emitBatch,
            _deps.JoinMaterializer,
            matchChunkRowCount);

        var utf8JoinKeys = JoinKeysIncludeUtf8(probeSchema, plan.ProbeKeyColumnIndices);
        switch (plan.Algorithm)
        {
            case PhysicalJoinAlgorithm.Hash:
                if (utf8JoinKeys)
                    RunHashJoinUtf8(plan, probeBatches, buildBatches, probeSchema, buildSchema, ct, emitter);
                else
                    RunHashJoinFixed(plan, probeBatches, buildBatches, ct, emitter);
                break;
            case PhysicalJoinAlgorithm.SortMerge:
                if (utf8JoinKeys)
                    RunSortMergeJoinUtf8(plan, probeBatches, buildBatches, probeSchema, buildSchema, ct, emitter);
                else
                    RunSortMergeJoinFixed(plan, probeBatches, buildBatches, probeSchema, ct, emitter);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(plan));
        }

        emitter.Flush();
    }

    public async ValueTask<IQueryResult> ExecuteAsync(
        JoinPhysicalPlan plan,
        IColumnarTableSource probeTable,
        IColumnarTableSource buildTable,
        IExecutionContext context)
    {
        var subFilters = await ResolveJoinSubqueryFiltersAsync(plan, context).ConfigureAwait(false);
        if (subFilters.IsDenyAll)
            return new ColumnarMaterializedQueryResult([_deps.JoinMaterializer.EmptyBatch(plan)]);

        var batches = new List<ColumnarBatch>();
        ExecuteStreaming(plan, probeTable, buildTable, context, batches.Add, JoinMatchChunkEmitter.DefaultChunkRowCount, subFilters);
        if (batches.Count == 0)
            batches.Add(_deps.JoinMaterializer.EmptyBatch(plan));

        IQueryResult r = new ColumnarMaterializedQueryResult(batches);
        return r;
    }

    private static async ValueTask<ResolvedJoinSubqueryFilters> ResolveJoinSubqueryFiltersAsync(
        JoinPhysicalPlan plan,
        IExecutionContext context)
    {
        if (plan.ProbeInSubqueries is null && plan.BuildInSubqueries is null && plan.ExistsSubqueries is null)
            return ResolvedJoinSubqueryFilters.Empty;

        if (context is not RainDbExecutionContext rc || rc.NestedExecutor is not { } executor)
            throw new InvalidOperationException("Join subquery predicates require NestedExecutor on the execution context.");

        return await SubqueryFilterResolver.ResolveJoinAsync(
            plan.ProbeInSubqueries,
            plan.BuildInSubqueries,
            plan.ExistsSubqueries,
            executor,
            context).ConfigureAwait(false);
    }

    private static void Validate(JoinPhysicalPlan plan, IColumnarTableSource probe, IColumnarTableSource build)
    {
        if (plan.ProbeTableId != probe.Id || plan.BuildTableId != build.Id)
            throw new ArgumentException("Physical join table ids do not match supplied tables.", nameof(plan));

        ValidateIndices(probe.Schema, plan.ProbeKeyColumnIndices);
        ValidateIndices(build.Schema, plan.BuildKeyColumnIndices);

        if (plan.ProbeSideFilters is { } pf)
        {
            foreach (var f in pf)
            {
                if ((uint)f.ColumnIndex >= (uint)probe.Schema.Columns.Count)
                    throw new ArgumentException("Probe-side filter column index is out of range.", nameof(plan));
            }
        }

        if (plan.BuildSideFilters is { } bf)
        {
            foreach (var f in bf)
            {
                if ((uint)f.ColumnIndex >= (uint)build.Schema.Columns.Count)
                    throw new ArgumentException("Build-side filter column index is out of range.", nameof(plan));
            }
        }

        if (plan.OutputSchema.Columns.Count == 0)
            throw new ArgumentException("Join output schema is empty.", nameof(plan));

        if (plan.OutputColumnOrder is { } oc && oc.Length != plan.OutputSchema.Columns.Count)
            throw new ArgumentException("Output column order length must match output schema.", nameof(plan));

        for (var i = 0; i < plan.ProbeKeyColumnIndices.Length; i++)
        {
            var pt = probe.Schema.Columns[plan.ProbeKeyColumnIndices[i]].Type;
            var bt = build.Schema.Columns[plan.BuildKeyColumnIndices[i]].Type;
            if (pt != bt)
                throw new ArgumentException($"Join key part {i} type mismatch {pt} vs {bt}.", nameof(plan));
            if (pt != RainDbType.Utf8 && !ColumnTypeSizes.IsFixedWidth(pt))
                throw new ArgumentException($"Join key column type {pt} is not supported for equi-join.", nameof(plan));
        }
    }

    private static bool JoinKeysIncludeUtf8(TableSchema schema, int[] keyIndices)
    {
        foreach (var ix in keyIndices)
        {
            if (schema.Columns[ix].Type == RainDbType.Utf8)
                return true;
        }

        return false;
    }

    private static bool PreserveUnmatchedProbe(LogicalJoinSemantics semantics) =>
        semantics is LogicalJoinSemantics.LeftOuter or LogicalJoinSemantics.FullOuter;

    private static bool PreserveUnmatchedBuild(LogicalJoinSemantics semantics) =>
        semantics is LogicalJoinSemantics.FullOuter;

    private static void ValidateIndices(TableSchema schema, int[] ix)
    {
        foreach (var i in ix)
        {
            if ((uint)i >= (uint)schema.Columns.Count)
                throw new ArgumentException($"Column index {i} out of range for schema.", nameof(ix));
        }
    }

    private bool RowPassesProbe(IColumnarBatch batch, ColumnCompareFilter[]? filters, int row) =>
        RowPassesSide(batch, filters, _resolvedJoinSubqueries.ProbeInFilters, row);

    private bool RowPassesBuild(IColumnarBatch batch, ColumnCompareFilter[]? filters, int row) =>
        RowPassesSide(batch, filters, _resolvedJoinSubqueries.BuildInFilters, row);

    private bool RowPassesSide(
        IColumnarBatch batch,
        ColumnCompareFilter[]? compareFilters,
        ColumnInSetFilter[]? inFilters,
        int row)
    {
        if (compareFilters is { Length: > 0 })
        {
            foreach (var f in compareFilters)
            {
                if (!_deps.Selection.RowMatchesFilter(batch.Columns[f.ColumnIndex], f, row))
                    return false;
            }
        }

        if (inFilters is { Length: > 0 })
        {
            if (!_deps.Selection.RowMatchesInSetFilters(batch, row, inFilters))
                return false;
        }

        return true;
    }

    private void RunHashJoinFixed(
        JoinPhysicalPlan plan,
        IReadOnlyList<IColumnarBatch> probeBatches,
        IReadOnlyList<IColumnarBatch> buildBatches,
        CancellationToken ct,
        JoinMatchChunkEmitter emitter)
    {
        var (dict, buildRows) = BuildHashIndexFixed(plan, buildBatches, ct);
        HashSet<RowRef>? matchedBuild = PreserveUnmatchedBuild(plan.Semantics) ? [] : null;
        ProbeHashJoinFixed(plan, probeBatches, dict, matchedBuild, ct, emitter);
        if (matchedBuild is not null)
            EmitUnmatchedBuildRows(plan, buildRows, matchedBuild, emitter);
    }

    private (Dictionary<GroupKey, List<RowRef>> Dict, List<RowRef> AllIndexedRows) BuildHashIndexFixed(
        JoinPhysicalPlan plan,
        IReadOnlyList<IColumnarBatch> buildBatches,
        CancellationToken ct)
    {
        var dict = new Dictionary<GroupKey, List<RowRef>>();
        var allIndexed = new List<RowRef>();
        var scratch = new ulong[plan.BuildKeyColumnIndices.Length];
        for (var bi = 0; bi < buildBatches.Count; bi++)
        {
            ct.ThrowIfCancellationRequested();
            var batch = buildBatches[bi];
            for (var row = 0; row < batch.RowCount; row++)
            {
                if (!RowPassesBuild(batch, plan.BuildSideFilters, row))
                    continue;
                var key = _deps.GroupKeys.BuildKey(batch, row, plan.BuildKeyColumnIndices, scratch);
                if (key.NullMask != 0)
                    continue;
                if (!dict.TryGetValue(key, out var list))
                {
                    list = [];
                    dict[key] = list;
                }

                var rr = new RowRef(bi, row);
                list.Add(rr);
                allIndexed.Add(rr);
            }
        }

        return (dict, allIndexed);
    }

    private void ProbeHashJoinFixed(
        JoinPhysicalPlan plan,
        IReadOnlyList<IColumnarBatch> probeBatches,
        Dictionary<GroupKey, List<RowRef>> dict,
        HashSet<RowRef>? matchedBuild,
        CancellationToken ct,
        JoinMatchChunkEmitter emitter)
    {
        var scratch = new ulong[plan.ProbeKeyColumnIndices.Length];
        for (var bi = 0; bi < probeBatches.Count; bi++)
        {
            ct.ThrowIfCancellationRequested();
            var batch = probeBatches[bi];
            for (var row = 0; row < batch.RowCount; row++)
            {
                if (!RowPassesProbe(batch, plan.ProbeSideFilters, row))
                    continue;
                var key = _deps.GroupKeys.BuildKey(batch, row, plan.ProbeKeyColumnIndices, scratch);
                EmitHashProbeMatchesFixed(plan, emitter, bi, row, key.NullMask != 0, dict, key, matchedBuild);
            }
        }
    }

    private static void EmitUnmatchedBuildRows(
        JoinPhysicalPlan plan,
        List<RowRef> indexedBuildRows,
        HashSet<RowRef> matchedBuild,
        JoinMatchChunkEmitter emitter)
    {
        foreach (var br in indexedBuildRows)
        {
            if (!matchedBuild.Contains(br))
                emitter.Add(JoinRowMatch.BuildOnly(br.BatchIdx, br.RowIdx));
        }
    }

    private void RunHashJoinUtf8(
        JoinPhysicalPlan plan,
        IReadOnlyList<IColumnarBatch> probeBatches,
        IReadOnlyList<IColumnarBatch> buildBatches,
        TableSchema probeSchema,
        TableSchema buildSchema,
        CancellationToken ct,
        JoinMatchChunkEmitter emitter)
    {
        var (dict, buildRows) = BuildHashIndexUtf8(plan, buildBatches, buildSchema, ct);
        HashSet<RowRef>? matchedBuild = PreserveUnmatchedBuild(plan.Semantics) ? [] : null;
        ProbeHashJoinUtf8(plan, probeBatches, probeSchema, dict, matchedBuild, ct, emitter);
        if (matchedBuild is not null)
            EmitUnmatchedBuildRows(plan, buildRows, matchedBuild, emitter);
    }

    private (Dictionary<CompositeJoinKey, List<RowRef>> Dict, List<RowRef> AllIndexedRows) BuildHashIndexUtf8(
        JoinPhysicalPlan plan,
        IReadOnlyList<IColumnarBatch> buildBatches,
        TableSchema buildSchema,
        CancellationToken ct)
    {
        var dict = new Dictionary<CompositeJoinKey, List<RowRef>>();
        var allIndexed = new List<RowRef>();
        for (var bi = 0; bi < buildBatches.Count; bi++)
        {
            ct.ThrowIfCancellationRequested();
            var batch = buildBatches[bi];
            for (var row = 0; row < batch.RowCount; row++)
            {
                if (!RowPassesBuild(batch, plan.BuildSideFilters, row))
                    continue;
                var key = _deps.CompositeJoinKeys.Build(buildSchema, batch, row, plan.BuildKeyColumnIndices);
                if (key.NullMask != 0)
                    continue;
                if (!dict.TryGetValue(key, out var list))
                {
                    list = [];
                    dict[key] = list;
                }

                var rr = new RowRef(bi, row);
                list.Add(rr);
                allIndexed.Add(rr);
            }
        }

        return (dict, allIndexed);
    }

    private void ProbeHashJoinUtf8(
        JoinPhysicalPlan plan,
        IReadOnlyList<IColumnarBatch> probeBatches,
        TableSchema probeSchema,
        Dictionary<CompositeJoinKey, List<RowRef>> dict,
        HashSet<RowRef>? matchedBuild,
        CancellationToken ct,
        JoinMatchChunkEmitter emitter)
    {
        for (var bi = 0; bi < probeBatches.Count; bi++)
        {
            ct.ThrowIfCancellationRequested();
            var batch = probeBatches[bi];
            for (var row = 0; row < batch.RowCount; row++)
            {
                if (!RowPassesProbe(batch, plan.ProbeSideFilters, row))
                    continue;
                var key = _deps.CompositeJoinKeys.Build(probeSchema, batch, row, plan.ProbeKeyColumnIndices);
                EmitHashProbeMatchesUtf8(plan, emitter, bi, row, key.NullMask != 0, dict, key, matchedBuild);
            }
        }
    }

    private static void EmitHashProbeMatchesFixed(
        JoinPhysicalPlan plan,
        JoinMatchChunkEmitter emitter,
        int probeBatchIdx,
        int probeRow,
        bool probeKeyIsNull,
        Dictionary<GroupKey, List<RowRef>> dict,
        GroupKey key,
        HashSet<RowRef>? matchedBuild)
    {
        if (probeKeyIsNull)
        {
            if (PreserveUnmatchedProbe(plan.Semantics))
                emitter.Add(JoinRowMatch.ProbeOnly(probeBatchIdx, probeRow));
            return;
        }

        if (!dict.TryGetValue(key, out var list) || list.Count == 0)
        {
            if (PreserveUnmatchedProbe(plan.Semantics))
                emitter.Add(JoinRowMatch.ProbeOnly(probeBatchIdx, probeRow));
            return;
        }

        foreach (var br in list)
        {
            matchedBuild?.Add(br);
            emitter.Add(new JoinRowMatch(probeBatchIdx, probeRow, br.BatchIdx, br.RowIdx));
        }
    }

    private static void EmitHashProbeMatchesUtf8(
        JoinPhysicalPlan plan,
        JoinMatchChunkEmitter emitter,
        int probeBatchIdx,
        int probeRow,
        bool probeKeyIsNull,
        Dictionary<CompositeJoinKey, List<RowRef>> dict,
        CompositeJoinKey key,
        HashSet<RowRef>? matchedBuild)
    {
        if (probeKeyIsNull)
        {
            if (PreserveUnmatchedProbe(plan.Semantics))
                emitter.Add(JoinRowMatch.ProbeOnly(probeBatchIdx, probeRow));
            return;
        }

        if (!dict.TryGetValue(key, out var list) || list.Count == 0)
        {
            if (PreserveUnmatchedProbe(plan.Semantics))
                emitter.Add(JoinRowMatch.ProbeOnly(probeBatchIdx, probeRow));
            return;
        }

        foreach (var br in list)
        {
            matchedBuild?.Add(br);
            emitter.Add(new JoinRowMatch(probeBatchIdx, probeRow, br.BatchIdx, br.RowIdx));
        }
    }

    private void RunSortMergeJoinFixed(
        JoinPhysicalPlan plan,
        IReadOnlyList<IColumnarBatch> probeBatches,
        IReadOnlyList<IColumnarBatch> buildBatches,
        TableSchema probeSchema,
        CancellationToken ct,
        JoinMatchChunkEmitter emitter)
    {
        var comparer = new GroupKeyComparer(probeSchema, plan.ProbeKeyColumnIndices);
        var left = FlattenNonNullFixedKeys(probeBatches, plan.ProbeKeyColumnIndices, plan.ProbeSideFilters, isProbeSide: true, ct);
        var right = FlattenNonNullFixedKeys(buildBatches, plan.BuildKeyColumnIndices, plan.BuildSideFilters, isProbeSide: false, ct);

        left.Sort((a, b) => comparer.Compare(a.Key, b.Key));
        right.Sort((a, b) => comparer.Compare(a.Key, b.Key));

        MergeSortedKeyRuns(plan, left, right, comparer, ct, emitter);
    }

    private void RunSortMergeJoinUtf8(
        JoinPhysicalPlan plan,
        IReadOnlyList<IColumnarBatch> probeBatches,
        IReadOnlyList<IColumnarBatch> buildBatches,
        TableSchema probeSchema,
        TableSchema buildSchema,
        CancellationToken ct,
        JoinMatchChunkEmitter emitter)
    {
        var comparer = new CompositeJoinKeyComparer(probeSchema, plan.ProbeKeyColumnIndices);
        var left = FlattenNonNullCompositeKeys(probeBatches, plan.ProbeKeyColumnIndices, plan.ProbeSideFilters, probeSchema, isProbeSide: true, ct);
        var right = FlattenNonNullCompositeKeys(buildBatches, plan.BuildKeyColumnIndices, plan.BuildSideFilters, buildSchema, isProbeSide: false, ct);

        left.Sort((a, b) => comparer.Compare(a.Key, b.Key));
        right.Sort((a, b) => comparer.Compare(a.Key, b.Key));

        MergeSortedCompositeRuns(plan, left, right, comparer, ct, emitter);
    }

    private void MergeSortedKeyRuns(
        JoinPhysicalPlan plan,
        List<SortEntryFixed> left,
        List<SortEntryFixed> right,
        GroupKeyComparer comparer,
        CancellationToken ct,
        JoinMatchChunkEmitter emitter)
    {
        var i = 0;
        var j = 0;
        while (i < left.Count && j < right.Count)
        {
            ct.ThrowIfCancellationRequested();
            var c = comparer.Compare(left[i].Key, right[j].Key);
            if (c < 0)
            {
                if (PreserveUnmatchedProbe(plan.Semantics))
                    emitter.Add(JoinRowMatch.ProbeOnly(left[i].BatchIdx, left[i].RowIdx));
                i++;
                continue;
            }

            if (c > 0)
            {
                if (PreserveUnmatchedBuild(plan.Semantics))
                    emitter.Add(JoinRowMatch.BuildOnly(right[j].BatchIdx, right[j].RowIdx));
                j++;
                continue;
            }

            var iStart = i;
            while (i < left.Count && comparer.Compare(left[i].Key, left[iStart].Key) == 0)
                i++;
            var jStart = j;
            while (j < right.Count && comparer.Compare(right[j].Key, right[jStart].Key) == 0)
                j++;

            for (var ii = iStart; ii < i; ii++)
            {
                for (var jj = jStart; jj < j; jj++)
                {
                    emitter.Add(new JoinRowMatch(
                        left[ii].BatchIdx,
                        left[ii].RowIdx,
                        right[jj].BatchIdx,
                        right[jj].RowIdx));
                }
            }
        }

        if (PreserveUnmatchedProbe(plan.Semantics))
        {
            for (; i < left.Count; i++)
                emitter.Add(JoinRowMatch.ProbeOnly(left[i].BatchIdx, left[i].RowIdx));
        }

        if (PreserveUnmatchedBuild(plan.Semantics))
        {
            for (; j < right.Count; j++)
                emitter.Add(JoinRowMatch.BuildOnly(right[j].BatchIdx, right[j].RowIdx));
        }
    }

    private void MergeSortedCompositeRuns(
        JoinPhysicalPlan plan,
        List<SortEntryUtf8> left,
        List<SortEntryUtf8> right,
        CompositeJoinKeyComparer comparer,
        CancellationToken ct,
        JoinMatchChunkEmitter emitter)
    {
        var i = 0;
        var j = 0;
        while (i < left.Count && j < right.Count)
        {
            ct.ThrowIfCancellationRequested();
            var c = comparer.Compare(left[i].Key, right[j].Key);
            if (c < 0)
            {
                if (PreserveUnmatchedProbe(plan.Semantics))
                    emitter.Add(JoinRowMatch.ProbeOnly(left[i].BatchIdx, left[i].RowIdx));
                i++;
                continue;
            }

            if (c > 0)
            {
                if (PreserveUnmatchedBuild(plan.Semantics))
                    emitter.Add(JoinRowMatch.BuildOnly(right[j].BatchIdx, right[j].RowIdx));
                j++;
                continue;
            }

            var iStart = i;
            while (i < left.Count && comparer.Compare(left[i].Key, left[iStart].Key) == 0)
                i++;
            var jStart = j;
            while (j < right.Count && comparer.Compare(right[j].Key, right[jStart].Key) == 0)
                j++;

            for (var ii = iStart; ii < i; ii++)
            {
                for (var jj = jStart; jj < j; jj++)
                {
                    emitter.Add(new JoinRowMatch(
                        left[ii].BatchIdx,
                        left[ii].RowIdx,
                        right[jj].BatchIdx,
                        right[jj].RowIdx));
                }
            }
        }

        if (PreserveUnmatchedProbe(plan.Semantics))
        {
            for (; i < left.Count; i++)
                emitter.Add(JoinRowMatch.ProbeOnly(left[i].BatchIdx, left[i].RowIdx));
        }

        if (PreserveUnmatchedBuild(plan.Semantics))
        {
            for (; j < right.Count; j++)
                emitter.Add(JoinRowMatch.BuildOnly(right[j].BatchIdx, right[j].RowIdx));
        }
    }

    private sealed class SortEntryUtf8
    {
        public required CompositeJoinKey Key { get; init; }

        public int BatchIdx { get; init; }

        public int RowIdx { get; init; }
    }

    private List<SortEntryFixed> FlattenNonNullFixedKeys(
        IReadOnlyList<IColumnarBatch> batches,
        int[] keyIndices,
        ColumnCompareFilter[]? sideFilters,
        bool isProbeSide,
        CancellationToken ct)
    {
        var scratch = new ulong[keyIndices.Length];
        var list = new List<SortEntryFixed>();
        for (var bi = 0; bi < batches.Count; bi++)
        {
            ct.ThrowIfCancellationRequested();
            var batch = batches[bi];
            for (var row = 0; row < batch.RowCount; row++)
            {
                if (isProbeSide ? !RowPassesProbe(batch, sideFilters, row) : !RowPassesBuild(batch, sideFilters, row))
                    continue;
                var key = _deps.GroupKeys.BuildKey(batch, row, keyIndices, scratch);
                if (key.NullMask != 0)
                    continue;
                list.Add(new SortEntryFixed { Key = key, BatchIdx = bi, RowIdx = row });
            }
        }

        return list;
    }

    private List<SortEntryUtf8> FlattenNonNullCompositeKeys(
        IReadOnlyList<IColumnarBatch> batches,
        int[] keyIndices,
        ColumnCompareFilter[]? sideFilters,
        TableSchema schema,
        bool isProbeSide,
        CancellationToken ct)
    {
        var list = new List<SortEntryUtf8>();
        for (var bi = 0; bi < batches.Count; bi++)
        {
            ct.ThrowIfCancellationRequested();
            var batch = batches[bi];
            for (var row = 0; row < batch.RowCount; row++)
            {
                if (isProbeSide ? !RowPassesProbe(batch, sideFilters, row) : !RowPassesBuild(batch, sideFilters, row))
                    continue;
                var key = _deps.CompositeJoinKeys.Build(schema, batch, row, keyIndices);
                if (key.NullMask != 0)
                    continue;
                list.Add(new SortEntryUtf8 { Key = key, BatchIdx = bi, RowIdx = row });
            }
        }

        return list;
    }
}
