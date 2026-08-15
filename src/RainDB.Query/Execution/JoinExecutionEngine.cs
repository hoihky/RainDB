using RainDB.Catalog;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Execution;
using RainDB.Query.Plans;
using RainDB.Query.Results;
using RainDB.Query.Vectorized;
using RainDB.Schema;

namespace RainDB.Query.Execution;

/// <summary>Phase 2 inner equi-join: hash build on the right, probe from the left; or sort-merge on join keys.</summary>
public static class JoinExecutionEngine
{
    private readonly record struct RowRef(int BatchIdx, int RowIdx);

    private readonly record struct RowRefMatch(int LeftBatchIdx, int LeftRow, int RightBatchIdx, int RightRow);

    private sealed class SortEntryFixed
    {
        public required GroupKey Key { get; init; }

        public int BatchIdx { get; init; }

        public int RowIdx { get; init; }
    }

    public static ValueTask<IQueryResult> ExecuteAsync(
        JoinPhysicalPlan plan,
        IColumnarTableSource probeTable,
        IColumnarTableSource buildTable,
        IExecutionContext context)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(probeTable);
        ArgumentNullException.ThrowIfNull(buildTable);
        ArgumentNullException.ThrowIfNull(context);
        Validate(plan, probeTable, buildTable);

        var probeSchema = probeTable.Schema;
        var buildSchema = buildTable.Schema;
        var probeBatches = probeTable.Batches;
        var buildBatches = buildTable.Batches;
        var ct = context.CancellationToken;

        var utf8JoinKeys = JoinKeysIncludeUtf8(probeSchema, plan.ProbeKeyColumnIndices);
        List<RowRefMatch> matches = plan.Algorithm switch
        {
            PhysicalJoinAlgorithm.Hash => utf8JoinKeys
                ? ComputeHashMatchesUtf8(plan, probeBatches, buildBatches, probeSchema, buildSchema, ct)
                : ComputeHashMatchesFixed(plan, probeBatches, buildBatches, ct),
            PhysicalJoinAlgorithm.SortMerge => utf8JoinKeys
                ? ComputeSortMergeMatchesUtf8(plan, probeBatches, buildBatches, probeSchema, buildSchema, ct)
                : ComputeSortMergeMatchesFixed(plan, probeBatches, buildBatches, probeSchema, ct),
            _ => throw new ArgumentOutOfRangeException(nameof(plan)),
        };

        var batch = MaterializeMatches(plan, probeBatches, buildBatches, probeSchema, buildSchema, matches);
        IQueryResult r = new ColumnarMaterializedQueryResult([batch]);
        return new ValueTask<IQueryResult>(r);
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

    private static void ValidateIndices(TableSchema schema, int[] ix)
    {
        foreach (var i in ix)
        {
            if ((uint)i >= (uint)schema.Columns.Count)
                throw new ArgumentException($"Column index {i} out of range for schema.", nameof(ix));
        }
    }

    private static bool RowPassesAll(IColumnarBatch batch, ColumnCompareFilter[]? filters, int row)
    {
        if (filters is null || filters.Length == 0)
            return true;
        foreach (var f in filters)
        {
            if (!SelectionEvaluator.RowMatchesFilter(batch.Columns[f.ColumnIndex], f, row))
                return false;
        }

        return true;
    }

    private static List<RowRefMatch> ComputeHashMatchesFixed(
        JoinPhysicalPlan plan,
        IReadOnlyList<IColumnarBatch> probeBatches,
        IReadOnlyList<IColumnarBatch> buildBatches,
        CancellationToken ct)
    {
        var dict = new Dictionary<GroupKey, List<RowRef>>();
        var scratch = new ulong[plan.BuildKeyColumnIndices.Length];
        for (var bi = 0; bi < buildBatches.Count; bi++)
        {
            ct.ThrowIfCancellationRequested();
            var batch = buildBatches[bi];
            for (var row = 0; row < batch.RowCount; row++)
            {
                if (!RowPassesAll(batch, plan.BuildSideFilters, row))
                    continue;
                var key = FixedWidthGroupKeyBuilder.BuildKey(batch, row, plan.BuildKeyColumnIndices, scratch);
                if (key.NullMask != 0)
                    continue;
                if (!dict.TryGetValue(key, out var list))
                {
                    list = [];
                    dict[key] = list;
                }

                list.Add(new RowRef(bi, row));
            }
        }

        var matches = new List<RowRefMatch>();
        scratch = new ulong[plan.ProbeKeyColumnIndices.Length];
        for (var bi = 0; bi < probeBatches.Count; bi++)
        {
            ct.ThrowIfCancellationRequested();
            var batch = probeBatches[bi];
            for (var row = 0; row < batch.RowCount; row++)
            {
                if (!RowPassesAll(batch, plan.ProbeSideFilters, row))
                    continue;
                var key = FixedWidthGroupKeyBuilder.BuildKey(batch, row, plan.ProbeKeyColumnIndices, scratch);
                if (key.NullMask != 0)
                    continue;
                if (!dict.TryGetValue(key, out var list))
                    continue;
                foreach (var br in list)
                    matches.Add(new RowRefMatch(bi, row, br.BatchIdx, br.RowIdx));
            }
        }

        return matches;
    }

    private static List<RowRefMatch> ComputeHashMatchesUtf8(
        JoinPhysicalPlan plan,
        IReadOnlyList<IColumnarBatch> probeBatches,
        IReadOnlyList<IColumnarBatch> buildBatches,
        TableSchema probeSchema,
        TableSchema buildSchema,
        CancellationToken ct)
    {
        var dict = new Dictionary<CompositeJoinKey, List<RowRef>>();
        for (var bi = 0; bi < buildBatches.Count; bi++)
        {
            ct.ThrowIfCancellationRequested();
            var batch = buildBatches[bi];
            for (var row = 0; row < batch.RowCount; row++)
            {
                if (!RowPassesAll(batch, plan.BuildSideFilters, row))
                    continue;
                var key = CompositeJoinKeyBuilder.Build(buildSchema, batch, row, plan.BuildKeyColumnIndices);
                if (key.NullMask != 0)
                    continue;
                if (!dict.TryGetValue(key, out var list))
                {
                    list = [];
                    dict[key] = list;
                }

                list.Add(new RowRef(bi, row));
            }
        }

        var matches = new List<RowRefMatch>();
        for (var bi = 0; bi < probeBatches.Count; bi++)
        {
            ct.ThrowIfCancellationRequested();
            var batch = probeBatches[bi];
            for (var row = 0; row < batch.RowCount; row++)
            {
                if (!RowPassesAll(batch, plan.ProbeSideFilters, row))
                    continue;
                var key = CompositeJoinKeyBuilder.Build(probeSchema, batch, row, plan.ProbeKeyColumnIndices);
                if (key.NullMask != 0)
                    continue;
                if (!dict.TryGetValue(key, out var list))
                    continue;
                foreach (var br in list)
                    matches.Add(new RowRefMatch(bi, row, br.BatchIdx, br.RowIdx));
            }
        }

        return matches;
    }

    private static List<RowRefMatch> ComputeSortMergeMatchesFixed(
        JoinPhysicalPlan plan,
        IReadOnlyList<IColumnarBatch> probeBatches,
        IReadOnlyList<IColumnarBatch> buildBatches,
        TableSchema probeSchema,
        CancellationToken ct)
    {
        var comparer = new GroupKeyComparer(probeSchema, plan.ProbeKeyColumnIndices);
        var left = FlattenNonNullFixedKeys(probeBatches, plan.ProbeKeyColumnIndices, plan.ProbeSideFilters, ct);
        var right = FlattenNonNullFixedKeys(buildBatches, plan.BuildKeyColumnIndices, plan.BuildSideFilters, ct);

        left.Sort((a, b) => comparer.Compare(a.Key, b.Key));
        right.Sort((a, b) => comparer.Compare(a.Key, b.Key));

        return MergeSortedKeyRuns(left, right, comparer, ct);
    }

    private static List<RowRefMatch> ComputeSortMergeMatchesUtf8(
        JoinPhysicalPlan plan,
        IReadOnlyList<IColumnarBatch> probeBatches,
        IReadOnlyList<IColumnarBatch> buildBatches,
        TableSchema probeSchema,
        TableSchema buildSchema,
        CancellationToken ct)
    {
        var comparer = new CompositeJoinKeyComparer(probeSchema, plan.ProbeKeyColumnIndices);
        var left = FlattenNonNullCompositeKeys(probeBatches, plan.ProbeKeyColumnIndices, plan.ProbeSideFilters, probeSchema, ct);
        var right = FlattenNonNullCompositeKeys(buildBatches, plan.BuildKeyColumnIndices, plan.BuildSideFilters, buildSchema, ct);

        left.Sort((a, b) => comparer.Compare(a.Key, b.Key));
        right.Sort((a, b) => comparer.Compare(a.Key, b.Key));

        return MergeSortedCompositeRuns(left, right, comparer, ct);
    }

    private static List<RowRefMatch> MergeSortedKeyRuns(
        List<SortEntryFixed> left,
        List<SortEntryFixed> right,
        GroupKeyComparer comparer,
        CancellationToken ct)
    {
        var matches = new List<RowRefMatch>();
        var i = 0;
        var j = 0;
        while (i < left.Count && j < right.Count)
        {
            ct.ThrowIfCancellationRequested();
            var c = comparer.Compare(left[i].Key, right[j].Key);
            if (c < 0)
            {
                i++;
                continue;
            }

            if (c > 0)
            {
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
                    matches.Add(new RowRefMatch(
                        left[ii].BatchIdx,
                        left[ii].RowIdx,
                        right[jj].BatchIdx,
                        right[jj].RowIdx));
                }
            }
        }

        return matches;
    }

    private static List<RowRefMatch> MergeSortedCompositeRuns(
        List<SortEntryUtf8> left,
        List<SortEntryUtf8> right,
        CompositeJoinKeyComparer comparer,
        CancellationToken ct)
    {
        var matches = new List<RowRefMatch>();
        var i = 0;
        var j = 0;
        while (i < left.Count && j < right.Count)
        {
            ct.ThrowIfCancellationRequested();
            var c = comparer.Compare(left[i].Key, right[j].Key);
            if (c < 0)
            {
                i++;
                continue;
            }

            if (c > 0)
            {
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
                    matches.Add(new RowRefMatch(
                        left[ii].BatchIdx,
                        left[ii].RowIdx,
                        right[jj].BatchIdx,
                        right[jj].RowIdx));
                }
            }
        }

        return matches;
    }

    private sealed class SortEntryUtf8
    {
        public required CompositeJoinKey Key { get; init; }

        public int BatchIdx { get; init; }

        public int RowIdx { get; init; }
    }

    private static List<SortEntryFixed> FlattenNonNullFixedKeys(
        IReadOnlyList<IColumnarBatch> batches,
        int[] keyIndices,
        ColumnCompareFilter[]? sideFilters,
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
                if (!RowPassesAll(batch, sideFilters, row))
                    continue;
                var key = FixedWidthGroupKeyBuilder.BuildKey(batch, row, keyIndices, scratch);
                if (key.NullMask != 0)
                    continue;
                list.Add(new SortEntryFixed { Key = key, BatchIdx = bi, RowIdx = row });
            }
        }

        return list;
    }

    private static List<SortEntryUtf8> FlattenNonNullCompositeKeys(
        IReadOnlyList<IColumnarBatch> batches,
        int[] keyIndices,
        ColumnCompareFilter[]? sideFilters,
        TableSchema schema,
        CancellationToken ct)
    {
        var list = new List<SortEntryUtf8>();
        for (var bi = 0; bi < batches.Count; bi++)
        {
            ct.ThrowIfCancellationRequested();
            var batch = batches[bi];
            for (var row = 0; row < batch.RowCount; row++)
            {
                if (!RowPassesAll(batch, sideFilters, row))
                    continue;
                var key = CompositeJoinKeyBuilder.Build(schema, batch, row, keyIndices);
                if (key.NullMask != 0)
                    continue;
                list.Add(new SortEntryUtf8 { Key = key, BatchIdx = bi, RowIdx = row });
            }
        }

        return list;
    }

    private static ColumnarBatch MaterializeMatches(
        JoinPhysicalPlan plan,
        IReadOnlyList<IColumnarBatch> probeBatches,
        IReadOnlyList<IColumnarBatch> buildBatches,
        TableSchema probeSchema,
        TableSchema buildSchema,
        List<RowRefMatch> matches)
    {
        var n = matches.Count;
        var outSchema = plan.OutputSchema;
        var totalCols = outSchema.Columns.Count;
        if (n == 0)
        {
            var empty = new IColumnChunk[totalCols];
            for (var c = 0; c < totalCols; c++)
                empty[c] = EmptyColumnChunk(outSchema.Columns[c].Type);
            return new ColumnarBatch(0, empty);
        }

        var cols = new IColumnChunk[totalCols];
        var order = plan.OutputColumnOrder;
        var leftColCount = probeSchema.Columns.Count;
        if (order is null)
        {
            for (var c = 0; c < leftColCount; c++)
                cols[c] = MaterializeOneColumn(probeBatches, probeSchema.Columns[c].Type, c, matches, useProbeSide: true);

            for (var c = 0; c < buildSchema.Columns.Count; c++)
                cols[leftColCount + c] = MaterializeOneColumn(
                    buildBatches,
                    buildSchema.Columns[c].Type,
                    c,
                    matches,
                    useProbeSide: false);
        }
        else
        {
            for (var ocol = 0; ocol < order.Length; ocol++)
            {
                var r = order[ocol];
                var typ = outSchema.Columns[ocol].Type;
                cols[ocol] = r.IsProbe
                    ? MaterializeOneColumn(probeBatches, typ, r.ColumnIndex, matches, useProbeSide: true)
                    : MaterializeOneColumn(buildBatches, typ, r.ColumnIndex, matches, useProbeSide: false);
            }
        }

        return new ColumnarBatch(n, cols);
    }

    private static IColumnChunk EmptyColumnChunk(RainDbType type)
    {
        if (type == RainDbType.Utf8)
            return new Utf8ColumnChunk(0, new[] { 0 }, Array.Empty<byte>(), ReadOnlyMemory<byte>.Empty, false);

        var w = ColumnTypeSizes.FixedWidthBytes(type);
        return new FixedWidthColumnChunk(type, 0, Array.Empty<byte>(), ReadOnlyMemory<byte>.Empty, false);
    }

    private static IColumnChunk MaterializeOneColumn(
        IReadOnlyList<IColumnarBatch> batches,
        RainDbType type,
        int colIndex,
        List<RowRefMatch> matches,
        bool useProbeSide)
    {
        if (type == RainDbType.Utf8)
            return MaterializeUtf8Column(batches, colIndex, matches, useProbeSide);

        var w = ColumnTypeSizes.FixedWidthBytes(type);
        var n = matches.Count;
        var outVals = new byte[checked(n * w)];
        var nbBytes = ColumnTypeSizes.NullBitmapBytes(n);
        var outNb = new byte[nbBytes];
        var anyNull = false;

        for (var o = 0; o < n; o++)
        {
            var m = matches[o];
            var bi = useProbeSide ? m.LeftBatchIdx : m.RightBatchIdx;
            var ri = useProbeSide ? m.LeftRow : m.RightRow;
            var batch = batches[bi];
            var col = batch.Columns[colIndex];
            var srcNb = col.HasNulls ? col.NullBitmap.Span : ReadOnlySpan<byte>.Empty;
            if (SelectionEvaluator.IsNull(srcNb, ri, col.HasNulls))
            {
                anyNull = true;
                SetNullBit(outNb.AsSpan(), o);
                continue;
            }

            var srcVals = col.Values.Span;
            srcVals.Slice(ri * w, w).CopyTo(outVals.AsSpan(o * w, w));
        }

        return new FixedWidthColumnChunk(
            type,
            n,
            outVals,
            anyNull ? outNb : ReadOnlyMemory<byte>.Empty,
            anyNull);
    }

    private static IColumnChunk MaterializeUtf8Column(
        IReadOnlyList<IColumnarBatch> batches,
        int colIndex,
        List<RowRefMatch> matches,
        bool useProbeSide)
    {
        var n = matches.Count;
        if (n == 0)
            return new Utf8ColumnChunk(0, new[] { 0 }, Array.Empty<byte>(), ReadOnlyMemory<byte>.Empty, hasNulls: false);

        var offsetsMem = new int[n + 1];
        var offsets = offsetsMem.AsSpan();
        var blob = new List<byte>(Math.Max(0, n * 4));
        var anyNull = false;
        byte[]? nbBuf = null;
        if (Utf8ColumnMayHaveNulls(batches, colIndex, matches, useProbeSide))
            nbBuf = new byte[ColumnTypeSizes.NullBitmapBytes(n)];

        for (var o = 0; o < n; o++)
        {
            offsets[o] = blob.Count;
            var m = matches[o];
            var bi = useProbeSide ? m.LeftBatchIdx : m.RightBatchIdx;
            var ri = useProbeSide ? m.LeftRow : m.RightRow;
            var col = batches[bi].Columns[colIndex];
            var srcNb = col.HasNulls ? col.NullBitmap.Span : ReadOnlySpan<byte>.Empty;
            if (SelectionEvaluator.IsNull(srcNb, ri, col.HasNulls))
            {
                anyNull = true;
                if (nbBuf != null)
                    SetNullBit(nbBuf.AsSpan(), o);
                continue;
            }

            foreach (var b in ReadUtf8Payload(col, ri))
                blob.Add(b);
        }

        offsets[n] = blob.Count;
        ReadOnlyMemory<byte> nbOut = nbBuf != null ? nbBuf : ReadOnlyMemory<byte>.Empty;
        return new Utf8ColumnChunk(n, offsetsMem, blob.ToArray(), nbOut, anyNull);
    }

    private static bool Utf8ColumnMayHaveNulls(
        IReadOnlyList<IColumnarBatch> batches,
        int colIndex,
        List<RowRefMatch> matches,
        bool useProbeSide)
    {
        foreach (var m in matches)
        {
            var bi = useProbeSide ? m.LeftBatchIdx : m.RightBatchIdx;
            if (batches[bi].Columns[colIndex].HasNulls)
                return true;
        }

        return false;
    }

    private static ReadOnlySpan<byte> ReadUtf8Payload(IColumnChunk col, int row)
    {
        return col switch
        {
            Utf8ColumnChunk utf8 => ReadUtf8ArrowPayload(utf8, row),
            Utf8LengthPrefixedColumnChunk lp => lp.GetPayloadSpan(row),
            _ => throw new NotSupportedException($"Unexpected UTF-8 chunk type {col.GetType().Name}."),
        };
    }

    private static ReadOnlySpan<byte> ReadUtf8ArrowPayload(Utf8ColumnChunk src, int row)
    {
        var off = src.Offsets.Span;
        var start = off[row];
        var end = off[row + 1];
        return src.Values.Span.Slice(start, end - start);
    }

    private static void SetNullBit(Span<byte> nb, int row) => nb[row >> 3] |= (byte)(1 << (row & 7));
}
