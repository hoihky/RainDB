using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using RainDB.Catalog;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Execution;
using RainDB.Query.Plans;
using RainDB.Query.Results;
using RainDB.Query.Vectorized;
using RainDB.Schema;

namespace RainDB.Query.Execution;

/// <summary>In-memory sort and/or LIMIT over columnar batches (single-table or pre-materialized join output).</summary>
public static class SortTopNEngine
{
    public static ValueTask<IQueryResult> ExecuteTableAsync(
        SortTopNPhysicalPlan plan,
        IColumnarTableSource table,
        IExecutionContext context)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(context);
        if (plan.TableId != table.Id)
            throw new ArgumentException("SortTopN plan table id does not match source.", nameof(table));
        ValidateSortKeys(table.Schema, plan.SortKeys);
        ValidateOutputAndFilters(table.Schema, plan.OutputColumnIndices, plan.Filters);

        var batches = table.Batches;
        var ct = context.CancellationToken;
        var rows = CollectFilteredRows(batches, plan.Filters, ct);
        if (plan.SortKeys.Length > 0)
            Array.Sort(rows, new RowLocComparer(table.Schema, plan.SortKeys, batches));

        var take = plan.Limit is { } lim ? Math.Min(lim, rows.Length) : rows.Length;
        var batch = MaterializeRows(batches, table.Schema, rows.AsSpan(0, take), plan.OutputColumnIndices);
        return new ValueTask<IQueryResult>(new ColumnarMaterializedQueryResult([batch]));
    }

    public static async ValueTask<IQueryResult> ExecuteJoinAsync(
        JoinSortTopNPhysicalPlan plan,
        IColumnarTableSource probeTable,
        IColumnarTableSource buildTable,
        IExecutionContext context)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(probeTable);
        ArgumentNullException.ThrowIfNull(buildTable);
        ArgumentNullException.ThrowIfNull(context);
        var joinRes = await JoinExecutionEngine.ExecuteAsync(plan.Join, probeTable, buildTable, context).ConfigureAwait(false);
        if (joinRes is not IColumnarQueryResult col)
            throw new InvalidOperationException("Join must return columnar result.");
        var batches = col.Batches;
        var schema = plan.Join.OutputSchema;
        ValidateSortKeys(schema, plan.SortKeys);
        var rows = CollectAllRows(batches, context.CancellationToken);
        if (plan.SortKeys.Length > 0)
            Array.Sort(rows, new RowLocComparer(schema, plan.SortKeys, batches));

        var take = plan.Limit is { } lim ? Math.Min(lim, rows.Length) : rows.Length;
        var outIx = new int[schema.Columns.Count];
        for (var i = 0; i < outIx.Length; i++)
            outIx[i] = i;
        var batch = MaterializeRows(batches, schema, rows.AsSpan(0, take), outIx);
        return new ColumnarMaterializedQueryResult([batch]);
    }

    private static void ValidateOutputAndFilters(TableSchema schema, int[] outputIx, ColumnCompareFilter[]? filters)
    {
        foreach (var ix in outputIx)
        {
            if ((uint)ix >= (uint)schema.Columns.Count)
                throw new ArgumentException($"Output column index {ix} is out of range.", nameof(outputIx));
        }

        if (filters is { } fa)
        {
            foreach (var f in fa)
            {
                if ((uint)f.ColumnIndex >= (uint)schema.Columns.Count)
                    throw new ArgumentException("Filter column index is out of range.", nameof(filters));
            }
        }
    }

    private static void ValidateSortKeys(TableSchema schema, SortKeyPhysicalSpec[] keys)
    {
        foreach (var k in keys)
        {
            if ((uint)k.ColumnIndex >= (uint)schema.Columns.Count)
                throw new ArgumentException($"Sort key column index {k.ColumnIndex} is out of range.", nameof(keys));
            var t = schema.Columns[k.ColumnIndex].Type;
            if (t != RainDbType.Utf8 && !ColumnTypeSizes.IsFixedWidth(t))
                throw new NotSupportedException($"ORDER BY on type {t} is not supported.");
        }
    }

    private static RowLoc[] CollectFilteredRows(
        IReadOnlyList<IColumnarBatch> batches,
        ColumnCompareFilter[]? filters,
        CancellationToken ct)
    {
        var rent = ArrayPool<int>.Shared;
        var total = 0;
        foreach (var b in batches)
            total += b.RowCount;
        var tmp = rent.Rent(Math.Max(total, 16));
        try
        {
            var list = new List<RowLoc>(total);
            for (var bi = 0; bi < batches.Count; bi++)
            {
                ct.ThrowIfCancellationRequested();
                var batch = batches[bi];
                int k;
                if (filters is { Length: > 0 } fa)
                {
                    k = SelectionEvaluator.FillSelectedRowsConjunctive(batch, fa, tmp.AsSpan(0, batch.RowCount));
                    for (var i = 0; i < k; i++)
                        list.Add(new RowLoc(bi, tmp[i]));
                }
                else
                {
                    for (var r = 0; r < batch.RowCount; r++)
                        list.Add(new RowLoc(bi, r));
                }
            }

            return list.ToArray();
        }
        finally
        {
            rent.Return(tmp);
        }
    }

    private static RowLoc[] CollectAllRows(IReadOnlyList<IColumnarBatch> batches, CancellationToken ct)
    {
        var list = new List<RowLoc>();
        for (var bi = 0; bi < batches.Count; bi++)
        {
            ct.ThrowIfCancellationRequested();
            var batch = batches[bi];
            for (var r = 0; r < batch.RowCount; r++)
                list.Add(new RowLoc(bi, r));
        }

        return list.ToArray();
    }

    private static ColumnarBatch MaterializeRows(
        IReadOnlyList<IColumnarBatch> batches,
        TableSchema schema,
        ReadOnlySpan<RowLoc> rows,
        ReadOnlySpan<int> outputColumnIndices)
    {
        var n = rows.Length;
        var cols = new IColumnChunk[outputColumnIndices.Length];
        for (var c = 0; c < outputColumnIndices.Length; c++)
        {
            var colIx = outputColumnIndices[c];
            var t = schema.Columns[colIx].Type;
            cols[c] = t == RainDbType.Utf8
                ? GatherUtf8Column(batches, colIx, rows)
                : GatherFixedWidthColumn(batches, colIx, t, rows);
        }

        return new ColumnarBatch(n, cols);
    }

    private static IColumnChunk GatherFixedWidthColumn(
        IReadOnlyList<IColumnarBatch> batches,
        int colIx,
        RainDbType type,
        ReadOnlySpan<RowLoc> rows)
    {
        var w = ColumnTypeSizes.FixedWidthBytes(type);
        var values = new byte[checked(rows.Length * w)];
        var nbBytes = ColumnTypeSizes.NullBitmapBytes(rows.Length);
        var nb = nbBytes > 0 ? new byte[nbBytes] : Array.Empty<byte>();
        var anyNull = false;
        for (var o = 0; o < rows.Length; o++)
        {
            var loc = rows[o];
            var col = batches[loc.BatchIdx].Columns[colIx];
            var r = loc.RowIdx;
            var srcNb = col.HasNulls ? col.NullBitmap.Span : ReadOnlySpan<byte>.Empty;
            if (SelectionEvaluator.IsNull(srcNb, r, col.HasNulls))
            {
                anyNull = true;
                SetNull(nb, o);
                continue;
            }

            col.Values.Span.Slice(r * w, w).CopyTo(values.AsSpan(o * w, w));
        }

        return new FixedWidthColumnChunk(type, rows.Length, values, nb, anyNull);
    }

    private static IColumnChunk GatherUtf8Column(IReadOnlyList<IColumnarBatch> batches, int colIx, ReadOnlySpan<RowLoc> rows)
    {
        var offsets = new int[rows.Length + 1];
        using var blob = new MemoryStream();
        var nbBytes = ColumnTypeSizes.NullBitmapBytes(rows.Length);
        var nb = nbBytes > 0 ? new byte[nbBytes] : Array.Empty<byte>();
        var anyNull = false;
        for (var o = 0; o < rows.Length; o++)
        {
            offsets[o] = (int)blob.Length;
            var loc = rows[o];
            var col = batches[loc.BatchIdx].Columns[colIx];
            var r = loc.RowIdx;
            var srcNb = col.HasNulls ? col.NullBitmap.Span : ReadOnlySpan<byte>.Empty;
            if (SelectionEvaluator.IsNull(srcNb, r, col.HasNulls))
            {
                anyNull = true;
                SetNull(nb, o);
                continue;
            }

            ReadOnlySpan<byte> payload = col switch
            {
                Utf8ColumnChunk u => u.Values.Span[u.Offsets.Span[r]..u.Offsets.Span[r + 1]],
                Utf8LengthPrefixedColumnChunk lp => lp.GetPayloadSpan(r),
                _ => throw new NotSupportedException($"UTF-8 chunk {col.GetType().Name}."),
            };
            blob.Write(payload);
        }

        offsets[rows.Length] = (int)blob.Length;
        return new Utf8ColumnChunk(rows.Length, offsets, blob.ToArray(), nb, anyNull);
    }

    private static void SetNull(byte[] nb, int row)
    {
        var b = row >> 3;
        nb[b] |= (byte)(1 << (row & 7));
    }

    private readonly struct RowLoc
    {
        public RowLoc(int batchIdx, int rowIdx)
        {
            BatchIdx = batchIdx;
            RowIdx = rowIdx;
        }

        public int BatchIdx { get; }

        public int RowIdx { get; }
    }

    private sealed class RowLocComparer : IComparer<RowLoc>
    {
        private readonly TableSchema _schema;
        private readonly SortKeyPhysicalSpec[] _keys;
        private readonly IReadOnlyList<IColumnarBatch> _batches;

        public RowLocComparer(TableSchema schema, SortKeyPhysicalSpec[] keys, IReadOnlyList<IColumnarBatch> batches)
        {
            _schema = schema;
            _keys = keys;
            _batches = batches;
        }

        public int Compare(RowLoc x, RowLoc y)
        {
            foreach (var spec in _keys)
            {
                var c = CompareAtColumn(spec.ColumnIndex, x, y);
                if (c != 0)
                    return spec.Descending ? -c : c;
            }

            return 0;
        }

        private int CompareAtColumn(int colIx, RowLoc a, RowLoc b)
        {
            var colA = _batches[a.BatchIdx].Columns[colIx];
            var colB = _batches[b.BatchIdx].Columns[colIx];
            var t = _schema.Columns[colIx].Type;
            var na = colA.HasNulls && SelectionEvaluator.IsNull(colA.NullBitmap.Span, a.RowIdx, true);
            var nb = colB.HasNulls && SelectionEvaluator.IsNull(colB.NullBitmap.Span, b.RowIdx, true);
            if (na && nb)
                return 0;
            if (na)
                return -1;
            if (nb)
                return 1;

            return t switch
            {
                RainDbType.Utf8 => CompareUtf8(colA, a.RowIdx, colB, b.RowIdx),
                RainDbType.Int32 => ReadI32(colA, a.RowIdx).CompareTo(ReadI32(colB, b.RowIdx)),
                RainDbType.Int64 => ReadI64(colA, a.RowIdx).CompareTo(ReadI64(colB, b.RowIdx)),
                RainDbType.Float64 => ReadF64(colA, a.RowIdx).CompareTo(ReadF64(colB, b.RowIdx)),
                RainDbType.Boolean => ReadBool(colA, a.RowIdx).CompareTo(ReadBool(colB, b.RowIdx)),
                _ => 0,
            };
        }

        private static int CompareUtf8(IColumnChunk ca, int ra, IColumnChunk cb, int rb)
        {
            ReadOnlySpan<byte> sa = ca switch
            {
                Utf8ColumnChunk u => u.Values.Span[u.Offsets.Span[ra]..u.Offsets.Span[ra + 1]],
                Utf8LengthPrefixedColumnChunk lp => lp.GetPayloadSpan(ra),
                _ => throw new InvalidOperationException(),
            };
            ReadOnlySpan<byte> sb = cb switch
            {
                Utf8ColumnChunk u => u.Values.Span[u.Offsets.Span[rb]..u.Offsets.Span[rb + 1]],
                Utf8LengthPrefixedColumnChunk lp => lp.GetPayloadSpan(rb),
                _ => throw new InvalidOperationException(),
            };
            return sa.SequenceCompareTo(sb);
        }

        private static int ReadI32(IColumnChunk c, int row) =>
            BinaryPrimitives.ReadInt32LittleEndian(c.Values.Span.Slice(row * sizeof(int), sizeof(int)));

        private static long ReadI64(IColumnChunk c, int row) =>
            BinaryPrimitives.ReadInt64LittleEndian(c.Values.Span.Slice(row * sizeof(long), sizeof(long)));

        private static double ReadF64(IColumnChunk c, int row) =>
            BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(c.Values.Span.Slice(row * sizeof(double), sizeof(double))));

        private static int ReadBool(IColumnChunk c, int row) => c.Values.Span[row] != 0 ? 1 : 0;
    }
}
