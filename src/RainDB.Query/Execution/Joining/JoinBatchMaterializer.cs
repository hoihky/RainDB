using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Query.Plans;
using RainDB.Query.Vectorized;
using RainDB.Schema;

namespace RainDB.Query.Execution.Joining;

internal static class JoinBatchMaterializer
{
    public static ColumnarBatch Materialize(
        JoinPhysicalPlan plan,
        IReadOnlyList<IColumnarBatch> probeBatches,
        IReadOnlyList<IColumnarBatch> buildBatches,
        TableSchema probeSchema,
        TableSchema buildSchema,
        IReadOnlyList<JoinRowMatch> matches)
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

    public static ColumnarBatch EmptyBatch(JoinPhysicalPlan plan)
    {
        var outSchema = plan.OutputSchema;
        var totalCols = outSchema.Columns.Count;
        var empty = new IColumnChunk[totalCols];
        for (var c = 0; c < totalCols; c++)
            empty[c] = EmptyColumnChunk(outSchema.Columns[c].Type);
        return new ColumnarBatch(0, empty);
    }

    private static IColumnChunk EmptyColumnChunk(RainDbType type)
    {
        if (type == RainDbType.Utf8)
            return new Utf8ColumnChunk(0, new[] { 0 }, Array.Empty<byte>(), ReadOnlyMemory<byte>.Empty, false);

        return new FixedWidthColumnChunk(type, 0, Array.Empty<byte>(), ReadOnlyMemory<byte>.Empty, false);
    }

    private static IColumnChunk MaterializeOneColumn(
        IReadOnlyList<IColumnarBatch> batches,
        RainDbType type,
        int colIndex,
        IReadOnlyList<JoinRowMatch> matches,
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
        IReadOnlyList<JoinRowMatch> matches,
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
        IReadOnlyList<JoinRowMatch> matches,
        bool useProbeSide)
    {
        for (var i = 0; i < matches.Count; i++)
        {
            var m = matches[i];
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
