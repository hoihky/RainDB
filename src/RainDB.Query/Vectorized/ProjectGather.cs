using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Schema;

namespace RainDB.Query.Vectorized;

internal static class ProjectGather
{
    internal static ColumnarBatch Project(
        IColumnarBatch batch,
        ReadOnlySpan<int> outputColumnIndices,
        bool useRowSelection,
        ReadOnlySpan<int> selectedRows,
        int selectedCount)
    {
        if (useRowSelection && selectedRows.Length < selectedCount)
            throw new ArgumentException(nameof(selectedCount));

        var cols = new IColumnChunk[outputColumnIndices.Length];
        for (var c = 0; c < outputColumnIndices.Length; c++)
        {
            var colIdx = outputColumnIndices[c];
            if ((uint)colIdx >= (uint)batch.Columns.Count)
                throw new ArgumentOutOfRangeException(nameof(outputColumnIndices));
            cols[c] = GatherColumn(batch.Columns[colIdx], useRowSelection, selectedRows, selectedCount);
        }

        return new ColumnarBatch(selectedCount, cols);
    }

    private static IColumnChunk GatherColumn(
        IColumnChunk source,
        bool useRowSelection,
        ReadOnlySpan<int> selectedRows,
        int selectedCount)
    {
        if (source.PhysicalType == RainDbType.Utf8)
        {
            if (source is Utf8ColumnChunk utf8)
                return GatherUtf8Arrow(utf8, useRowSelection, selectedRows, selectedCount);
            if (source is Utf8LengthPrefixedColumnChunk lp)
                return GatherUtf8LengthPrefixed(lp, useRowSelection, selectedRows, selectedCount);
            throw new NotSupportedException("Unknown UTF-8 chunk implementation.");
        }

        return GatherFixedWidth(source, useRowSelection, selectedRows, selectedCount);
    }

    private static IColumnChunk GatherFixedWidth(
        IColumnChunk source,
        bool useRowSelection,
        ReadOnlySpan<int> selectedRows,
        int selectedCount)
    {
        var type = source.PhysicalType;
        var w = ColumnTypeSizes.FixedWidthBytes(type);
        var srcValues = source.Values.Span;
        var srcNb = source.HasNulls ? source.NullBitmap.Span : ReadOnlySpan<byte>.Empty;
        var outValues = new byte[checked(selectedCount * w)];
        var anyNull = false;
        if (source.HasNulls)
        {
            var nbBytes = ColumnTypeSizes.NullBitmapBytes(selectedCount);
            var outNb = new byte[nbBytes];
            for (var o = 0; o < selectedCount; o++)
            {
                var r = RowAt(useRowSelection, selectedRows, o);
                if (SelectionEvaluator.IsNull(srcNb, r, true))
                {
                    anyNull = true;
                    SetNullBit(outNb, o);
                }
                else
                {
                    srcValues.Slice(r * w, w).CopyTo(outValues.AsSpan(o * w, w));
                }
            }

            return new FixedWidthColumnChunk(type, selectedCount, outValues, outNb, anyNull);
        }

        for (var o = 0; o < selectedCount; o++)
        {
            var r = RowAt(useRowSelection, selectedRows, o);
            srcValues.Slice(r * w, w).CopyTo(outValues.AsSpan(o * w, w));
        }

        return new FixedWidthColumnChunk(type, selectedCount, outValues, ReadOnlyMemory<byte>.Empty, hasNulls: false);
    }

    private static void SetNullBit(Span<byte> nb, int row) => nb[row >> 3] |= (byte)(1 << (row & 7));

    private static int RowAt(bool useRowSelection, ReadOnlySpan<int> selectedRows, int o) =>
        useRowSelection ? selectedRows[o] : o;

    private static Utf8ColumnChunk GatherUtf8Arrow(
        Utf8ColumnChunk src,
        bool useRowSelection,
        ReadOnlySpan<int> selectedRows,
        int selectedCount)
    {
        var offsetsMem = new int[selectedCount + 1];
        var offsets = offsetsMem.AsSpan();
        var srcOffsets = src.Offsets.Span;
        var blob = new List<byte>(Math.Max(0, selectedCount * 4));
        var anyNull = false;
        byte[]? nbBuf = null;
        if (src.HasNulls)
        {
            var nbBytes = ColumnTypeSizes.NullBitmapBytes(selectedCount);
            nbBuf = new byte[nbBytes];
        }

        var srcNb = src.HasNulls ? src.NullBitmap.Span : ReadOnlySpan<byte>.Empty;
        for (var o = 0; o < selectedCount; o++)
        {
            offsets[o] = blob.Count;
            var r = RowAt(useRowSelection, selectedRows, o);
            if (src.HasNulls && SelectionEvaluator.IsNull(srcNb, r, true))
            {
                anyNull = true;
                if (nbBuf != null)
                    SetNullBit(nbBuf.AsSpan(), o);
                continue;
            }

            var start = srcOffsets[r];
            var end = srcOffsets[r + 1];
            var len = end - start;
            foreach (var b in src.Values.Span.Slice(start, len))
                blob.Add(b);
        }

        offsets[selectedCount] = blob.Count;
        ReadOnlyMemory<byte> nbOut = nbBuf != null ? nbBuf : ReadOnlyMemory<byte>.Empty;
        return new Utf8ColumnChunk(selectedCount, offsetsMem, blob.ToArray(), nbOut, anyNull);
    }

    private static Utf8LengthPrefixedColumnChunk GatherUtf8LengthPrefixed(
        Utf8LengthPrefixedColumnChunk src,
        bool useRowSelection,
        ReadOnlySpan<int> selectedRows,
        int selectedCount)
    {
        var blob = new List<byte>(Math.Max(0, selectedCount * 6));
        var anyNull = false;
        ReadOnlyMemory<byte> nbOut = ReadOnlyMemory<byte>.Empty;
        byte[]? nbBuf = null;
        if (src.HasNulls)
        {
            var nbBytes = ColumnTypeSizes.NullBitmapBytes(selectedCount);
            nbBuf = new byte[nbBytes];
        }

        var srcNb = src.HasNulls ? src.NullBitmap.Span : ReadOnlySpan<byte>.Empty;
        for (var o = 0; o < selectedCount; o++)
        {
            var r = RowAt(useRowSelection, selectedRows, o);
            if (src.HasNulls && SelectionEvaluator.IsNull(srcNb, r, true))
            {
                anyNull = true;
                if (nbBuf != null)
                    SetNullBit(nbBuf.AsSpan(), o);
                var z = BitConverter.GetBytes(0);
                foreach (var b in z)
                    blob.Add(b);
                continue;
            }

            var payload = src.GetPayloadSpan(r);
            var lenBytes = BitConverter.GetBytes(payload.Length);
            foreach (var b in lenBytes)
                blob.Add(b);
            foreach (var b in payload)
                blob.Add(b);
        }

        if (nbBuf != null)
            nbOut = nbBuf;
        return new Utf8LengthPrefixedColumnChunk(selectedCount, blob.ToArray(), nbOut, anyNull);
    }
}
