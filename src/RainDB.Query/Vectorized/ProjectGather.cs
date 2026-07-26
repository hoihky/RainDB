using System.Buffers;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Memory;
using RainDB.Schema;

namespace RainDB.Query.Vectorized;

internal static class ProjectGather
{
    internal static ColumnarBatch Project(
        IColumnarBatch batch,
        ReadOnlySpan<int> outputColumnIndices,
        bool useRowSelection,
        ReadOnlySpan<int> selectedRows,
        int selectedCount,
        IBufferPool bufferPool,
        IAlignedBufferPool alignedBufferPool)
    {
        ArgumentNullException.ThrowIfNull(bufferPool);
        ArgumentNullException.ThrowIfNull(alignedBufferPool);
        if (useRowSelection && selectedRows.Length < selectedCount)
            throw new ArgumentException(nameof(selectedCount));

        var cols = new IColumnChunk[outputColumnIndices.Length];
        for (var c = 0; c < outputColumnIndices.Length; c++)
        {
            var colIdx = outputColumnIndices[c];
            if ((uint)colIdx >= (uint)batch.Columns.Count)
                throw new ArgumentOutOfRangeException(nameof(outputColumnIndices));
            cols[c] = GatherColumn(
                batch.Columns[colIdx],
                useRowSelection,
                selectedRows,
                selectedCount,
                bufferPool,
                alignedBufferPool);
        }

        return new ColumnarBatch(selectedCount, cols);
    }

    private static IColumnChunk GatherColumn(
        IColumnChunk source,
        bool useRowSelection,
        ReadOnlySpan<int> selectedRows,
        int selectedCount,
        IBufferPool bufferPool,
        IAlignedBufferPool alignedBufferPool)
    {
        if (source.PhysicalType == RainDbType.Utf8)
        {
            if (source is Utf8ColumnChunk utf8)
                return GatherUtf8Arrow(utf8, useRowSelection, selectedRows, selectedCount);
            if (source is Utf8LengthPrefixedColumnChunk lp)
                return GatherUtf8LengthPrefixed(lp, useRowSelection, selectedRows, selectedCount);
            throw new NotSupportedException("Unknown UTF-8 chunk implementation.");
        }

        return GatherFixedWidth(source, useRowSelection, selectedRows, selectedCount, bufferPool, alignedBufferPool);
    }

    private static IColumnChunk GatherFixedWidth(
        IColumnChunk source,
        bool useRowSelection,
        ReadOnlySpan<int> selectedRows,
        int selectedCount,
        IBufferPool bufferPool,
        IAlignedBufferPool alignedBufferPool)
    {
        var type = source.PhysicalType;
        var w = ColumnTypeSizes.FixedWidthBytes(type);
        var srcValues = source.Values.Span;
        var srcNb = source.HasNulls ? source.NullBitmap.Span : ReadOnlySpan<byte>.Empty;

        if (!useRowSelection && selectedCount == source.RowCount)
            return CopyEntireFixedWidthColumn(source, type, w, srcValues, srcNb, alignedBufferPool, bufferPool);

        var valueBytes = checked(selectedCount * w);
        var valuesOwner = alignedBufferPool.RentAligned(valueBytes);
        var outValues = valuesOwner.Memory.Span[..valueBytes];

        if (source.HasNulls)
        {
            var nbBytes = ColumnTypeSizes.NullBitmapBytes(selectedCount);
            var nullOwner = CreateNullOwner(bufferPool, bufferPool.Rent(nbBytes), nbBytes);
            var outNb = nullOwner.Memory.Span[..nbBytes];
            outNb.Clear();
            var anyNull = false;
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
                    srcValues.Slice(r * w, w).CopyTo(outValues.Slice(o * w, w));
                }
            }

            return new PooledFixedWidthColumnChunk(type, selectedCount, valuesOwner, valueBytes, nullOwner, nbBytes, anyNull);
        }

        for (var o = 0; o < selectedCount; o++)
        {
            var r = RowAt(useRowSelection, selectedRows, o);
            srcValues.Slice(r * w, w).CopyTo(outValues.Slice(o * w, w));
        }

        return new PooledFixedWidthColumnChunk(type, selectedCount, valuesOwner, valueBytes, nullBitmapOwner: null, nullBitmapBytes: 0, hasNulls: false);
    }

    private static IColumnChunk CopyEntireFixedWidthColumn(
        IColumnChunk source,
        RainDbType type,
        int w,
        ReadOnlySpan<byte> srcValues,
        ReadOnlySpan<byte> srcNb,
        IAlignedBufferPool alignedBufferPool,
        IBufferPool bufferPool)
    {
        var rowCount = source.RowCount;
        var valueBytes = checked(rowCount * w);
        var valuesOwner = alignedBufferPool.RentAligned(valueBytes);
        srcValues.CopyTo(valuesOwner.Memory.Span[..valueBytes]);

        if (!source.HasNulls)
        {
            return new PooledFixedWidthColumnChunk(
                type,
                rowCount,
                valuesOwner,
                valueBytes,
                nullBitmapOwner: null,
                nullBitmapBytes: 0,
                hasNulls: false);
        }

        var nbBytes = ColumnTypeSizes.NullBitmapBytes(rowCount);
        var nullOwner = CreateNullOwner(bufferPool, bufferPool.Rent(nbBytes), nbBytes);
        srcNb[..nbBytes].CopyTo(nullOwner.Memory.Span[..nbBytes]);
        return new PooledFixedWidthColumnChunk(type, rowCount, valuesOwner, valueBytes, nullOwner, nbBytes, hasNulls: true);
    }

    private static PooledBufferOwner CreateNullOwner(IBufferPool bufferPool, byte[] rented, int length) =>
        new(bufferPool, rented, length);

    private sealed class PooledBufferOwner : IMemoryOwner<byte>
    {
        private readonly IBufferPool _pool;
        private byte[]? _buffer;
        private readonly int _length;

        public PooledBufferOwner(IBufferPool pool, byte[] buffer, int length)
        {
            _pool = pool;
            _buffer = buffer;
            _length = length;
        }

        public Memory<byte> Memory
        {
            get
            {
                var b = _buffer ?? throw new ObjectDisposedException(nameof(PooledBufferOwner));
                return new Memory<byte>(b, 0, _length);
            }
        }

        public void Dispose()
        {
            var b = Interlocked.Exchange(ref _buffer, null);
            if (b is not null)
                _pool.Return(b);
        }
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
