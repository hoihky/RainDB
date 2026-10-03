using System.Buffers.Binary;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Schema;

namespace RainDB.Core.Persistence;

/// <summary>Heuristic Int32 dictionary encoding for batch persistence.</summary>
public sealed class Int32DictionaryColumnEncoder
{
    public bool TryEncode(FixedWidthColumnChunk column, out DictionaryEncodedInt32ColumnChunk? encoded)
    {
        encoded = null;
        if (column.PhysicalType != RainDbType.Int32)
            return false;
        var rowCount = column.RowCount;
        if (rowCount < 4)
            return false;

        var values = column.Values.Span;
        var nb = column.HasNulls ? column.NullBitmap.Span : ReadOnlySpan<byte>.Empty;
        var distinct = new Dictionary<int, int>();
        for (var row = 0; row < rowCount; row++)
        {
            if (column.HasNulls && IsNull(nb, row))
                continue;
            var v = BinaryPrimitives.ReadInt32LittleEndian(values.Slice(row * sizeof(int), sizeof(int)));
            if (!distinct.ContainsKey(v))
                distinct[v] = distinct.Count;
        }

        var dictCount = distinct.Count;
        if (dictCount == 0 || dictCount > 65535)
            return false;

        var indexWidth = dictCount <= byte.MaxValue ? (byte)1 : (byte)2;
        var rawBytes = rowCount * sizeof(int);
        var encodedBytes = dictCount * sizeof(int) + rowCount * indexWidth + (column.HasNulls ? ColumnTypeSizes.NullBitmapBytes(rowCount) : 0);
        if (encodedBytes >= rawBytes)
            return false;

        var dictionary = new int[dictCount];
        foreach (var (value, ordinal) in distinct)
            dictionary[ordinal] = value;

        var indices = GC.AllocateUninitializedArray<byte>(rowCount * indexWidth);
        for (var row = 0; row < rowCount; row++)
        {
            if (column.HasNulls && IsNull(nb, row))
            {
                WriteIndex(indices, row, indexWidth, 0);
                continue;
            }

            var v = BinaryPrimitives.ReadInt32LittleEndian(values.Slice(row * sizeof(int), sizeof(int)));
            WriteIndex(indices, row, indexWidth, distinct[v]);
        }

        encoded = new DictionaryEncodedInt32ColumnChunk(
            rowCount,
            dictionary,
            indices,
            indexWidth,
            column.NullBitmap,
            column.HasNulls);
        return true;
    }

    private static void WriteIndex(byte[] indices, int row, byte indexWidth, int ordinal)
    {
        var o = row * indexWidth;
        switch (indexWidth)
        {
            case 1:
                indices[o] = (byte)ordinal;
                break;
            case 2:
                BinaryPrimitives.WriteUInt16LittleEndian(indices.AsSpan(o, 2), (ushort)ordinal);
                break;
            default:
                BinaryPrimitives.WriteUInt32LittleEndian(indices.AsSpan(o, 4), (uint)ordinal);
                break;
        }
    }

    private static bool IsNull(ReadOnlySpan<byte> nullBitmap, int row) =>
        (nullBitmap[row >> 3] & (1 << (row & 7))) != 0;
}
