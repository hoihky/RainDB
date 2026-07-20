using RainDB.Columnar;
using RainDB.Schema;

namespace RainDB.Core.Columnar;

/// <summary>Fixed-width column data packed in <see cref="Values"/> (native endianness).</summary>
public sealed class FixedWidthColumnChunk : IColumnChunk
{
    public FixedWidthColumnChunk(RainDbType type, int rowCount, ReadOnlyMemory<byte> values, ReadOnlyMemory<byte> nullBitmap, bool hasNulls)
    {
        if (!ColumnTypeSizes.IsFixedWidth(type))
            throw new ArgumentException("Use Utf8ColumnChunk for Utf8.", nameof(type));
        if (rowCount < 0)
            throw new ArgumentOutOfRangeException(nameof(rowCount));
        var w = ColumnTypeSizes.FixedWidthBytes(type);
        if (values.Length != rowCount * w)
            throw new ArgumentException($"Values length {values.Length} != rowCount({rowCount}) * width({w}).", nameof(values));
        var nb = ColumnTypeSizes.NullBitmapBytes(rowCount);
        if (hasNulls && nullBitmap.Length < nb)
            throw new ArgumentException("Null bitmap too short for row count.", nameof(nullBitmap));
        PhysicalType = type;
        RowCount = rowCount;
        Values = values;
        NullBitmap = hasNulls ? nullBitmap[..nb] : ReadOnlyMemory<byte>.Empty;
        HasNulls = hasNulls;
    }

    public RainDbType PhysicalType { get; }

    public int RowCount { get; }

    public bool HasNulls { get; }

    public ReadOnlyMemory<byte> NullBitmap { get; }

    public ReadOnlyMemory<byte> Values { get; }
}
