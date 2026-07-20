using RainDB.Columnar;
using RainDB.Schema;

namespace RainDB.Core.Columnar;

/// <summary>UTF-8 column: <see cref="Values"/> holds concatenated bytes; <see cref="Offsets"/> has length <c>rowCount + 1</c> (Arrow-style).</summary>
public sealed class Utf8ColumnChunk : IColumnChunk
{
    public Utf8ColumnChunk(int rowCount, ReadOnlyMemory<int> offsets, ReadOnlyMemory<byte> values, ReadOnlyMemory<byte> nullBitmap, bool hasNulls)
    {
        if (rowCount < 0)
            throw new ArgumentOutOfRangeException(nameof(rowCount));
        if (offsets.Length != rowCount + 1)
            throw new ArgumentException("Offsets must have length rowCount + 1.", nameof(offsets));
        if (offsets.Length > 0 && offsets.Span[0] != 0)
            throw new ArgumentException("Offsets[0] must be 0.", nameof(offsets));
        var last = offsets.Length > 0 ? offsets.Span[^1] : 0;
        if (last != values.Length)
            throw new ArgumentException("Last offset must equal Values.Length.", nameof(offsets));
        for (var i = 1; i < offsets.Length; i++)
        {
            if (offsets.Span[i] < offsets.Span[i - 1])
                throw new ArgumentException("Offsets must be non-decreasing.", nameof(offsets));
        }

        var nb = ColumnTypeSizes.NullBitmapBytes(rowCount);
        if (hasNulls && nullBitmap.Length < nb)
            throw new ArgumentException("Null bitmap too short for row count.", nameof(nullBitmap));
        RowCount = rowCount;
        Offsets = offsets;
        Values = values;
        NullBitmap = hasNulls ? nullBitmap[..nb] : ReadOnlyMemory<byte>.Empty;
        HasNulls = hasNulls;
    }

    public RainDbType PhysicalType => RainDbType.Utf8;

    public int RowCount { get; }

    public bool HasNulls { get; }

    public ReadOnlyMemory<byte> NullBitmap { get; }

    /// <summary>Offsets into <see cref="Values"/>; row <c>i</c> spans <c>[Offsets[i], Offsets[i+1])</c>.</summary>
    public ReadOnlyMemory<int> Offsets { get; }

    /// <summary>Concatenated UTF-8 payload.</summary>
    public ReadOnlyMemory<byte> Values { get; }
}
