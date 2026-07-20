using RainDB.Columnar;
using RainDB.Schema;

namespace RainDB.Core.Columnar;

/// <summary>
/// UTF-8 column stored as <c>[int32 little-endian length][utf8 bytes]...</c> per row (length-prefixed run).
/// Random access builds a prefix index once in the constructor.
/// </summary>
public sealed class Utf8LengthPrefixedColumnChunk : IColumnChunk
{
    private readonly int[] _rowStarts;

    public Utf8LengthPrefixedColumnChunk(int rowCount, ReadOnlyMemory<byte> values, ReadOnlyMemory<byte> nullBitmap, bool hasNulls)
    {
        if (rowCount < 0)
            throw new ArgumentOutOfRangeException(nameof(rowCount));
        var nb = ColumnTypeSizes.NullBitmapBytes(rowCount);
        if (hasNulls && nullBitmap.Length < nb)
            throw new ArgumentException("Null bitmap too short for row count.", nameof(nullBitmap));
        _rowStarts = new int[rowCount + 1];
        var span = values.Span;
        var pos = 0;
        for (var r = 0; r < rowCount; r++)
        {
            _rowStarts[r] = pos;
            if (pos + 4 > span.Length)
                throw new ArgumentException("Truncated length prefix in UTF-8 payload.", nameof(values));
            var len = BitConverter.ToInt32(span.Slice(pos, 4));
            if (len < 0)
                throw new ArgumentOutOfRangeException(nameof(values), "Negative UTF-8 length.");
            pos += 4;
            if (pos + len > span.Length)
                throw new ArgumentException("UTF-8 payload extends past buffer.", nameof(values));
            pos += len;
        }

        _rowStarts[rowCount] = pos;
        if (pos != span.Length)
            throw new ArgumentException("Payload has trailing bytes after last row.", nameof(values));
        RowCount = rowCount;
        Values = values;
        NullBitmap = hasNulls ? nullBitmap[..nb] : ReadOnlyMemory<byte>.Empty;
        HasNulls = hasNulls;
    }

    public RainDbType PhysicalType => RainDbType.Utf8;

    public int RowCount { get; }

    public bool HasNulls { get; }

    public ReadOnlyMemory<byte> NullBitmap { get; }

    public ReadOnlyMemory<byte> Values { get; }

    /// <summary>Byte offset in <see cref="Values"/> where row <paramref name="row"/> payload starts (after 4-byte length).</summary>
    public int GetPayloadStart(int row) => _rowStarts[row] + 4;

    public int GetPayloadLength(int row)
    {
        var a = _rowStarts[row];
        var b = _rowStarts[row + 1];
        return b - a - 4;
    }

    public ReadOnlySpan<byte> GetPayloadSpan(int row)
    {
        var start = GetPayloadStart(row);
        var len = GetPayloadLength(row);
        return Values.Span.Slice(start, len);
    }
}
