using RainDB.Schema;

namespace RainDB.Columnar;

/// <summary>One column vector for a contiguous row range (OLAP chunk; LSP: variable-width via dedicated implementations).</summary>
public interface IColumnChunk
{
    RainDbType PhysicalType { get; }

    int RowCount { get; }

    /// <summary>When false, all rows are non-null and <see cref="NullBitmap"/> may be empty.</summary>
    bool HasNulls { get; }

    /// <summary>Packed bits: 1 = null, row <c>i</c> uses bit <c>(i >> 3)</c> mask <c>1 << (i &amp; 7)</c>.</summary>
    ReadOnlyMemory<byte> NullBitmap { get; }

    /// <summary>Fixed-width packed values, or UTF-8 payload for <see cref="RainDbType.Utf8"/> (see chunk implementation).</summary>
    ReadOnlyMemory<byte> Values { get; }
}
