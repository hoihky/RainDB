using System.Buffers.Binary;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Schema;

namespace RainDB.Core.IO;

/// <summary>On-disk layout for a single fixed-width <see cref="IColumnChunk"/> (P1 zero-copy mmap).</summary>
public static class ColumnarFixedWidthFileFormat
{
    public const int HeaderSizeBytes = 32;

    /// <summary>8-byte magic + versioned payload.</summary>
    public static ReadOnlySpan<byte> Magic => "RNBFCOL1"u8;

    public static void WriteFile(string path, IColumnChunk chunk)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        ArgumentNullException.ThrowIfNull(chunk);
        if (!ColumnTypeSizes.IsFixedWidth(chunk.PhysicalType))
            throw new ArgumentException("Only fixed-width columns can be written with this format.", nameof(chunk));

        var rowCount = chunk.RowCount;
        var w = ColumnTypeSizes.FixedWidthBytes(chunk.PhysicalType);
        var nullBytes = chunk.HasNulls ? ColumnTypeSizes.NullBitmapBytes(rowCount) : 0;
        var valuesBytes = rowCount * w;
        var header = new byte[HeaderSizeBytes];
        Magic.CopyTo(header);
        BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(8), 1);
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(12), rowCount);
        header[16] = (byte)chunk.PhysicalType;
        header[17] = (byte)(chunk.HasNulls ? 1 : 0);
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(20), nullBytes);
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(24), valuesBytes);

        using var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None);
        fs.Write(header);
        if (nullBytes > 0)
            fs.Write(chunk.NullBitmap.Span[..nullBytes]);
        fs.Write(chunk.Values.Span[..valuesBytes]);
    }
}
