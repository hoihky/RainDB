using System.Buffers.Binary;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Schema;

namespace RainDB.Core.Persistence;

/// <summary>Binary batch format v1 (little-endian). Supports <see cref="FixedWidthColumnChunk"/>, <see cref="Utf8ColumnChunk"/>, and <see cref="Utf8LengthPrefixedColumnChunk"/>.</summary>
public static class RainDbBatchBinaryCodec
{
    private const uint FormatVersion = 1;

    private static ReadOnlySpan<byte> Magic => "RNBATCH1"u8;

    private const byte KindFixed = 1;
    private const byte KindUtf8Arrow = 2;
    private const byte KindUtf8LengthPrefixed = 3;
    private const byte KindDictInt32 = 4;

    private static readonly Int32DictionaryColumnEncoder DictionaryEncoder = new();

    public static void WriteBatch(Stream destination, IColumnarBatch batch, RainDbBatchCodecOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(destination);
        ArgumentNullException.ThrowIfNull(batch);
        destination.Write(Magic);
        WriteU32(destination, FormatVersion);
        WriteI32(destination, batch.RowCount);
        WriteI32(destination, batch.Columns.Count);
        for (var i = 0; i < batch.Columns.Count; i++)
            WriteColumn(destination, batch.Columns[i], batch.RowCount, options);
    }

    public static byte[] EncodeBatch(IColumnarBatch batch, RainDbBatchCodecOptions? options = null)
    {
        using var ms = new MemoryStream();
        WriteBatch(ms, batch, options);
        return ms.ToArray();
    }

    public static ColumnarBatch DecodeBatch(ReadOnlySpan<byte> data)
    {
        if (data.Length < Magic.Length + 4 + 4 + 4)
            throw new InvalidDataException("Batch buffer too small for header.");
        if (!data[..Magic.Length].SequenceEqual(Magic))
            throw new InvalidDataException("Unrecognized batch magic.");
        var o = Magic.Length;
        var version = ReadU32(data, ref o);
        if (version != FormatVersion)
            throw new InvalidDataException($"Unsupported batch format version {version}.");
        var rowCount = ReadI32(data, ref o);
        var colCount = ReadI32(data, ref o);
        if (rowCount < 0 || colCount < 0)
            throw new InvalidDataException("Invalid row or column count.");
        var cols = new IColumnChunk[colCount];
        for (var i = 0; i < colCount; i++)
            cols[i] = ReadColumn(data, ref o, rowCount);
        if (o != data.Length)
            throw new InvalidDataException($"Batch buffer has {data.Length - o} trailing byte(s) after column data.");
        return new ColumnarBatch(rowCount, cols);
    }

    private static void WriteColumn(Stream s, IColumnChunk chunk, int batchRowCount, RainDbBatchCodecOptions? options)
    {
        if (chunk.RowCount != batchRowCount)
            throw new ArgumentException("Column row count does not match batch row count.", nameof(chunk));
        if (chunk is FixedWidthColumnChunk fwTry
            && fwTry.PhysicalType == RainDbType.Int32
            && options?.EnableInt32DictionaryEncoding == true
            && DictionaryEncoder.TryEncode(fwTry, out var encoded)
            && encoded is not null)
        {
            WriteDictionaryInt32(s, encoded);
            return;
        }

        if (chunk is DictionaryEncodedInt32ColumnChunk dict)
        {
            WriteDictionaryInt32(s, dict);
            return;
        }

        switch (chunk)
        {
            case FixedWidthColumnChunk fw:
                s.WriteByte(KindFixed);
                s.WriteByte((byte)fw.PhysicalType);
                s.WriteByte((byte)(fw.HasNulls ? 1 : 0));
                WriteI32(s, fw.RowCount);
                WriteI32(s, fw.Values.Length);
                s.Write(fw.Values.Span);
                WriteNullBitmap(s, fw);
                return;
            case Utf8ColumnChunk utf8:
                s.WriteByte(KindUtf8Arrow);
                s.WriteByte((byte)(utf8.HasNulls ? 1 : 0));
                WriteI32(s, utf8.RowCount);
                WriteI32(s, utf8.Offsets.Length);
                foreach (var off in utf8.Offsets.Span)
                    WriteI32(s, off);
                WriteI32(s, utf8.Values.Length);
                s.Write(utf8.Values.Span);
                WriteNullBitmap(s, utf8);
                return;
            case Utf8LengthPrefixedColumnChunk lp:
                s.WriteByte(KindUtf8LengthPrefixed);
                s.WriteByte((byte)(lp.HasNulls ? 1 : 0));
                WriteI32(s, lp.RowCount);
                WriteI32(s, lp.Values.Length);
                s.Write(lp.Values.Span);
                WriteNullBitmap(s, lp);
                return;
            default:
                throw new NotSupportedException($"Column chunk type {chunk.GetType().Name} is not supported for persistence.");
        }
    }

    private static void WriteDictionaryInt32(Stream s, DictionaryEncodedInt32ColumnChunk col)
    {
        s.WriteByte(KindDictInt32);
        s.WriteByte((byte)(col.HasNulls ? 1 : 0));
        WriteI32(s, col.RowCount);
        var dict = col.Dictionary.Span;
        WriteI32(s, dict.Length);
        foreach (var v in dict)
            WriteI32(s, v);
        s.WriteByte(col.IndexWidthBytes);
        WriteI32(s, col.Indices.Length);
        s.Write(col.Indices.Span);
        WriteNullBitmap(s, col);
    }

    private static void WriteNullBitmap(Stream s, IColumnChunk c)
    {
        if (!c.HasNulls)
            return;
        var nb = ColumnTypeSizes.NullBitmapBytes(c.RowCount);
        s.Write(c.NullBitmap.Span[..nb]);
    }

    private static IColumnChunk ReadColumn(ReadOnlySpan<byte> data, ref int o, int batchRowCount)
    {
        var kind = ReadByte(data, ref o);
        return kind switch
        {
            KindFixed => ReadFixed(data, ref o, batchRowCount),
            KindUtf8Arrow => ReadUtf8Arrow(data, ref o, batchRowCount),
            KindUtf8LengthPrefixed => ReadUtf8Lp(data, ref o, batchRowCount),
            KindDictInt32 => ReadDictionaryInt32(data, ref o, batchRowCount),
            _ => throw new InvalidDataException($"Unknown column kind {kind}."),
        };
    }

    private static IColumnChunk ReadDictionaryInt32(ReadOnlySpan<byte> data, ref int o, int batchRowCount)
    {
        var hasNulls = ReadByte(data, ref o) != 0;
        var rowCount = ReadI32(data, ref o);
        if (rowCount != batchRowCount)
            throw new InvalidDataException("Column row count mismatch.");
        var dictLen = ReadI32(data, ref o);
        if (dictLen < 0)
            throw new InvalidDataException("Invalid dictionary length.");
        var dictionary = new int[dictLen];
        for (var i = 0; i < dictLen; i++)
            dictionary[i] = ReadI32(data, ref o);
        var indexWidth = ReadByte(data, ref o);
        if (indexWidth is not (1 or 2 or 4))
            throw new InvalidDataException("Invalid dictionary index width.");
        var indicesLen = ReadI32(data, ref o);
        var indices = ReadBytes(data, ref o, indicesLen);
        ReadOnlyMemory<byte> nbMem = ReadOnlyMemory<byte>.Empty;
        if (hasNulls)
        {
            var nb = ColumnTypeSizes.NullBitmapBytes(rowCount);
            nbMem = ReadBytes(data, ref o, nb);
        }

        return new DictionaryEncodedInt32ColumnChunk(
            rowCount,
            dictionary,
            indices,
            indexWidth,
            nbMem,
            hasNulls);
    }

    private static IColumnChunk ReadFixed(ReadOnlySpan<byte> data, ref int o, int batchRowCount)
    {
        var phys = (RainDbType)ReadByte(data, ref o);
        var hasNulls = ReadByte(data, ref o) != 0;
        var rowCount = ReadI32(data, ref o);
        if (rowCount != batchRowCount)
            throw new InvalidDataException("Column row count mismatch.");
        var valuesLen = ReadI32(data, ref o);
        var values = ReadBytes(data, ref o, valuesLen);
        ReadOnlyMemory<byte> nbMem = ReadOnlyMemory<byte>.Empty;
        if (hasNulls)
        {
            var nb = ColumnTypeSizes.NullBitmapBytes(rowCount);
            nbMem = ReadBytes(data, ref o, nb);
        }

        return new FixedWidthColumnChunk(phys, rowCount, values, nbMem, hasNulls);
    }

    private static IColumnChunk ReadUtf8Arrow(ReadOnlySpan<byte> data, ref int o, int batchRowCount)
    {
        var hasNulls = ReadByte(data, ref o) != 0;
        var rowCount = ReadI32(data, ref o);
        if (rowCount != batchRowCount)
            throw new InvalidDataException("Column row count mismatch.");
        var offLen = ReadI32(data, ref o);
        if (offLen != rowCount + 1)
            throw new InvalidDataException("Invalid UTF-8 offsets length.");
        var offsets = new int[offLen];
        for (var i = 0; i < offLen; i++)
            offsets[i] = ReadI32(data, ref o);
        var blobLen = ReadI32(data, ref o);
        var blob = ReadBytes(data, ref o, blobLen);
        ReadOnlyMemory<byte> nbMem = ReadOnlyMemory<byte>.Empty;
        if (hasNulls)
        {
            var nb = ColumnTypeSizes.NullBitmapBytes(rowCount);
            nbMem = ReadBytes(data, ref o, nb);
        }

        return new Utf8ColumnChunk(rowCount, offsets, blob, nbMem, hasNulls);
    }

    private static IColumnChunk ReadUtf8Lp(ReadOnlySpan<byte> data, ref int o, int batchRowCount)
    {
        var hasNulls = ReadByte(data, ref o) != 0;
        var rowCount = ReadI32(data, ref o);
        if (rowCount != batchRowCount)
            throw new InvalidDataException("Column row count mismatch.");
        var payloadLen = ReadI32(data, ref o);
        var payload = ReadBytes(data, ref o, payloadLen);
        ReadOnlyMemory<byte> nbMem = ReadOnlyMemory<byte>.Empty;
        if (hasNulls)
        {
            var nb = ColumnTypeSizes.NullBitmapBytes(rowCount);
            nbMem = ReadBytes(data, ref o, nb);
        }

        return new Utf8LengthPrefixedColumnChunk(rowCount, payload, nbMem, hasNulls);
    }

    private static ReadOnlyMemory<byte> ReadBytes(ReadOnlySpan<byte> data, ref int o, int len)
    {
        if (len < 0 || o + len > data.Length)
            throw new InvalidDataException("Unexpected end of batch buffer.");
        var slice = data.Slice(o, len);
        o += len;
        return slice.ToArray();
    }

    private static byte ReadByte(ReadOnlySpan<byte> data, ref int o)
    {
        if (o >= data.Length)
            throw new InvalidDataException("Unexpected end of batch buffer.");
        return data[o++];
    }

    private static int ReadI32(ReadOnlySpan<byte> data, ref int o)
    {
        if (o + sizeof(int) > data.Length)
            throw new InvalidDataException("Unexpected end of batch buffer.");
        var v = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(o, sizeof(int)));
        o += sizeof(int);
        return v;
    }

    private static uint ReadU32(ReadOnlySpan<byte> data, ref int o)
    {
        if (o + sizeof(uint) > data.Length)
            throw new InvalidDataException("Unexpected end of batch buffer.");
        var v = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(o, sizeof(uint)));
        o += sizeof(uint);
        return v;
    }

    private static void WriteI32(Stream s, int v)
    {
        Span<byte> tmp = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(tmp, v);
        s.Write(tmp);
    }

    private static void WriteU32(Stream s, uint v)
    {
        Span<byte> tmp = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(tmp, v);
        s.Write(tmp);
    }
}
