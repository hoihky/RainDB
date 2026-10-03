using System.Buffers;
using System.Buffers.Binary;
using System.IO.MemoryMappedFiles;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Schema;

namespace RainDB.Core.Persistence;

/// <summary>Maps a v1 <c>.batch</c> file and exposes column payloads without copying fixed-width value buffers.</summary>
public sealed class RainDbBatchMmapReader
{
    private static ReadOnlySpan<byte> Magic => "RNBATCH1"u8;

    public MappedColumnarBatch Open(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        var mmf = MemoryMappedFile.CreateFromFile(path, FileMode.Open, null, 0, MemoryMappedFileAccess.Read);
        MemoryMappedViewAccessor accessor;
        try
        {
            accessor = mmf.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read);
        }
        catch
        {
            mmf.Dispose();
            throw;
        }

        try
        {
            var capacity = accessor.Capacity;
            if (capacity > int.MaxValue)
                throw new NotSupportedException("Batch file exceeds int.MaxValue bytes.");
            var manager = new MmapBytesMemoryManager(accessor, checked((int)capacity));
            var bytes = manager.Memory;
            var batch = DecodeMappedBatch(bytes);
            var lifetime = new MmapLifetime(mmf, accessor, manager);
            return new MappedColumnarBatch(batch, lifetime);
        }
        catch
        {
            accessor.Dispose();
            mmf.Dispose();
            throw;
        }
    }

    private static ColumnarBatch DecodeMappedBatch(ReadOnlyMemory<byte> data)
    {
        var span = data.Span;
        if (span.Length < Magic.Length + 12)
            throw new InvalidDataException("Batch buffer too small for header.");
        if (!span[..Magic.Length].SequenceEqual(Magic))
            throw new InvalidDataException("Unrecognized batch magic.");
        var o = Magic.Length;
        var version = ReadU32(span, ref o);
        if (version != 1)
            throw new InvalidDataException($"Unsupported batch format version {version}.");
        var rowCount = ReadI32(span, ref o);
        var colCount = ReadI32(span, ref o);
        if (rowCount < 0 || colCount < 0)
            throw new InvalidDataException("Invalid row or column count.");
        var cols = new IColumnChunk[colCount];
        for (var i = 0; i < colCount; i++)
            cols[i] = ReadMappedColumn(data, ref o, rowCount);
        if (o > span.Length)
            throw new InvalidDataException("Batch column parsing overrun.");
        return new ColumnarBatch(rowCount, cols);
    }

    private static IColumnChunk ReadMappedColumn(ReadOnlyMemory<byte> data, ref int o, int batchRowCount)
    {
        var span = data.Span;
        var kind = ReadByte(span, ref o);
        return kind switch
        {
            1 => ReadMappedFixed(data, ref o, batchRowCount),
            2 => ReadUtf8ArrowCopy(data.Span, ref o, batchRowCount),
            3 => ReadUtf8LpCopy(data.Span, ref o, batchRowCount),
            4 => ReadDictionaryInt32Copy(data.Span, ref o, batchRowCount),
            _ => throw new InvalidDataException($"Unknown column kind {kind}."),
        };
    }

    private static IColumnChunk ReadMappedFixed(ReadOnlyMemory<byte> data, ref int o, int batchRowCount)
    {
        var span = data.Span;
        var phys = (RainDbType)ReadByte(span, ref o);
        var hasNulls = ReadByte(span, ref o) != 0;
        var rowCount = ReadI32(span, ref o);
        if (rowCount != batchRowCount)
            throw new InvalidDataException("Column row count mismatch.");
        var valuesLen = ReadI32(span, ref o);
        var valuesMem = data.Slice(o, valuesLen);
        o += valuesLen;
        ReadOnlyMemory<byte> nbMem = ReadOnlyMemory<byte>.Empty;
        if (hasNulls)
        {
            var nb = ColumnTypeSizes.NullBitmapBytes(rowCount);
            nbMem = data.Slice(o, nb);
            o += nb;
        }

        return new FixedWidthColumnChunk(phys, rowCount, valuesMem, nbMem, hasNulls);
    }

    private static IColumnChunk ReadUtf8ArrowCopy(ReadOnlySpan<byte> data, ref int o, int batchRowCount)
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
        var blob = data.Slice(o, blobLen).ToArray();
        o += blobLen;
        ReadOnlyMemory<byte> nbMem = ReadOnlyMemory<byte>.Empty;
        if (hasNulls)
        {
            var nb = ColumnTypeSizes.NullBitmapBytes(rowCount);
            nbMem = data.Slice(o, nb).ToArray();
            o += nb;
        }

        return new Utf8ColumnChunk(rowCount, offsets, blob, nbMem, hasNulls);
    }

    private static IColumnChunk ReadDictionaryInt32Copy(ReadOnlySpan<byte> data, ref int o, int batchRowCount)
    {
        var hasNulls = ReadByte(data, ref o) != 0;
        var rowCount = ReadI32(data, ref o);
        if (rowCount != batchRowCount)
            throw new InvalidDataException("Column row count mismatch.");
        var dictLen = ReadI32(data, ref o);
        var dictionary = new int[dictLen];
        for (var i = 0; i < dictLen; i++)
            dictionary[i] = ReadI32(data, ref o);
        var indexWidth = ReadByte(data, ref o);
        var indicesLen = ReadI32(data, ref o);
        var indices = data.Slice(o, indicesLen).ToArray();
        o += indicesLen;
        ReadOnlyMemory<byte> nbMem = ReadOnlyMemory<byte>.Empty;
        if (hasNulls)
        {
            var nb = ColumnTypeSizes.NullBitmapBytes(rowCount);
            nbMem = data.Slice(o, nb).ToArray();
            o += nb;
        }

        return new DictionaryEncodedInt32ColumnChunk(rowCount, dictionary, indices, indexWidth, nbMem, hasNulls);
    }

    private static IColumnChunk ReadUtf8LpCopy(ReadOnlySpan<byte> data, ref int o, int batchRowCount)
    {
        var hasNulls = ReadByte(data, ref o) != 0;
        var rowCount = ReadI32(data, ref o);
        if (rowCount != batchRowCount)
            throw new InvalidDataException("Column row count mismatch.");
        var payloadLen = ReadI32(data, ref o);
        var payload = data.Slice(o, payloadLen).ToArray();
        o += payloadLen;
        ReadOnlyMemory<byte> nbMem = ReadOnlyMemory<byte>.Empty;
        if (hasNulls)
        {
            var nb = ColumnTypeSizes.NullBitmapBytes(rowCount);
            nbMem = data.Slice(o, nb).ToArray();
            o += nb;
        }

        return new Utf8LengthPrefixedColumnChunk(rowCount, payload, nbMem, hasNulls);
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

    private sealed class MmapLifetime : IDisposable
    {
        private readonly MemoryMappedFile _mmf;
        private readonly MemoryMappedViewAccessor _accessor;
        private readonly MmapBytesMemoryManager _manager;

        public MmapLifetime(MemoryMappedFile mmf, MemoryMappedViewAccessor accessor, MmapBytesMemoryManager manager)
        {
            _mmf = mmf;
            _accessor = accessor;
            _manager = manager;
        }

        public void Dispose()
        {
            _manager.ReleasePointer();
            _accessor.Dispose();
            _mmf.Dispose();
        }
    }

    private sealed unsafe class MmapBytesMemoryManager : MemoryManager<byte>
    {
        private readonly MemoryMappedViewAccessor _accessor;
        private byte* _pointer;
        private readonly int _length;

        public MmapBytesMemoryManager(MemoryMappedViewAccessor accessor, int length)
        {
            _accessor = accessor;
            _length = length;
            _pointer = null;
            accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref _pointer);
            if (_pointer == null)
                throw new InvalidOperationException("Failed to acquire memory-mapped pointer.");
        }

        public override Span<byte> GetSpan() => new(_pointer, _length);

        public override MemoryHandle Pin(int elementIndex = 0) => new(_pointer + elementIndex);

        public override void Unpin() { }

        protected override bool TryGetArray(out ArraySegment<byte> segment)
        {
            segment = default;
            return false;
        }

        public void ReleasePointer()
        {
            if (_pointer != null)
            {
                _accessor.SafeMemoryMappedViewHandle.ReleasePointer();
                _pointer = null;
            }
        }

        protected override void Dispose(bool disposing) => ReleasePointer();
    }
}
