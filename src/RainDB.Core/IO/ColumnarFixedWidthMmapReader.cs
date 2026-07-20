using System.Buffers;
using System.Buffers.Binary;
using System.IO.MemoryMappedFiles;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Schema;

namespace RainDB.Core.IO;

/// <summary>Memory-maps a <see cref="ColumnarFixedWidthFileFormat"/> file and exposes an <see cref="IColumnChunk"/> backed by spans over the map.</summary>
public sealed class ColumnarFixedWidthMmapReader : IDisposable
{
    private readonly MemoryMappedFile _mmf;
    private readonly MemoryMappedViewAccessor _accessor;
    private readonly MmapBytesMemoryManager _manager;
    private readonly MappedFixedWidthColumnChunk _chunk;

    private ColumnarFixedWidthMmapReader(
        MemoryMappedFile mmf,
        MemoryMappedViewAccessor accessor,
        MmapBytesMemoryManager manager,
        MappedFixedWidthColumnChunk chunk)
    {
        _mmf = mmf;
        _accessor = accessor;
        _manager = manager;
        _chunk = chunk;
    }

    public IColumnChunk Chunk => _chunk;

    public static ColumnarFixedWidthMmapReader Open(string path)
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
                throw new NotSupportedException("Mapped column file exceeds int.MaxValue bytes.");
            var manager = new MmapBytesMemoryManager(accessor, checked((int)capacity));
            var bytes = manager.Memory;
            var span = bytes.Span;
            if (span.Length < ColumnarFixedWidthFileFormat.HeaderSizeBytes)
                throw new InvalidDataException("Truncated column file header.");

            if (!span.StartsWith(ColumnarFixedWidthFileFormat.Magic))
                throw new InvalidDataException("Unexpected column file magic.");

            var version = BinaryPrimitives.ReadUInt32LittleEndian(span.Slice(8, 4));
            if (version != 1)
                throw new InvalidDataException($"Unsupported column file version {version}.");

            var rowCount = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(12, 4));
            if (rowCount < 0)
                throw new InvalidDataException("Invalid row count.");

            var type = (RainDbType)span[16];
            if (!ColumnTypeSizes.IsFixedWidth(type))
                throw new InvalidDataException("Mapped column type must be fixed-width.");

            var flags = span[17];
            var hasNulls = (flags & 1) != 0;
            var nullBytes = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(20, 4));
            var valuesBytes = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(24, 4));
            var w = ColumnTypeSizes.FixedWidthBytes(type);
            if (valuesBytes != checked(rowCount * w))
                throw new InvalidDataException("Values byte length does not match row count and type width.");

            var expectedNull = hasNulls ? ColumnTypeSizes.NullBitmapBytes(rowCount) : 0;
            if (nullBytes != expectedNull)
                throw new InvalidDataException("Null bitmap size does not match header flags / row count.");

            var payloadStart = ColumnarFixedWidthFileFormat.HeaderSizeBytes;
            var total = payloadStart + nullBytes + valuesBytes;
            if (span.Length < total)
                throw new InvalidDataException("Column file truncated.");

            ReadOnlyMemory<byte> nullMem;
            ReadOnlyMemory<byte> valuesMem;
            if (nullBytes == 0)
            {
                nullMem = ReadOnlyMemory<byte>.Empty;
                valuesMem = bytes.Slice(payloadStart, valuesBytes);
            }
            else
            {
                nullMem = bytes.Slice(payloadStart, nullBytes);
                valuesMem = bytes.Slice(payloadStart + nullBytes, valuesBytes);
            }

            var chunk = new MappedFixedWidthColumnChunk(type, rowCount, hasNulls, nullMem, valuesMem);
            return new ColumnarFixedWidthMmapReader(mmf, accessor, manager, chunk);
        }
        catch
        {
            accessor.Dispose();
            mmf.Dispose();
            throw;
        }
    }

    public void Dispose()
    {
        _manager.ReleasePointer();
        _accessor.Dispose();
        _mmf.Dispose();
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

    private sealed class MappedFixedWidthColumnChunk : IColumnChunk
    {
        public MappedFixedWidthColumnChunk(
            RainDbType type,
            int rowCount,
            bool hasNulls,
            ReadOnlyMemory<byte> nullBitmap,
            ReadOnlyMemory<byte> values)
        {
            PhysicalType = type;
            RowCount = rowCount;
            HasNulls = hasNulls;
            NullBitmap = nullBitmap;
            Values = values;
        }

        public RainDbType PhysicalType { get; }

        public int RowCount { get; }

        public bool HasNulls { get; }

        public ReadOnlyMemory<byte> NullBitmap { get; }

        public ReadOnlyMemory<byte> Values { get; }
    }
}
