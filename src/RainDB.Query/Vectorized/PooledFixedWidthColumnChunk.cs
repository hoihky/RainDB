using System.Buffers;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Schema;

namespace RainDB.Query.Vectorized;

/// <summary>Fixed-width column chunk whose value/null buffers are rented from <see cref="IMemoryOwner{T}"/> and returned on dispose.</summary>
internal sealed class PooledFixedWidthColumnChunk : IColumnChunk, IDisposable
{
    private IMemoryOwner<byte>? _valuesOwner;
    private IMemoryOwner<byte>? _nullBitmapOwner;
    private int _valueBytes;
    private int _nullBitmapBytes;

    public PooledFixedWidthColumnChunk(
        RainDbType type,
        int rowCount,
        IMemoryOwner<byte> valuesOwner,
        int valueBytes,
        IMemoryOwner<byte>? nullBitmapOwner,
        int nullBitmapBytes,
        bool hasNulls)
    {
        if (!ColumnTypeSizes.IsFixedWidth(type))
            throw new ArgumentException("Use Utf8ColumnChunk for Utf8.", nameof(type));
        if (rowCount < 0)
            throw new ArgumentOutOfRangeException(nameof(rowCount));
        var w = ColumnTypeSizes.FixedWidthBytes(type);
        if (valueBytes != rowCount * w)
            throw new ArgumentException($"Value byte length {valueBytes} != rowCount({rowCount}) * width({w}).", nameof(valueBytes));
        if (hasNulls && nullBitmapOwner is null)
            throw new ArgumentException("Null bitmap owner required when hasNulls is true.", nameof(nullBitmapOwner));
        var nb = ColumnTypeSizes.NullBitmapBytes(rowCount);
        if (hasNulls && nullBitmapBytes < nb)
            throw new ArgumentException("Null bitmap too short for row count.", nameof(nullBitmapBytes));

        PhysicalType = type;
        RowCount = rowCount;
        HasNulls = hasNulls;
        _valuesOwner = valuesOwner;
        _valueBytes = valueBytes;
        _nullBitmapOwner = nullBitmapOwner;
        _nullBitmapBytes = hasNulls ? nb : 0;
    }

    public RainDbType PhysicalType { get; }

    public int RowCount { get; }

    public bool HasNulls { get; }

    public ReadOnlyMemory<byte> NullBitmap
    {
        get
        {
            if (!HasNulls)
                return ReadOnlyMemory<byte>.Empty;
            if (_nullBitmapOwner is null)
                throw new ObjectDisposedException(nameof(PooledFixedWidthColumnChunk));
            return _nullBitmapOwner.Memory[.._nullBitmapBytes];
        }
    }

    public ReadOnlyMemory<byte> Values
    {
        get
        {
            if (_valuesOwner is null)
                throw new ObjectDisposedException(nameof(PooledFixedWidthColumnChunk));
            return _valuesOwner.Memory[.._valueBytes];
        }
    }

    public void Dispose()
    {
        _valuesOwner?.Dispose();
        _valuesOwner = null;
        _nullBitmapOwner?.Dispose();
        _nullBitmapOwner = null;
    }
}
