using System.Buffers.Binary;
using RainDB.Columnar;
using RainDB.Schema;

namespace RainDB.Core.Columnar;

/// <summary>Int32 column stored as dictionary + per-row indices; materializes fixed-width values on first <see cref="Values"/> access.</summary>
public sealed class DictionaryEncodedInt32ColumnChunk : IColumnChunk
{
    private readonly ReadOnlyMemory<int> _dictionary;
    private readonly ReadOnlyMemory<byte> _indices;
    private readonly byte _indexWidthBytes;
    private ReadOnlyMemory<byte> _materializedValues;

    public DictionaryEncodedInt32ColumnChunk(
        int rowCount,
        ReadOnlyMemory<int> dictionary,
        ReadOnlyMemory<byte> indices,
        byte indexWidthBytes,
        ReadOnlyMemory<byte> nullBitmap,
        bool hasNulls)
    {
        if (indexWidthBytes is not (1 or 2 or 4))
            throw new ArgumentOutOfRangeException(nameof(indexWidthBytes));
        if (indices.Length != checked(rowCount * indexWidthBytes))
            throw new ArgumentException("Index payload length does not match row count.", nameof(indices));
        RowCount = rowCount;
        _dictionary = dictionary;
        _indices = indices;
        _indexWidthBytes = indexWidthBytes;
        NullBitmap = nullBitmap;
        HasNulls = hasNulls;
    }

    public RainDbType PhysicalType => RainDbType.Int32;

    public int RowCount { get; }

    public bool HasNulls { get; }

    public ReadOnlyMemory<byte> NullBitmap { get; }

    public ReadOnlyMemory<byte> Values
    {
        get
        {
            if (_materializedValues.IsEmpty)
                _materializedValues = MaterializeValues();
            return _materializedValues;
        }
    }

    public ReadOnlyMemory<int> Dictionary => _dictionary;

    public ReadOnlyMemory<byte> Indices => _indices;

    public byte IndexWidthBytes => _indexWidthBytes;

    public FixedWidthColumnChunk Materialize()
    {
        var values = MaterializeValues();
        return new FixedWidthColumnChunk(PhysicalType, RowCount, values, NullBitmap, HasNulls);
    }

    private ReadOnlyMemory<byte> MaterializeValues()
    {
        var bytes = GC.AllocateUninitializedArray<byte>(checked(RowCount * sizeof(int)));
        var dict = _dictionary.Span;
        var idx = _indices.Span;
        for (var row = 0; row < RowCount; row++)
        {
            var ord = ReadIndex(idx, row, _indexWidthBytes);
            if ((uint)ord >= (uint)dict.Length)
                throw new InvalidDataException("Dictionary index out of range.");
            BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(row * sizeof(int), sizeof(int)), dict[ord]);
        }

        return bytes;
    }

    internal static int ReadIndex(ReadOnlySpan<byte> indices, int row, byte indexWidthBytes) =>
        indexWidthBytes switch
        {
            1 => indices[row],
            2 => BinaryPrimitives.ReadUInt16LittleEndian(indices.Slice(row * 2, 2)),
            4 => checked((int)BinaryPrimitives.ReadUInt32LittleEndian(indices.Slice(row * 4, 4))),
            _ => throw new InvalidOperationException(),
        };
}
