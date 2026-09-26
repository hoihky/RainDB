using System.Buffers.Binary;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Query.Vectorized;
using RainDB.Schema;

namespace RainDB.Query.Execution;

internal sealed class GroupKeyFactory
{
    private readonly SelectionEvaluator _selection;

    public GroupKeyFactory(SelectionEvaluator selection) =>
        _selection = selection ?? throw new ArgumentNullException(nameof(selection));

    public GroupKey BuildKey(IColumnarBatch batch, int row, int[] keyIndices, ulong[] scratch)
    {
        uint mask = 0;
        for (var i = 0; i < keyIndices.Length; i++)
        {
            var col = batch.Columns[keyIndices[i]];
            var nb = col.HasNulls ? col.NullBitmap.Span : ReadOnlySpan<byte>.Empty;
            if (_selection.IsNull(nb, row, col.HasNulls))
            {
                mask |= 1u << i;
                scratch[i] = 0;
                continue;
            }

            scratch[i] = PhysicalValueToULong(col, row);
        }

        var owned = new ulong[keyIndices.Length];
        scratch.AsSpan(0, keyIndices.Length).CopyTo(owned);
        return new GroupKey(owned, mask);
    }

    public ulong PhysicalValueToULong(IColumnChunk col, int row)
    {
        var values = col.Values.Span;
        return col.PhysicalType switch
        {
            RainDbType.Int32 => (ulong)(uint)BinaryPrimitives.ReadInt32LittleEndian(values.Slice(row * sizeof(int), sizeof(int))),
            RainDbType.Int64 => (ulong)BinaryPrimitives.ReadInt64LittleEndian(values.Slice(row * sizeof(long), sizeof(long))),
            RainDbType.Float64 => (ulong)BinaryPrimitives.ReadInt64LittleEndian(values.Slice(row * sizeof(double), sizeof(double))),
            RainDbType.Boolean => values[row] != 0 ? 1UL : 0UL,
            _ => throw new InvalidOperationException($"Unexpected key physical type {col.PhysicalType}."),
        };
    }
}
