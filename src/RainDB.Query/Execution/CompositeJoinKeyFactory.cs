using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Query.Vectorized;
using RainDB.Schema;

namespace RainDB.Query.Execution;

internal sealed class CompositeJoinKeyFactory
{
    private readonly SelectionEvaluator _selection;
    private readonly GroupKeyFactory _groupKeys;

    public CompositeJoinKeyFactory(SelectionEvaluator selection, GroupKeyFactory? groupKeys = null)
    {
        _selection = selection ?? throw new ArgumentNullException(nameof(selection));
        _groupKeys = groupKeys ?? new GroupKeyFactory(selection);
    }

    public CompositeJoinKey Build(TableSchema schema, IColumnarBatch batch, int row, int[] keyIndices)
    {
        var n = keyIndices.Length;
        var numeric = new ulong[n];
        byte[]?[] utf8Payloads = new byte[n][];
        uint mask = 0;
        for (var i = 0; i < n; i++)
        {
            var colIx = keyIndices[i];
            var col = batch.Columns[colIx];
            var typ = schema.Columns[colIx].Type;
            var nb = col.HasNulls ? col.NullBitmap.Span : ReadOnlySpan<byte>.Empty;
            if (_selection.IsNull(nb, row, col.HasNulls))
            {
                mask |= 1u << i;
                numeric[i] = 0;
                utf8Payloads[i] = null;
                continue;
            }

            if (typ == RainDbType.Utf8)
            {
                numeric[i] = 0;
                utf8Payloads[i] = CopyUtf8Payload(col, row);
            }
            else
            {
                utf8Payloads[i] = null;
                numeric[i] = _groupKeys.PhysicalValueToULong(col, row);
            }
        }

        return new CompositeJoinKey(mask, numeric, utf8Payloads);
    }

    private static byte[] CopyUtf8Payload(IColumnChunk col, int row) =>
        col switch
        {
            Utf8ColumnChunk utf8 =>
                utf8.Values.Span[utf8.Offsets.Span[row]..utf8.Offsets.Span[row + 1]].ToArray(),
            Utf8LengthPrefixedColumnChunk lp => lp.GetPayloadSpan(row).ToArray(),
            _ => throw new InvalidOperationException($"Unexpected UTF-8 physical chunk {col.GetType().Name}."),
        };
}
