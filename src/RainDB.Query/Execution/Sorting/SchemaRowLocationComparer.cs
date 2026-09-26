using System.Buffers.Binary;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Query.Plans;
using RainDB.Query.Vectorized;
using RainDB.Schema;

namespace RainDB.Query.Execution.Sorting;

/// <summary>SQL ORDER BY semantics over row locations (nulls first, per-key ASC/DESC).</summary>
internal sealed class SchemaRowLocationComparer : IComparer<RowLocation>
{
    private readonly TableSchema _schema;
    private readonly SortKeyPhysicalSpec[] _keys;
    private readonly IReadOnlyList<IColumnarBatch> _batches;

    public SchemaRowLocationComparer(
        TableSchema schema,
        SortKeyPhysicalSpec[] keys,
        IReadOnlyList<IColumnarBatch> batches)
    {
        _schema = schema;
        _keys = keys;
        _batches = batches;
    }

    public int Compare(RowLocation x, RowLocation y)
    {
        foreach (var spec in _keys)
        {
            var c = CompareAtColumn(spec.ColumnIndex, x, y);
            if (c != 0)
                return spec.Descending ? -c : c;
        }

        return 0;
    }

    private int CompareAtColumn(int colIx, RowLocation a, RowLocation b)
    {
        var colA = _batches[a.BatchIndex].Columns[colIx];
        var colB = _batches[b.BatchIndex].Columns[colIx];
        var t = _schema.Columns[colIx].Type;
        var na = colA.HasNulls && SelectionEvaluator.IsNull(colA.NullBitmap.Span, a.RowIndex, true);
        var nb = colB.HasNulls && SelectionEvaluator.IsNull(colB.NullBitmap.Span, b.RowIndex, true);
        if (na && nb)
            return 0;
        if (na)
            return -1;
        if (nb)
            return 1;

        return t switch
        {
            RainDbType.Utf8 => CompareUtf8(colA, a.RowIndex, colB, b.RowIndex),
            RainDbType.Int32 => ReadI32(colA, a.RowIndex).CompareTo(ReadI32(colB, b.RowIndex)),
            RainDbType.Int64 => ReadI64(colA, a.RowIndex).CompareTo(ReadI64(colB, b.RowIndex)),
            RainDbType.Float64 => ReadF64(colA, a.RowIndex).CompareTo(ReadF64(colB, b.RowIndex)),
            RainDbType.Boolean => ReadBool(colA, a.RowIndex).CompareTo(ReadBool(colB, b.RowIndex)),
            _ => 0,
        };
    }

    private static int CompareUtf8(IColumnChunk ca, int ra, IColumnChunk cb, int rb)
    {
        ReadOnlySpan<byte> sa = ca switch
        {
            Utf8ColumnChunk u => u.Values.Span[u.Offsets.Span[ra]..u.Offsets.Span[ra + 1]],
            Utf8LengthPrefixedColumnChunk lp => lp.GetPayloadSpan(ra),
            _ => throw new InvalidOperationException(),
        };
        ReadOnlySpan<byte> sb = cb switch
        {
            Utf8ColumnChunk u => u.Values.Span[u.Offsets.Span[rb]..u.Offsets.Span[rb + 1]],
            Utf8LengthPrefixedColumnChunk lp => lp.GetPayloadSpan(rb),
            _ => throw new InvalidOperationException(),
        };
        return sa.SequenceCompareTo(sb);
    }

    private static int ReadI32(IColumnChunk c, int row) =>
        BinaryPrimitives.ReadInt32LittleEndian(c.Values.Span.Slice(row * sizeof(int), sizeof(int)));

    private static long ReadI64(IColumnChunk c, int row) =>
        BinaryPrimitives.ReadInt64LittleEndian(c.Values.Span.Slice(row * sizeof(long), sizeof(long)));

    private static double ReadF64(IColumnChunk c, int row) =>
        BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(c.Values.Span.Slice(row * sizeof(double), sizeof(double))));

    private static int ReadBool(IColumnChunk c, int row) => c.Values.Span[row] != 0 ? 1 : 0;
}
