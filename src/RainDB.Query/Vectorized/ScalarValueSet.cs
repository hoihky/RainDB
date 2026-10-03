using System.Buffers.Binary;
using System.Text;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Execution;
using RainDB.Schema;

namespace RainDB.Query.Vectorized;

/// <summary>Hash set of scalar cell values materialized from a single-column subquery result.</summary>
internal sealed class ScalarValueSet
{
    private readonly RainDbType _type;
    private readonly HashSet<int>? _int32;
    private readonly HashSet<long>? _int64;
    private readonly HashSet<ulong>? _floatBits;
    private readonly HashSet<string>? _utf8;

    private ScalarValueSet(RainDbType type, HashSet<int>? i32, HashSet<long>? i64, HashSet<ulong>? f64, HashSet<string>? utf8)
    {
        _type = type;
        _int32 = i32;
        _int64 = i64;
        _floatBits = f64;
        _utf8 = utf8;
    }

    internal static ScalarValueSet FromSingleColumn(IColumnarQueryResult result, int columnIndex, RainDbType type)
    {
        return type switch
        {
            RainDbType.Int32 => new ScalarValueSet(type, BuildInt32(result, columnIndex), null, null, null),
            RainDbType.Int64 => new ScalarValueSet(type, null, BuildInt64(result, columnIndex), null, null),
            RainDbType.Float64 => new ScalarValueSet(type, null, null, BuildFloat64(result, columnIndex), null),
            RainDbType.Utf8 => new ScalarValueSet(type, null, null, null, BuildUtf8(result, columnIndex)),
            _ => throw new NotSupportedException($"IN subquery column type {type} is not supported."),
        };
    }

    internal bool Contains(IColumnChunk col, int row, SelectionEvaluator selection)
    {
        var nb = col.HasNulls ? col.NullBitmap.Span : ReadOnlySpan<byte>.Empty;
        if (selection.IsNull(nb, row, col.HasNulls))
            return false;

        return _type switch
        {
            RainDbType.Int32 => _int32!.Contains(ReadI32(col, row)),
            RainDbType.Int64 => _int64!.Contains(ReadI64(col, row)),
            RainDbType.Float64 => _floatBits!.Contains(ReadF64Bits(col, row)),
            RainDbType.Utf8 => _utf8!.Contains(ReadUtf8(col, row)),
            _ => false,
        };
    }

    private static HashSet<int> BuildInt32(IColumnarQueryResult result, int col)
    {
        var set = new HashSet<int>();
        foreach (var batch in result.Batches)
        {
            var column = batch.Columns[col];
            var nb = column.HasNulls ? column.NullBitmap.Span : ReadOnlySpan<byte>.Empty;
            for (var r = 0; r < batch.RowCount; r++)
            {
                if ((nb.Length > 0) && ((nb[r >> 3] & (1 << (r & 7))) != 0))
                    continue;
                set.Add(ReadI32(column, r));
            }
        }

        return set;
    }

    private static HashSet<long> BuildInt64(IColumnarQueryResult result, int col)
    {
        var set = new HashSet<long>();
        foreach (var batch in result.Batches)
        {
            var column = batch.Columns[col];
            var nb = column.HasNulls ? column.NullBitmap.Span : ReadOnlySpan<byte>.Empty;
            for (var r = 0; r < batch.RowCount; r++)
            {
                if ((nb.Length > 0) && ((nb[r >> 3] & (1 << (r & 7))) != 0))
                    continue;
                set.Add(ReadI64(column, r));
            }
        }

        return set;
    }

    private static HashSet<ulong> BuildFloat64(IColumnarQueryResult result, int col)
    {
        var set = new HashSet<ulong>();
        foreach (var batch in result.Batches)
        {
            var column = batch.Columns[col];
            var nb = column.HasNulls ? column.NullBitmap.Span : ReadOnlySpan<byte>.Empty;
            for (var r = 0; r < batch.RowCount; r++)
            {
                if ((nb.Length > 0) && ((nb[r >> 3] & (1 << (r & 7))) != 0))
                    continue;
                set.Add(ReadF64Bits(column, r));
            }
        }

        return set;
    }

    private static HashSet<string> BuildUtf8(IColumnarQueryResult result, int col)
    {
        var set = new HashSet<string>(StringComparer.Ordinal);
        foreach (var batch in result.Batches)
        {
            var column = batch.Columns[col];
            var nb = column.HasNulls ? column.NullBitmap.Span : ReadOnlySpan<byte>.Empty;
            for (var r = 0; r < batch.RowCount; r++)
            {
                if ((nb.Length > 0) && ((nb[r >> 3] & (1 << (r & 7))) != 0))
                    continue;
                set.Add(ReadUtf8(column, r));
            }
        }

        return set;
    }

    private static int ReadI32(IColumnChunk c, int row) =>
        BinaryPrimitives.ReadInt32LittleEndian(c.Values.Span.Slice(row * sizeof(int), sizeof(int)));

    private static long ReadI64(IColumnChunk c, int row) =>
        BinaryPrimitives.ReadInt64LittleEndian(c.Values.Span.Slice(row * sizeof(long), sizeof(long)));

    private static ulong ReadF64Bits(IColumnChunk c, int row) =>
        BinaryPrimitives.ReadUInt64LittleEndian(c.Values.Span.Slice(row * sizeof(double), sizeof(double)));

    private static string ReadUtf8(IColumnChunk col, int row) =>
        col switch
        {
            Utf8ColumnChunk u => Encoding.UTF8.GetString(u.Values.Span[u.Offsets.Span[row]..u.Offsets.Span[row + 1]]),
            Utf8LengthPrefixedColumnChunk lp => Encoding.UTF8.GetString(lp.GetPayloadSpan(row)),
            _ => throw new NotSupportedException(),
        };
}
