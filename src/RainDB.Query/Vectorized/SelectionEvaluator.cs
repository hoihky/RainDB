using System.Buffers.Binary;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Execution;
using RainDB.Query.Plans;
using RainDB.Schema;

namespace RainDB.Query.Vectorized;

internal static class SelectionEvaluator
{
    internal static bool IsNull(ReadOnlySpan<byte> nullBitmap, int row, bool hasNulls)
    {
        if (!hasNulls)
            return false;
        return (nullBitmap[row >> 3] & (1 << (row & 7))) != 0;
    }

    /// <summary>Writes matching row indices; returns count.</summary>
    internal static int FillSelectedRowsConjunctive(IColumnarBatch batch, ReadOnlySpan<ColumnCompareFilter> filters, Span<int> dest)
    {
        var n = batch.RowCount;
        if (filters.Length == 0)
        {
            if (dest.Length < n)
                throw new ArgumentException(nameof(dest));
            for (var i = 0; i < n; i++)
                dest[i] = i;
            return n;
        }

        var count = 0;
        for (var r = 0; r < n; r++)
        {
            var ok = true;
            foreach (var f in filters)
            {
                if (!RowMatchesFilter(batch.Columns[f.ColumnIndex], f, r))
                {
                    ok = false;
                    break;
                }
            }

            if (ok)
                dest[count++] = r;
        }

        return count;
    }

    /// <summary>Writes 0..rowCount-1 row indices that pass <paramref name="filter"/> into <paramref name="dest"/>; returns match count.</summary>
    internal static int FillSelectedRows(IColumnChunk column, ColumnCompareFilter filter, Span<int> dest)
    {
        if (filter.ColumnIndex < 0)
            throw new ArgumentOutOfRangeException(nameof(filter));

        var n = column.RowCount;
        if (dest.Length < n)
            throw new ArgumentException("Selection buffer too small.", nameof(dest));

        if (filter.Utf8LiteralBytes is not null)
        {
            if (column.PhysicalType != RainDbType.Utf8)
                throw new ArgumentException("UTF-8 literal filter requires a UTF-8 column.", nameof(column));
            return FillUtf8Selected(column, filter, dest);
        }

        if (column.PhysicalType == RainDbType.Utf8)
            throw new NotSupportedException("UTF-8 column requires a string literal predicate.");

        var nb = column.HasNulls ? column.NullBitmap.Span : ReadOnlySpan<byte>.Empty;
        var values = column.Values.Span;
        var count = 0;
        switch (column.PhysicalType)
        {
            case RainDbType.Int32:
                FillInt32(values, nb, column.HasNulls, filter.Op, (int)filter.ImmediateBits, dest, ref count);
                break;
            case RainDbType.Int64:
                FillInt64(values, nb, column.HasNulls, filter.Op, filter.ImmediateBits, dest, ref count);
                break;
            case RainDbType.Float64:
                FillFloat64(values, nb, column.HasNulls, filter.Op, BitConverter.Int64BitsToDouble(filter.ImmediateBits), dest, ref count);
                break;
            case RainDbType.Boolean:
                FillBool(values, nb, column.HasNulls, filter.Op, filter.ImmediateBits != 0, dest, ref count);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(column), column.PhysicalType, "Unsupported physical type.");
        }

        return count;
    }

    internal static bool RowMatchesFilter(IColumnChunk column, ColumnCompareFilter filter, int row)
    {
        if (filter.Utf8LiteralBytes is { } lit)
        {
            if (column.PhysicalType != RainDbType.Utf8)
                return false;
            var nb = column.HasNulls ? column.NullBitmap.Span : ReadOnlySpan<byte>.Empty;
            if (IsNull(nb, row, column.HasNulls))
                return false;
            var eq = Utf8PayloadEquals(column, row, lit);
            return filter.Op switch
            {
                ScalarCompareOp.Eq => eq,
                ScalarCompareOp.Ne => !eq,
                _ => false,
            };
        }

        if (column.PhysicalType == RainDbType.Utf8)
            return false;

        var nb2 = column.HasNulls ? column.NullBitmap.Span : ReadOnlySpan<byte>.Empty;
        if (IsNull(nb2, row, column.HasNulls))
            return false;

        var values = column.Values.Span;
        switch (column.PhysicalType)
        {
            case RainDbType.Int32:
            {
                var v = BinaryPrimitives.ReadInt32LittleEndian(values.Slice(row * sizeof(int), sizeof(int)));
                return CompareInt32(v, (int)filter.ImmediateBits, filter.Op);
            }
            case RainDbType.Int64:
            {
                var v = BinaryPrimitives.ReadInt64LittleEndian(values.Slice(row * sizeof(long), sizeof(long)));
                return CompareInt64(v, filter.ImmediateBits, filter.Op);
            }
            case RainDbType.Float64:
            {
                var v = BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(values.Slice(row * sizeof(double), sizeof(double))));
                return CompareDouble(v, BitConverter.Int64BitsToDouble(filter.ImmediateBits), filter.Op);
            }
            case RainDbType.Boolean:
            {
                var v = values[row] != 0;
                return CompareBool(v, filter.ImmediateBits != 0, filter.Op);
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(column), column.PhysicalType, "Unsupported physical type.");
        }
    }

    private static int FillUtf8Selected(IColumnChunk column, ColumnCompareFilter filter, Span<int> dest)
    {
        var n = column.RowCount;
        var count = 0;
        for (var i = 0; i < n; i++)
        {
            if (RowMatchesFilter(column, filter, i))
                dest[count++] = i;
        }

        return count;
    }

    private static bool Utf8PayloadEquals(IColumnChunk column, int row, ReadOnlySpan<byte> literal)
    {
        return column switch
        {
            Utf8ColumnChunk utf8 => Utf8ArrowRowEquals(utf8, row, literal),
            Utf8LengthPrefixedColumnChunk lp => lp.GetPayloadSpan(row).SequenceEqual(literal),
            _ => throw new NotSupportedException($"Unknown UTF-8 chunk {column.GetType().Name}."),
        };
    }

    private static bool Utf8ArrowRowEquals(Utf8ColumnChunk src, int row, ReadOnlySpan<byte> literal)
    {
        var off = src.Offsets.Span;
        var start = off[row];
        var end = off[row + 1];
        return src.Values.Span.Slice(start, end - start).SequenceEqual(literal);
    }

    private static void FillInt32(
        ReadOnlySpan<byte> values,
        ReadOnlySpan<byte> nb,
        bool hasNulls,
        ScalarCompareOp op,
        int imm,
        Span<int> dest,
        ref int count)
    {
        var n = values.Length / sizeof(int);
        for (var i = 0; i < n; i++)
        {
            if (IsNull(nb, i, hasNulls))
                continue;
            var v = BinaryPrimitives.ReadInt32LittleEndian(values.Slice(i * sizeof(int), sizeof(int)));
            if (CompareInt32(v, imm, op))
                dest[count++] = i;
        }
    }

    private static void FillInt64(
        ReadOnlySpan<byte> values,
        ReadOnlySpan<byte> nb,
        bool hasNulls,
        ScalarCompareOp op,
        long imm,
        Span<int> dest,
        ref int count)
    {
        var n = values.Length / sizeof(long);
        for (var i = 0; i < n; i++)
        {
            if (IsNull(nb, i, hasNulls))
                continue;
            var v = BinaryPrimitives.ReadInt64LittleEndian(values.Slice(i * sizeof(long), sizeof(long)));
            if (CompareInt64(v, imm, op))
                dest[count++] = i;
        }
    }

    private static void FillFloat64(
        ReadOnlySpan<byte> values,
        ReadOnlySpan<byte> nb,
        bool hasNulls,
        ScalarCompareOp op,
        double imm,
        Span<int> dest,
        ref int count)
    {
        var n = values.Length / sizeof(double);
        for (var i = 0; i < n; i++)
        {
            if (IsNull(nb, i, hasNulls))
                continue;
            var v = BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(values.Slice(i * sizeof(double), sizeof(double))));
            if (CompareDouble(v, imm, op))
                dest[count++] = i;
        }
    }

    private static void FillBool(
        ReadOnlySpan<byte> values,
        ReadOnlySpan<byte> nb,
        bool hasNulls,
        ScalarCompareOp op,
        bool imm,
        Span<int> dest,
        ref int count)
    {
        var n = values.Length;
        for (var i = 0; i < n; i++)
        {
            if (IsNull(nb, i, hasNulls))
                continue;
            var v = values[i] != 0;
            if (CompareBool(v, imm, op))
                dest[count++] = i;
        }
    }

    private static bool CompareInt32(int v, int imm, ScalarCompareOp op) =>
        op switch
        {
            ScalarCompareOp.Eq => v == imm,
            ScalarCompareOp.Ne => v != imm,
            ScalarCompareOp.Lt => v < imm,
            ScalarCompareOp.Le => v <= imm,
            ScalarCompareOp.Gt => v > imm,
            ScalarCompareOp.Ge => v >= imm,
            _ => false,
        };

    private static bool CompareInt64(long v, long imm, ScalarCompareOp op) =>
        op switch
        {
            ScalarCompareOp.Eq => v == imm,
            ScalarCompareOp.Ne => v != imm,
            ScalarCompareOp.Lt => v < imm,
            ScalarCompareOp.Le => v <= imm,
            ScalarCompareOp.Gt => v > imm,
            ScalarCompareOp.Ge => v >= imm,
            _ => false,
        };

    private static bool CompareDouble(double v, double imm, ScalarCompareOp op) =>
        op switch
        {
            ScalarCompareOp.Eq => v == imm,
            ScalarCompareOp.Ne => v != imm,
            ScalarCompareOp.Lt => v < imm,
            ScalarCompareOp.Le => v <= imm,
            ScalarCompareOp.Gt => v > imm,
            ScalarCompareOp.Ge => v >= imm,
            _ => false,
        };

    private static bool CompareBool(bool v, bool imm, ScalarCompareOp op) =>
        op switch
        {
            ScalarCompareOp.Eq => v == imm,
            ScalarCompareOp.Ne => v != imm,
            ScalarCompareOp.Lt => !v & imm,
            ScalarCompareOp.Le => !v | imm,
            ScalarCompareOp.Gt => v & !imm,
            ScalarCompareOp.Ge => v | !imm,
            _ => false,
        };
}
