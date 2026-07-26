using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using RainDB.Columnar;
using RainDB.Execution;
using RainDB.Query.Plans;
using RainDB.Schema;

namespace RainDB.Query.Vectorized;

/// <summary>Fixed-width compare kernels writing dense selection vectors (row indices).</summary>
internal static class FixedWidthSelectionKernels
{
    internal static int FillSelectedIndices(
        IColumnChunk column,
        ColumnCompareFilter filter,
        Span<int> dest)
    {
        var n = column.RowCount;
        if (dest.Length < n)
            throw new ArgumentException("Selection buffer too small.", nameof(dest));

        var hasNulls = column.HasNulls;
        var nb = hasNulls ? column.NullBitmap.Span : ReadOnlySpan<byte>.Empty;
        var values = column.Values.Span;

        return column.PhysicalType switch
        {
            RainDbType.Int32 => FillInt32(values, nb, hasNulls, filter.Op, (int)filter.ImmediateBits, dest),
            RainDbType.Int64 => FillInt64(values, nb, hasNulls, filter.Op, filter.ImmediateBits, dest),
            RainDbType.Float64 => FillFloat64(values, nb, hasNulls, filter.Op, BitConverter.Int64BitsToDouble(filter.ImmediateBits), dest),
            RainDbType.Boolean => FillBool(values, nb, hasNulls, filter.Op, filter.ImmediateBits != 0, dest),
            _ => throw new ArgumentOutOfRangeException(nameof(column), column.PhysicalType, "Unsupported physical type."),
        };
    }

    /// <summary>Compact <paramref name="selectedRows"/>[0..<paramref name="count"/>] to rows that also pass <paramref name="filter"/>.</summary>
    internal static int IntersectSelectedIndices(
        IColumnChunk column,
        ColumnCompareFilter filter,
        Span<int> selectedRows,
        int count)
    {
        if (count == 0)
            return 0;

        var hasNulls = column.HasNulls;
        var nb = hasNulls ? column.NullBitmap.Span : ReadOnlySpan<byte>.Empty;
        var values = column.Values.Span;
        var write = 0;
        switch (column.PhysicalType)
        {
            case RainDbType.Int32:
                write = IntersectInt32(values, nb, hasNulls, filter.Op, (int)filter.ImmediateBits, selectedRows, count);
                break;
            case RainDbType.Int64:
                write = IntersectInt64(values, nb, hasNulls, filter.Op, filter.ImmediateBits, selectedRows, count);
                break;
            case RainDbType.Float64:
                write = IntersectFloat64(values, nb, hasNulls, filter.Op, BitConverter.Int64BitsToDouble(filter.ImmediateBits), selectedRows, count);
                break;
            case RainDbType.Boolean:
                write = IntersectBool(values, nb, hasNulls, filter.Op, filter.ImmediateBits != 0, selectedRows, count);
                break;
            default:
                for (var r = 0; r < count; r++)
                {
                    var row = selectedRows[r];
                    if (SelectionEvaluator.RowMatchesFilter(column, filter, row))
                        selectedRows[write++] = row;
                }

                break;
        }

        return write;
    }

    private static int FillInt32(
        ReadOnlySpan<byte> values,
        ReadOnlySpan<byte> nb,
        bool hasNulls,
        ScalarCompareOp op,
        int imm,
        Span<int> dest)
    {
        var ints = MemoryMarshal.Cast<byte, int>(values);
        if (!hasNulls && op == ScalarCompareOp.Eq && Vector128.IsHardwareAccelerated)
            return FillInt32EqVectorized(ints, imm, dest);

        var count = 0;
        for (var i = 0; i < ints.Length; i++)
        {
            if (SelectionEvaluator.IsNull(nb, i, hasNulls))
                continue;
            if (CompareInt32(ints[i], imm, op))
                dest[count++] = i;
        }

        return count;
    }

    private static int FillInt32EqVectorized(ReadOnlySpan<int> values, int imm, Span<int> dest)
    {
        var count = 0;
        var i = 0;
        var vImm = Vector128.Create(imm);
        ref var start = ref MemoryMarshal.GetReference(values);
        var limit = values.Length - (values.Length % Vector128<int>.Count);
        for (; i < limit; i += Vector128<int>.Count)
        {
            var block = Vector128.LoadUnsafe(ref Unsafe.Add(ref start, i));
            var eq = Vector128.Equals(block, vImm);
            for (var j = 0; j < Vector128<int>.Count; j++)
            {
                if (eq.GetElement(j) != 0)
                    dest[count++] = i + j;
            }
        }

        for (; i < values.Length; i++)
        {
            if (values[i] == imm)
                dest[count++] = i;
        }

        return count;
    }

    private static int IntersectInt32(
        ReadOnlySpan<byte> values,
        ReadOnlySpan<byte> nb,
        bool hasNulls,
        ScalarCompareOp op,
        int imm,
        Span<int> selectedRows,
        int count)
    {
        var ints = MemoryMarshal.Cast<byte, int>(values);
        var write = 0;
        for (var r = 0; r < count; r++)
        {
            var i = selectedRows[r];
            if (SelectionEvaluator.IsNull(nb, i, hasNulls))
                continue;
            if (CompareInt32(ints[i], imm, op))
                selectedRows[write++] = i;
        }

        return write;
    }

    private static int FillInt64(
        ReadOnlySpan<byte> values,
        ReadOnlySpan<byte> nb,
        bool hasNulls,
        ScalarCompareOp op,
        long imm,
        Span<int> dest)
    {
        var count = 0;
        var longs = MemoryMarshal.Cast<byte, long>(values);
        for (var i = 0; i < longs.Length; i++)
        {
            if (SelectionEvaluator.IsNull(nb, i, hasNulls))
                continue;
            if (CompareInt64(longs[i], imm, op))
                dest[count++] = i;
        }

        return count;
    }

    private static int IntersectInt64(
        ReadOnlySpan<byte> values,
        ReadOnlySpan<byte> nb,
        bool hasNulls,
        ScalarCompareOp op,
        long imm,
        Span<int> selectedRows,
        int count)
    {
        var longs = MemoryMarshal.Cast<byte, long>(values);
        var write = 0;
        for (var r = 0; r < count; r++)
        {
            var i = selectedRows[r];
            if (SelectionEvaluator.IsNull(nb, i, hasNulls))
                continue;
            if (CompareInt64(longs[i], imm, op))
                selectedRows[write++] = i;
        }

        return write;
    }

    private static int FillFloat64(
        ReadOnlySpan<byte> values,
        ReadOnlySpan<byte> nb,
        bool hasNulls,
        ScalarCompareOp op,
        double imm,
        Span<int> dest)
    {
        var count = 0;
        var doubles = MemoryMarshal.Cast<byte, double>(values);
        for (var i = 0; i < doubles.Length; i++)
        {
            if (SelectionEvaluator.IsNull(nb, i, hasNulls))
                continue;
            if (CompareDouble(doubles[i], imm, op))
                dest[count++] = i;
        }

        return count;
    }

    private static int IntersectFloat64(
        ReadOnlySpan<byte> values,
        ReadOnlySpan<byte> nb,
        bool hasNulls,
        ScalarCompareOp op,
        double imm,
        Span<int> selectedRows,
        int count)
    {
        var doubles = MemoryMarshal.Cast<byte, double>(values);
        var write = 0;
        for (var r = 0; r < count; r++)
        {
            var i = selectedRows[r];
            if (SelectionEvaluator.IsNull(nb, i, hasNulls))
                continue;
            if (CompareDouble(doubles[i], imm, op))
                selectedRows[write++] = i;
        }

        return write;
    }

    private static int FillBool(
        ReadOnlySpan<byte> values,
        ReadOnlySpan<byte> nb,
        bool hasNulls,
        ScalarCompareOp op,
        bool imm,
        Span<int> dest)
    {
        var count = 0;
        for (var i = 0; i < values.Length; i++)
        {
            if (SelectionEvaluator.IsNull(nb, i, hasNulls))
                continue;
            var v = values[i] != 0;
            if (CompareBool(v, imm, op))
                dest[count++] = i;
        }

        return count;
    }

    private static int IntersectBool(
        ReadOnlySpan<byte> values,
        ReadOnlySpan<byte> nb,
        bool hasNulls,
        ScalarCompareOp op,
        bool imm,
        Span<int> selectedRows,
        int count)
    {
        var write = 0;
        for (var r = 0; r < count; r++)
        {
            var i = selectedRows[r];
            if (SelectionEvaluator.IsNull(nb, i, hasNulls))
                continue;
            var v = values[i] != 0;
            if (CompareBool(v, imm, op))
                selectedRows[write++] = i;
        }

        return write;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
