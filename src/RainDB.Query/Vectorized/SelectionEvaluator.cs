using System.Buffers.Binary;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Execution;
using RainDB.Query.Plans;
using RainDB.Schema;

namespace RainDB.Query.Vectorized;

/// <summary>
/// Builds dense selection vectors (matching row indices) for filters.
/// Fixed-width predicates use column-wise compare kernels; conjunctive <c>AND</c> intersects successive selections in-place.
/// </summary>
/// <remarks>
/// <para><b>UTF-8 predicates</b> (dedicated path): only <c>=</c>, <c>!=</c>, and <c>&lt;&gt;</c> with a single-quoted literal are supported.
/// Comparison is raw UTF-8 byte equality against the cell payload (Arrow blob slice or length-prefixed payload).
/// SQL NULL cells do not match. Range, <c>LIKE</c>, and collation are not implemented.</para>
/// </remarks>
internal sealed class SelectionEvaluator
{
    private FixedWidthSelectionKernels? _kernels;

    internal void BindSelectionKernels(FixedWidthSelectionKernels kernels) =>
        _kernels = kernels ?? throw new ArgumentNullException(nameof(kernels));

    internal bool IsNull(ReadOnlySpan<byte> nullBitmap, int row, bool hasNulls)
    {
        if (!hasNulls)
            return false;
        return (nullBitmap[row >> 3] & (1 << (row & 7))) != 0;
    }

    /// <summary>Writes matching row indices; returns count.</summary>
    internal int FillSelectedRowsConjunctive(IColumnarBatch batch, ReadOnlySpan<ColumnCompareFilter> filters, Span<int> dest)
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

        if (dest.Length < n)
            throw new ArgumentException("Selection buffer too small.", nameof(dest));

        var count = ApplyFilter(batch, filters[0], dest);
        for (var f = 1; f < filters.Length; f++)
            count = IntersectFilter(batch, filters[f], dest, count);

        return count;
    }

    private int ApplyFilter(IColumnarBatch batch, ColumnCompareFilter filter, Span<int> dest)
    {
        if (filter.Int32Expression is not null)
            return FillInt32ExpressionSelected(batch, filter, dest);
        if (filter.Float64Expression is not null)
            return FillFloat64ExpressionSelected(batch, filter, dest);
        return FillSelectedRows(batch.Columns[filter.ColumnIndex], filter, dest);
    }

    private int IntersectFilter(IColumnarBatch batch, ColumnCompareFilter filter, Span<int> dest, int count)
    {
        if (filter.Int32Expression is not null)
            return IntersectInt32Expression(batch, filter, dest, count);
        if (filter.Float64Expression is not null)
            return IntersectFloat64Expression(batch, filter, dest, count);
        var col = batch.Columns[filter.ColumnIndex];
        return filter.Utf8LiteralBytes is not null || col.PhysicalType == RainDbType.Utf8
            ? IntersectUtf8(col, filter, dest, count)
            : (_kernels ?? throw new InvalidOperationException("Fixed-width selection kernels are not bound."))
                .IntersectSelectedIndices(col, filter, dest, count);
    }

    /// <summary>Writes 0..rowCount-1 row indices that pass <paramref name="filter"/> into <paramref name="dest"/>; returns match count.</summary>
    internal int FillSelectedRows(IColumnChunk column, ColumnCompareFilter filter, Span<int> dest)
    {
        if (filter.ColumnIndex < 0)
            throw new ArgumentOutOfRangeException(nameof(filter));

        if (filter.Utf8LiteralBytes is not null)
        {
            if (column.PhysicalType != RainDbType.Utf8)
                throw new ArgumentException("UTF-8 literal filter requires a UTF-8 column.", nameof(column));
            return FillUtf8Selected(column, filter, dest);
        }

        if (column.PhysicalType == RainDbType.Utf8)
            throw new NotSupportedException("UTF-8 column requires a string literal predicate.");

        if (_kernels is null)
            throw new InvalidOperationException("Fixed-width selection kernels are not bound.");

        return _kernels.FillSelectedIndices(column, filter, dest);
    }

    internal bool RowMatchesFilter(IColumnChunk column, ColumnCompareFilter filter, int row)
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

    private int IntersectUtf8(IColumnChunk column, ColumnCompareFilter filter, Span<int> selectedRows, int count)
    {
        var write = 0;
        for (var r = 0; r < count; r++)
        {
            var row = selectedRows[r];
            if (RowMatchesFilter(column, filter, row))
                selectedRows[write++] = row;
        }

        return write;
    }

    private int FillUtf8Selected(IColumnChunk column, ColumnCompareFilter filter, Span<int> dest)
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

    private static int FillInt32ExpressionSelected(IColumnarBatch batch, ColumnCompareFilter filter, Span<int> dest)
    {
        var expr = filter.Int32Expression!;
        var imm = (int)filter.ImmediateBits;
        var n = batch.RowCount;
        var count = 0;
        for (var i = 0; i < n; i++)
        {
            if (expr.TryGetInt32(batch, i, out var v) && CompareInt32(v, imm, filter.Op))
                dest[count++] = i;
        }

        return count;
    }

    private static int FillFloat64ExpressionSelected(IColumnarBatch batch, ColumnCompareFilter filter, Span<int> dest)
    {
        var expr = filter.Float64Expression!;
        var imm = BitConverter.Int64BitsToDouble(filter.ImmediateBits);
        var n = batch.RowCount;
        var count = 0;
        for (var i = 0; i < n; i++)
        {
            if (expr.TryGetFloat64(batch, i, out var v) && CompareDouble(v, imm, filter.Op))
                dest[count++] = i;
        }

        return count;
    }

    private static int IntersectInt32Expression(IColumnarBatch batch, ColumnCompareFilter filter, Span<int> dest, int count)
    {
        var expr = filter.Int32Expression!;
        var imm = (int)filter.ImmediateBits;
        var write = 0;
        for (var r = 0; r < count; r++)
        {
            var row = dest[r];
            if (expr.TryGetInt32(batch, row, out var v) && CompareInt32(v, imm, filter.Op))
                dest[write++] = row;
        }

        return write;
    }

    private static int IntersectFloat64Expression(IColumnarBatch batch, ColumnCompareFilter filter, Span<int> dest, int count)
    {
        var expr = filter.Float64Expression!;
        var imm = BitConverter.Int64BitsToDouble(filter.ImmediateBits);
        var write = 0;
        for (var r = 0; r < count; r++)
        {
            var row = dest[r];
            if (expr.TryGetFloat64(batch, row, out var v) && CompareDouble(v, imm, filter.Op))
                dest[write++] = row;
        }

        return write;
    }
}
