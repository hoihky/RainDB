using System.Buffers.Binary;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Execution;
using RainDB.Query.Plans;
using RainDB.Memory;
using RainDB.Schema;

namespace RainDB.Query.Vectorized;

internal static class GroupHavingEvaluator
{
    internal static ColumnarBatch Apply(
        ColumnarBatch batch,
        ReadOnlySpan<GroupOutputCompareFilter> filters,
        IBufferPool bufferPool,
        IAlignedBufferPool alignedBufferPool)
    {
        if (filters.Length == 0)
            return batch;
        var rowCount = batch.RowCount;
        var selected = new List<int>(rowCount);
        for (var r = 0; r < rowCount; r++)
        {
            if (RowPasses(batch, filters, r))
                selected.Add(r);
        }

        if (selected.Count == rowCount)
            return batch;
        if (selected.Count == 0)
            return new ColumnarBatch(0, Array.Empty<IColumnChunk>());

        var rent = selected.ToArray();
        var gather = new ProjectGather(new SelectionEvaluator());
        return gather.Project(
            batch,
            Enumerable.Range(0, batch.Columns.Count).ToArray(),
            useRowSelection: true,
            selectedRows: rent,
            selected.Count,
            bufferPool,
            alignedBufferPool);
    }

    private static bool RowPasses(IColumnarBatch batch, ReadOnlySpan<GroupOutputCompareFilter> filters, int row)
    {
        foreach (var f in filters)
        {
            var col = batch.Columns[f.OutputColumnIndex];
            if (!CellMatches(col, f, row))
                return false;
        }

        return true;
    }

    private static bool CellMatches(IColumnChunk col, GroupOutputCompareFilter filter, int row)
    {
        if (filter.Utf8LiteralBytes is { } utf8Lit)
        {
            if (col.PhysicalType != RainDbType.Utf8)
                return false;
            var nb = col.HasNulls ? col.NullBitmap.Span : ReadOnlySpan<byte>.Empty;
            if (RowNullBitmap.IsNull(nb, row, col.HasNulls))
                return false;
            var eq = Utf8Equals(col, row, utf8Lit);
            return filter.Op switch
            {
                ScalarCompareOp.Eq => eq,
                ScalarCompareOp.Ne => !eq,
                _ => false,
            };
        }

        var nb2 = col.HasNulls ? col.NullBitmap.Span : ReadOnlySpan<byte>.Empty;
        if (RowNullBitmap.IsNull(nb2, row, col.HasNulls))
            return false;

        return filter.ColumnType switch
        {
            RainDbType.Int32 => CompareInt32(
                BinaryPrimitives.ReadInt32LittleEndian(col.Values.Span.Slice(row * sizeof(int), sizeof(int))),
                (int)filter.ImmediateBits,
                filter.Op),
            RainDbType.Int64 => CompareInt64(
                BinaryPrimitives.ReadInt64LittleEndian(col.Values.Span.Slice(row * sizeof(long), sizeof(long))),
                filter.ImmediateBits,
                filter.Op),
            RainDbType.Float64 => CompareDouble(
                BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(col.Values.Span.Slice(row * sizeof(double), sizeof(double)))),
                BitConverter.Int64BitsToDouble(filter.ImmediateBits),
                filter.Op),
            _ => false,
        };
    }

    private static bool Utf8Equals(IColumnChunk col, int row, ReadOnlySpan<byte> literal) =>
        col switch
        {
            Utf8ColumnChunk u => u.Values.Span[u.Offsets.Span[row]..u.Offsets.Span[row + 1]].SequenceEqual(literal),
            Utf8LengthPrefixedColumnChunk lp => lp.GetPayloadSpan(row).SequenceEqual(literal),
            _ => false,
        };

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
}
