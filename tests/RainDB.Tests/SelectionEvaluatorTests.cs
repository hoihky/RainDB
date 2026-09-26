using System.Buffers.Binary;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Execution;
using RainDB.Query.Plans;
using RainDB.Query.Vectorized;
using RainDB.Schema;

namespace RainDB.Tests;

public class SelectionEvaluatorTests
{
    [Fact]
    public void Conjunctive_where_intersects_predicates()
    {
        var batch = new ColumnarBatch(4, [
            TestDataBuilders.Int32Column([1, 2, 3, 4]),
            TestDataBuilders.Int32Column([10, 20, 30, 40]),
        ]);
        var filters = new[]
        {
            new ColumnCompareFilter(0, ScalarCompareOp.Gt, 1),
            new ColumnCompareFilter(1, ScalarCompareOp.Lt, 35),
        };
        Span<int> dest = stackalloc int[4];
        var count = SelectionEvaluator.FillSelectedRowsConjunctive(batch, filters, dest);
        Assert.Equal(2, count);
        Assert.Equal(1, dest[0]);
        Assert.Equal(2, dest[1]);
    }

    [Fact]
    public void Utf8_equality_literal_matches_row()
    {
        var batch = new ColumnarBatch(2, [
            TestDataBuilders.Utf8Column(["ab", "cd"]),
        ]);
        var filter = new ColumnCompareFilter(0, ScalarCompareOp.Eq, 0, "ab"u8.ToArray());
        Span<int> dest = stackalloc int[2];
        var count = SelectionEvaluator.FillSelectedRows(batch.Columns[0], filter, dest);
        Assert.Equal(1, count);
        Assert.Equal(0, dest[0]);
    }

    [Fact]
    public void Utf8_null_cell_does_not_match_equality()
    {
        var blob = "x"u8.ToArray();
        var col = new Utf8ColumnChunk(1, new[] { 0, 1 }, blob, new byte[] { 0b0000_0001 }, hasNulls: true);
        var filter = new ColumnCompareFilter(0, ScalarCompareOp.Eq, 0, "x"u8.ToArray());
        Span<int> dest = stackalloc int[1];
        var count = SelectionEvaluator.FillSelectedRows(col, filter, dest);
        Assert.Equal(0, count);
    }

    [Fact]
    public void Fixed_width_null_row_excluded_from_equality()
    {
        var vals = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(vals, 5);
        var col = new FixedWidthColumnChunk(RainDbType.Int32, 1, vals, new byte[] { 0b0000_0001 }, hasNulls: true);
        var filter = new ColumnCompareFilter(0, ScalarCompareOp.Eq, 5);
        Assert.False(SelectionEvaluator.RowMatchesFilter(col, filter, 0));
    }
}
