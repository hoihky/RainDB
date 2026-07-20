using System.Buffers.Binary;
using RainDB;
using RainDB.Columnar;
using RainDB.Core.Catalog;
using RainDB.Core.Columnar;
using RainDB.Core.Tables;
using RainDB.Execution;
using RainDB.Logical;
using RainDB.Query.Plans;
using RainDB.Schema;
using RainDB.Sql;

namespace RainDB.Tests;

public class SqlOrderByLimitTests
{
    [Fact]
    public void Parse_logical_scan_order_by_limit()
    {
        var plan = StrictSqlSubset.ParseLogicalPlan("SELECT k FROM m ORDER BY k DESC LIMIT 2");
        var scan = Assert.IsType<LogicalTableScan>(plan.Root);
        Assert.NotNull(scan.OrderBy);
        Assert.Single(scan.OrderBy);
        Assert.True(scan.OrderBy[0].Descending);
        Assert.Equal(2, scan.Limit);
    }

    [Fact]
    public void CompilePhysicalPlan_sort_top_n_for_table()
    {
        var cat = new InMemoryCatalog();
        cat.Register(new MemoryTable("m", new TableSchema([
            new ColumnDef("k", RainDbType.Int32),
            new ColumnDef("v", RainDbType.Int64),
        ])));
        var plan = StrictSqlSubset.CompilePhysicalPlan("SELECT k FROM m ORDER BY k LIMIT 1", cat);
        var st = Assert.IsType<SortTopNPhysicalPlan>(plan);
        Assert.Single(st.SortKeys);
        Assert.Equal(1, st.Limit);
    }

    [Fact]
    public async Task ExecuteSql_order_by_limit_single_table()
    {
        var engine = RainDbEngine.CreateDefault();
        var schema = new TableSchema([
            new ColumnDef("k", RainDbType.Int32),
        ]);
        var t = new MemoryTable("m", schema);
        var kb = new byte[] { 3, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0 };
        t.AppendBatch(new ColumnarBatch(3, [
            new FixedWidthColumnChunk(RainDbType.Int32, 3, kb, ReadOnlyMemory<byte>.Empty, false),
        ]));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT k FROM m ORDER BY k ASC LIMIT 2");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(2, col.RowCount);
        Assert.Equal(1, BinaryPrimitives.ReadInt32LittleEndian(col.Batches[0].Columns[0].Values.Span));
        Assert.Equal(2, BinaryPrimitives.ReadInt32LittleEndian(col.Batches[0].Columns[0].Values.Span[sizeof(int)..]));
    }

    [Fact]
    public async Task ExecuteSql_join_order_by_limit()
    {
        var engine = RainDbEngine.CreateDefault();
        var leftSchema = new TableSchema([new ColumnDef("id", RainDbType.Int32)]);
        var rightSchema = new TableSchema([
            new ColumnDef("id", RainDbType.Int32),
            new ColumnDef("amt", RainDbType.Int64),
        ]);
        var left = new MemoryTable("L", leftSchema);
        var right = new MemoryTable("R", rightSchema);
        var lid = new byte[] { 1, 0, 0, 0, 2, 0, 0, 0 };
        left.AppendBatch(new ColumnarBatch(2, [
            new FixedWidthColumnChunk(RainDbType.Int32, 2, lid, ReadOnlyMemory<byte>.Empty, false),
        ]));
        var rid = new byte[] { 1, 0, 0, 0, 2, 0, 0, 0 };
        var amts = new byte[16];
        BinaryPrimitives.WriteInt64LittleEndian(amts.AsSpan(0, 8), 100);
        BinaryPrimitives.WriteInt64LittleEndian(amts.AsSpan(8, 8), 50);
        right.AppendBatch(new ColumnarBatch(2, [
            new FixedWidthColumnChunk(RainDbType.Int32, 2, rid, ReadOnlyMemory<byte>.Empty, false),
            new FixedWidthColumnChunk(RainDbType.Int64, 2, amts, ReadOnlyMemory<byte>.Empty, false),
        ]));
        engine.Catalog.Register(left);
        engine.Catalog.Register(right);

        await using var r = await engine.ExecuteSqlAsync(
            "SELECT L.id, R.amt FROM L INNER JOIN R ON L.id = R.id ORDER BY R.amt DESC LIMIT 1");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(1, col.RowCount);
        Assert.Equal(1, BinaryPrimitives.ReadInt32LittleEndian(col.Batches[0].Columns[0].Values.Span));
        Assert.Equal(100L, BinaryPrimitives.ReadInt64LittleEndian(col.Batches[0].Columns[1].Values.Span));
    }

    [Fact]
    public void Group_by_with_order_by_throws_at_parse()
    {
        var ex = Assert.Throws<SqlCompileException>(() =>
            StrictSqlSubset.ParseLogicalPlan("SELECT k, SUM(v) FROM m GROUP BY k ORDER BY k"));
        Assert.Contains("ORDER BY", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
