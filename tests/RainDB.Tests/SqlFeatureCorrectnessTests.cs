using System.Buffers.Binary;
using RainDB;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Core.Catalog;
using RainDB.Core.Tables;
using RainDB.Execution;
using RainDB.Schema;
using RainDB.Sql;

namespace RainDB.Tests;

/// <summary>Cross-cutting SQL and engine correctness (null semantics, types, edge cases).</summary>
public class SqlFeatureCorrectnessTests
{
    [Fact]
    public async Task Where_null_cell_never_matches_equality_predicate()
    {
        var engine = RainDbEngine.CreateDefault();
        var t = TestDataBuilders.TableWithInt32Column("t", "x", [(1, false), (2, true)]);
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT x FROM t WHERE x = 1");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(1, col.RowCount);
        Assert.Equal(1, TestDataBuilders.ReadInt32(col.Batches[0].Columns[0], 0));
    }

    [Fact]
    public async Task Global_sum_empty_table_is_sql_null()
    {
        var engine = RainDbEngine.CreateDefault();
        engine.Catalog.Register(TestDataBuilders.EmptyTable("t", RainDbType.Float64, "amt"));

        await using var r = await engine.ExecuteSqlAsync("SELECT SUM(amt) FROM t");
        var agg = Assert.IsAssignableFrom<IAggregateQueryResult>(r);
        Assert.True(agg.ValueIsNull);
    }

    [Fact]
    public async Task Global_count_star_empty_table_is_zero_not_null()
    {
        var engine = RainDbEngine.CreateDefault();
        engine.Catalog.Register(TestDataBuilders.EmptyTable("t", RainDbType.Int32, "x"));

        await using var r = await engine.ExecuteSqlAsync("SELECT COUNT(*) FROM t");
        var agg = Assert.IsAssignableFrom<IAggregateQueryResult>(r);
        Assert.False(agg.ValueIsNull);
        Assert.Equal(0L, agg.Int64Value);
    }

    [Fact]
    public async Task Count_column_skips_null_rows()
    {
        var engine = RainDbEngine.CreateDefault();
        var t = TestDataBuilders.TableWithInt32Column("t", "x", [(1, false), (2, true), (3, false)]);
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT COUNT(x) FROM t");
        var agg = Assert.IsAssignableFrom<IAggregateQueryResult>(r);
        Assert.Equal(2L, agg.Int64Value);
    }

    [Fact]
    public async Task Global_min_all_null_measure_is_sql_null()
    {
        var engine = RainDbEngine.CreateDefault();
        var nb = new byte[] { 0b0000_0011 };
        var vals = BitConverter.GetBytes(1.0).Concat(BitConverter.GetBytes(2.0)).ToArray();
        var t = new MemoryTable("t", new TableSchema([new ColumnDef("v", RainDbType.Float64)]));
        t.AppendBatch(new ColumnarBatch(2, [
            new FixedWidthColumnChunk(RainDbType.Float64, 2, vals, nb, hasNulls: true),
        ]));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT MIN(v) FROM t");
        var agg = Assert.IsAssignableFrom<IAggregateQueryResult>(r);
        Assert.True(agg.ValueIsNull);
    }

    [Fact]
    public async Task Where_boolean_true_literal()
    {
        var engine = RainDbEngine.CreateDefault();
        var vals = new byte[] { 0, 1, 0 };
        var t = new MemoryTable("t", new TableSchema([new ColumnDef("flag", RainDbType.Boolean)]));
        t.AppendBatch(new ColumnarBatch(3, [
            new FixedWidthColumnChunk(RainDbType.Boolean, 3, vals, ReadOnlyMemory<byte>.Empty, false),
        ]));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT flag FROM t WHERE flag = TRUE");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(1, col.RowCount);
        Assert.Equal(1, col.Batches[0].Columns[0].Values.Span[0]);
    }

    [Fact]
    public async Task Where_utf8_not_equal_literal()
    {
        var engine = RainDbEngine.CreateDefault();
        var t = new MemoryTable("t", new TableSchema([new ColumnDef("region", RainDbType.Utf8)]));
        var utf8 = new Utf8ColumnChunk(3, new[] { 0, 2, 4, 6 }, "usukca"u8.ToArray(), ReadOnlyMemory<byte>.Empty, false);
        t.AppendBatch(new ColumnarBatch(3, [utf8]));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT region FROM t WHERE region != 'us'");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(2, col.RowCount);
    }

    [Fact]
    public async Task Inner_join_no_matching_keys_returns_empty()
    {
        var engine = RainDbEngine.CreateDefault();
        var left = new MemoryTable("L", new TableSchema([new ColumnDef("id", RainDbType.Int32)]));
        left.AppendBatch(TestDataBuilders.SingleInt32Batch(1));
        var right = new MemoryTable("R", new TableSchema([new ColumnDef("id", RainDbType.Int32)]));
        right.AppendBatch(TestDataBuilders.SingleInt32Batch(99));
        engine.Catalog.Register(left);
        engine.Catalog.Register(right);

        await using var r = await engine.ExecuteSqlAsync("SELECT * FROM L INNER JOIN R ON L.id = R.id");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(0, col.RowCount);
    }

    [Fact]
    public async Task Order_by_desc_and_limit()
    {
        var engine = RainDbEngine.CreateDefault();
        var t = new MemoryTable("t", new TableSchema([new ColumnDef("x", RainDbType.Int32)]));
        var bytes = new byte[12];
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(0, 4), 1);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(4, 4), 3);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(8, 4), 2);
        t.AppendBatch(new ColumnarBatch(3, [
            new FixedWidthColumnChunk(RainDbType.Int32, 3, bytes, ReadOnlyMemory<byte>.Empty, false),
        ]));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT x FROM t ORDER BY x DESC LIMIT 2");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(2, col.RowCount);
        Assert.Equal(3, TestDataBuilders.ReadInt32(col.Batches[0].Columns[0], 0));
        Assert.Equal(2, TestDataBuilders.ReadInt32(col.Batches[0].Columns[0], 1));
    }

    [Fact]
    public async Task Multi_batch_scan_preserves_batch_order_in_output()
    {
        var engine = RainDbEngine.CreateDefault();
        var t = new MemoryTable("t", new TableSchema([new ColumnDef("x", RainDbType.Int32)]));
        t.AppendBatch(TestDataBuilders.SingleInt32Batch(10));
        t.AppendBatch(TestDataBuilders.SingleInt32Batch(20));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT x FROM t");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(2, col.Batches.Count);
        Assert.Equal(10, TestDataBuilders.ReadInt32(col.Batches[0].Columns[0], 0));
        Assert.Equal(20, TestDataBuilders.ReadInt32(col.Batches[1].Columns[0], 0));
    }

    [Fact]
    public async Task Group_by_utf8_key_aggregates_per_string()
    {
        var engine = RainDbEngine.CreateDefault();
        var schema = new TableSchema([
            new ColumnDef("k", RainDbType.Utf8),
            new ColumnDef("v", RainDbType.Int64),
        ]);
        var t = new MemoryTable("m", schema);
        var utf8 = new Utf8ColumnChunk(2, new[] { 0, 1, 2 }, "ab"u8.ToArray(), ReadOnlyMemory<byte>.Empty, false);
        var vb = new byte[16];
        BinaryPrimitives.WriteInt64LittleEndian(vb.AsSpan(0, 8), 4);
        BinaryPrimitives.WriteInt64LittleEndian(vb.AsSpan(8, 8), 5);
        t.AppendBatch(new ColumnarBatch(2, [utf8, new FixedWidthColumnChunk(RainDbType.Int64, 2, vb, ReadOnlyMemory<byte>.Empty, false)]));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT k, SUM(v) FROM m GROUP BY k");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(2, col.Batches[0].RowCount);
    }

    [Fact]
    public void Compile_rejects_having_without_aggregate()
    {
        var cat = new InMemoryCatalog();
        cat.Register(new MemoryTable("t", new TableSchema([new ColumnDef("x", RainDbType.Int32)])));
        var ex = Assert.Throws<SqlCompileException>(() =>
            StrictSqlSubset.CompilePhysicalPlan("SELECT x FROM t GROUP BY x HAVING x > 0", cat));
        Assert.Contains("aggregate", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Float_where_greater_than_literal()
    {
        var engine = RainDbEngine.CreateDefault();
        var t = new MemoryTable("t", new TableSchema([new ColumnDef("v", RainDbType.Float64)]));
        var vals = BitConverter.GetBytes(1.5).Concat(BitConverter.GetBytes(2.5)).ToArray();
        t.AppendBatch(new ColumnarBatch(2, [
            new FixedWidthColumnChunk(RainDbType.Float64, 2, vals, ReadOnlyMemory<byte>.Empty, false),
        ]));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT v FROM t WHERE v > 2.0");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(1, col.RowCount);
        Assert.Equal(2.5, BitConverter.ToDouble(col.Batches[0].Columns[0].Values.Span));
    }

    [Fact]
    public async Task Where_filters_before_global_sum()
    {
        var engine = RainDbEngine.CreateDefault();
        var t = new MemoryTable("t", new TableSchema([new ColumnDef("k", RainDbType.Int32), new ColumnDef("v", RainDbType.Int64)]));
        var k = new byte[] { 1, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0 };
        var v = new byte[24];
        BinaryPrimitives.WriteInt64LittleEndian(v.AsSpan(0, 8), 10);
        BinaryPrimitives.WriteInt64LittleEndian(v.AsSpan(8, 8), 100);
        BinaryPrimitives.WriteInt64LittleEndian(v.AsSpan(16, 8), 5);
        t.AppendBatch(new ColumnarBatch(3, [
            new FixedWidthColumnChunk(RainDbType.Int32, 3, k, ReadOnlyMemory<byte>.Empty, false),
            new FixedWidthColumnChunk(RainDbType.Int64, 3, v, ReadOnlyMemory<byte>.Empty, false),
        ]));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT SUM(v) FROM t WHERE k = 2");
        var agg = Assert.IsAssignableFrom<IAggregateQueryResult>(r);
        Assert.False(agg.ValueIsNull);
        Assert.Equal(105L, agg.Int64Value);
    }

    [Fact]
    public async Task Columnar_result_dispose_can_be_called_twice()
    {
        var engine = RainDbEngine.CreateDefault();
        var t = new MemoryTable("t", new TableSchema([new ColumnDef("x", RainDbType.Int32)]));
        t.AppendBatch(TestDataBuilders.SingleInt32Batch(1));
        engine.Catalog.Register(t);

        var r = await engine.ExecuteSqlAsync("SELECT x FROM t");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(1, col.RowCount);
        await r.DisposeAsync();
        await r.DisposeAsync();
    }

}
