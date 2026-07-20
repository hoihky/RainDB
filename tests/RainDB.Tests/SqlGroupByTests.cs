using System.Buffers.Binary;
using System.Text;
using RainDB;
using RainDB.Columnar;
using RainDB.Core.Catalog;
using RainDB.Core.Columnar;
using RainDB.Core.Tables;
using RainDB.Execution;
using RainDB.Query.Plans;
using RainDB.Schema;
using RainDB.Sql;

namespace RainDB.Tests;

public class SqlGroupByTests
{
    [Fact]
    public void CompilePhysicalPlan_group_by_produces_hash_aggregate()
    {
        var cat = new InMemoryCatalog();
        var schema = new TableSchema([
            new ColumnDef("k", RainDbType.Int32),
            new ColumnDef("v", RainDbType.Int64),
        ]);
        cat.Register(new MemoryTable("m", schema));

        var plan = StrictSqlSubset.CompilePhysicalPlan("SELECT k, SUM(v) FROM m GROUP BY k", cat);
        var ha = Assert.IsType<HashAggregatePhysicalPlan>(plan);
        Assert.Single(ha.GroupKeyColumnIndices);
        Assert.Single(ha.Aggregates);
        Assert.Equal(AggregateKind.Sum, ha.Aggregates[0].Kind);
        Assert.Equal(2, ha.OutputColumns.Length);
    }

    [Fact]
    public async Task ExecuteSql_group_by_sum_counts_rows_per_key()
    {
        var engine = RainDbEngine.CreateDefault();
        var schema = new TableSchema([
            new ColumnDef("k", RainDbType.Int32),
            new ColumnDef("v", RainDbType.Int64),
        ]);
        var t = new MemoryTable("m", schema);
        var k = new byte[] { 1, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0 };
        var v = new byte[24];
        BinaryPrimitives.WriteInt64LittleEndian(v.AsSpan(0, 8), 10);
        BinaryPrimitives.WriteInt64LittleEndian(v.AsSpan(8, 8), 20);
        BinaryPrimitives.WriteInt64LittleEndian(v.AsSpan(16, 8), 100);
        t.AppendBatch(new ColumnarBatch(3, [
            new FixedWidthColumnChunk(RainDbType.Int32, 3, k, ReadOnlyMemory<byte>.Empty, false),
            new FixedWidthColumnChunk(RainDbType.Int64, 3, v, ReadOnlyMemory<byte>.Empty, false),
        ]));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT k, SUM(v) FROM m GROUP BY k");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Single(col.Batches);
        Assert.Equal(2, col.Batches[0].RowCount);
        Assert.Equal(30L, ReadSumForKey(col.Batches[0], key: 1));
        Assert.Equal(100L, ReadSumForKey(col.Batches[0], key: 2));
    }

    [Fact]
    public async Task ExecuteSql_group_by_select_order_sum_before_key()
    {
        var engine = RainDbEngine.CreateDefault();
        var schema = new TableSchema([
            new ColumnDef("k", RainDbType.Int32),
            new ColumnDef("v", RainDbType.Int64),
        ]);
        var t = new MemoryTable("m", schema);
        var kb = new byte[] { 1, 0, 0, 0, 2, 0, 0, 0 };
        var vb = new byte[16];
        BinaryPrimitives.WriteInt64LittleEndian(vb.AsSpan(0, 8), 5);
        BinaryPrimitives.WriteInt64LittleEndian(vb.AsSpan(8, 8), 7);
        t.AppendBatch(new ColumnarBatch(2, [
            new FixedWidthColumnChunk(RainDbType.Int32, 2, kb, ReadOnlyMemory<byte>.Empty, false),
            new FixedWidthColumnChunk(RainDbType.Int64, 2, vb, ReadOnlyMemory<byte>.Empty, false),
        ]));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT SUM(v), k FROM m GROUP BY k");
        var batch = Assert.IsAssignableFrom<IColumnarQueryResult>(r).Batches[0];
        Assert.Equal(RainDbType.Int64, batch.Columns[0].PhysicalType);
        Assert.Equal(RainDbType.Int32, batch.Columns[1].PhysicalType);
        Assert.Equal(5L, BinaryPrimitives.ReadInt64LittleEndian(batch.Columns[0].Values.Span));
        Assert.Equal(1, BinaryPrimitives.ReadInt32LittleEndian(batch.Columns[1].Values.Span));
        Assert.Equal(7L, BinaryPrimitives.ReadInt64LittleEndian(batch.Columns[0].Values.Span[sizeof(long)..]));
        Assert.Equal(2, BinaryPrimitives.ReadInt32LittleEndian(batch.Columns[1].Values.Span[sizeof(int)..]));
    }

    [Fact]
    public async Task ExecuteSql_count_star_and_count_column()
    {
        var engine = RainDbEngine.CreateDefault();
        var schema = new TableSchema([
            new ColumnDef("k", RainDbType.Int32),
            new ColumnDef("v", RainDbType.Int64),
        ]);
        var t = new MemoryTable("m", schema);
        var rows = 2;
        var nbBytes = (rows + 7) >> 3;
        var nullBmp = new byte[nbBytes];
        nullBmp[0] |= 1 << 0; // row 0 null
        var vals = new byte[16];
        BinaryPrimitives.WriteInt64LittleEndian(vals.AsSpan(0, 8), 1);
        BinaryPrimitives.WriteInt64LittleEndian(vals.AsSpan(8, 8), 2);
        var k = new byte[] { 1, 0, 0, 0, 1, 0, 0, 0 };
        t.AppendBatch(new ColumnarBatch(2, [
            new FixedWidthColumnChunk(RainDbType.Int32, 2, k, ReadOnlyMemory<byte>.Empty, false),
            new FixedWidthColumnChunk(RainDbType.Int64, 2, vals, nullBmp, hasNulls: true),
        ]));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT k, COUNT(*), COUNT(v) FROM m GROUP BY k");
        var batch = Assert.IsAssignableFrom<IColumnarQueryResult>(r).Batches[0];
        Assert.Equal(3, batch.Columns.Count);
        Assert.Equal(2L, BinaryPrimitives.ReadInt64LittleEndian(batch.Columns[1].Values.Span));
        Assert.Equal(1L, BinaryPrimitives.ReadInt64LittleEndian(batch.Columns[2].Values.Span));
    }

    [Fact]
    public async Task ExecuteSql_global_count_star_empty_table()
    {
        var engine = RainDbEngine.CreateDefault();
        var schema = new TableSchema([new ColumnDef("x", RainDbType.Int32)]);
        engine.Catalog.Register(new MemoryTable("e", schema));

        await using var r = await engine.ExecuteSqlAsync("SELECT COUNT(*) FROM e");
        var agg = Assert.IsAssignableFrom<IAggregateQueryResult>(r);
        Assert.False(agg.ValueIsNull);
        Assert.Equal(0L, agg.Int64Value);
    }

    [Fact]
    public async Task ExecuteSql_global_sum_all_null_is_sql_null()
    {
        var engine = RainDbEngine.CreateDefault();
        var schema = new TableSchema([new ColumnDef("x", RainDbType.Float64)]);
        var t = new MemoryTable("n", schema);
        var rows = 2;
        var nb = new byte[(rows + 7) >> 3];
        nb[0] = 0b11; // both null
        var vals = new byte[16]; // payload ignored
        t.AppendBatch(new ColumnarBatch(rows, [
            new FixedWidthColumnChunk(RainDbType.Float64, rows, vals, nb, hasNulls: true),
        ]));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT SUM(x) FROM n");
        var agg = Assert.IsAssignableFrom<IAggregateQueryResult>(r);
        Assert.True(agg.ValueIsNull);
    }

    [Fact]
    public async Task ExecuteSql_group_by_where_filters_before_grouping()
    {
        var engine = RainDbEngine.CreateDefault();
        var schema = new TableSchema([
            new ColumnDef("k", RainDbType.Int32),
            new ColumnDef("v", RainDbType.Int64),
        ]);
        var t = new MemoryTable("m", schema);
        var k = new byte[] { 1, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0 };
        var v = new byte[24];
        BinaryPrimitives.WriteInt64LittleEndian(v.AsSpan(0, 8), 10);
        BinaryPrimitives.WriteInt64LittleEndian(v.AsSpan(8, 8), 20);
        BinaryPrimitives.WriteInt64LittleEndian(v.AsSpan(16, 8), 100);
        t.AppendBatch(new ColumnarBatch(3, [
            new FixedWidthColumnChunk(RainDbType.Int32, 3, k, ReadOnlyMemory<byte>.Empty, false),
            new FixedWidthColumnChunk(RainDbType.Int64, 3, v, ReadOnlyMemory<byte>.Empty, false),
        ]));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT k, SUM(v) FROM m WHERE k = 1 GROUP BY k");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Single(col.Batches);
        Assert.Equal(1, col.Batches[0].RowCount);
        Assert.Equal(30L, ReadSumForKey(col.Batches[0], key: 1));
    }

    [Fact]
    public void CompilePhysicalPlan_grouped_inner_join_returns_grouped_join_plan()
    {
        var cat = new InMemoryCatalog();
        cat.Register(new MemoryTable("L", new TableSchema([new ColumnDef("id", RainDbType.Int32)])));
        cat.Register(
            new MemoryTable(
                "R",
                new TableSchema([
                    new ColumnDef("id", RainDbType.Int32),
                    new ColumnDef("amt", RainDbType.Int64),
                ])));
        var plan = StrictSqlSubset.CompilePhysicalPlan(
            "SELECT L.id, SUM(R.amt) FROM L INNER JOIN R ON L.id = R.id GROUP BY L.id",
            cat);
        var gj = Assert.IsType<GroupedJoinPhysicalPlan>(plan);
        Assert.NotNull(gj.Join);
        Assert.NotNull(gj.Aggregate);
    }

    [Fact]
    public async Task ExecuteSql_group_by_utf8_key_sums_numeric_column()
    {
        var engine = RainDbEngine.CreateDefault();
        var schema = new TableSchema([
            new ColumnDef("label", RainDbType.Utf8),
            new ColumnDef("n", RainDbType.Int64),
        ]);
        var t = new MemoryTable("m", schema);
        var nb = new byte[24];
        BinaryPrimitives.WriteInt64LittleEndian(nb.AsSpan(0, 8), 1);
        BinaryPrimitives.WriteInt64LittleEndian(nb.AsSpan(8, 8), 2);
        BinaryPrimitives.WriteInt64LittleEndian(nb.AsSpan(16, 8), 4);
        t.AppendBatch(new ColumnarBatch(3, [
            Utf8Col(["a", "b", "a"]),
            new FixedWidthColumnChunk(RainDbType.Int64, 3, nb, ReadOnlyMemory<byte>.Empty, false),
        ]));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT label, SUM(n) FROM m GROUP BY label");
        var batch = Assert.IsAssignableFrom<IColumnarQueryResult>(r).Batches[0];
        Assert.Equal(2, batch.RowCount);
        long sum = 0;
        for (var i = 0; i < batch.RowCount; i++)
            sum += BinaryPrimitives.ReadInt64LittleEndian(batch.Columns[1].Values.Span.Slice(i * sizeof(long), sizeof(long)));
        Assert.Equal(7L, sum);
    }

    private static Utf8ColumnChunk Utf8Col(string[] rows)
    {
        var offsets = new int[rows.Length + 1];
        var blob = new List<byte>();
        for (var i = 0; i < rows.Length; i++)
        {
            offsets[i] = blob.Count;
            blob.AddRange(Encoding.UTF8.GetBytes(rows[i]));
        }

        offsets[^1] = blob.Count;
        return new Utf8ColumnChunk(rows.Length, offsets, blob.ToArray(), ReadOnlyMemory<byte>.Empty, false);
    }

    private static long ReadSumForKey(IColumnarBatch batch, int key)
    {
        for (var r = 0; r < batch.RowCount; r++)
        {
            var k = BinaryPrimitives.ReadInt32LittleEndian(batch.Columns[0].Values.Span.Slice(r * sizeof(int), sizeof(int)));
            if (k != key)
                continue;
            return BinaryPrimitives.ReadInt64LittleEndian(batch.Columns[1].Values.Span.Slice(r * sizeof(long), sizeof(long)));
        }

        throw new InvalidOperationException($"key {key} not found");
    }
}
