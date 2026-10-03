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

public class PhaseD5Tests
{
    [Fact]
    public async Task Distinct_int64_deduplicates_all_unique_values()
    {
        var engine = RainDbEngine.CreateDefault();
        var t = new MemoryTable("t", new TableSchema([new ColumnDef("k", RainDbType.Int64)]));
        t.AppendBatch(new ColumnarBatch(4, [Int64Chunk([100L, 200L, 100L, 300L])]));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT DISTINCT k FROM t ORDER BY k");
        Assert.Equal([100L, 200L, 300L], ReadInt64Column(r, 0));
    }

    [Fact]
    public async Task Select_distinct_deduplicates_rows()
    {
        var engine = RainDbEngine.CreateDefault();
        var t = new MemoryTable("t", new TableSchema([new ColumnDef("x", RainDbType.Int32)]));
        t.AppendBatch(Int32Batch([1, 1, 2]));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT DISTINCT x FROM t ORDER BY x");
        Assert.Equal([1, 2], ReadInt32Column(r, 0));
    }

    [Fact]
    public async Task Union_distinct_executes()
    {
        var engine = RainDbEngine.CreateDefault();
        var a = new MemoryTable("a", new TableSchema([new ColumnDef("x", RainDbType.Int32)]));
        a.AppendBatch(Int32Batch([1, 2]));
        var b = new MemoryTable("b", new TableSchema([new ColumnDef("x", RainDbType.Int32)]));
        b.AppendBatch(Int32Batch([2, 3]));
        engine.Catalog.Register(a);
        engine.Catalog.Register(b);

        await using var r = await engine.ExecuteSqlAsync("SELECT x FROM a UNION SELECT x FROM b ORDER BY x");
        Assert.Equal([1, 2, 3], ReadInt32Column(r, 0));
    }

    [Fact]
    public async Task Right_join_matches_left_join_swap()
    {
        var engine = RainDbEngine.CreateDefault();
        RegisterLR(engine, [1, 2], [1], [9L]);

        await using var r = await engine.ExecuteSqlAsync(
            "SELECT R.b FROM L RIGHT JOIN R ON L.id = R.id");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(1, col.RowCount);
        Assert.Equal(9L, BinaryPrimitives.ReadInt64LittleEndian(col.Batches[0].Columns[0].Values.Span));
    }

    [Fact]
    public async Task Count_distinct_per_group()
    {
        var engine = RainDbEngine.CreateDefault();
        var t = new MemoryTable("t", new TableSchema([
            new ColumnDef("g", RainDbType.Int32),
            new ColumnDef("v", RainDbType.Int32),
        ]));
        t.AppendBatch(new ColumnarBatch(4, [Int32Chunk([1, 1, 2, 2]), Int32Chunk([10, 20, 10, 30])]));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync(
            "SELECT g, COUNT(DISTINCT v) FROM t GROUP BY g ORDER BY g");
        Assert.Equal([1, 2], ReadInt32Column(r, 0));
        Assert.Equal([2L, 2L], ReadInt64Column(r, 1));
    }

    [Fact]
    public async Task Group_by_order_by_limit()
    {
        var engine = RainDbEngine.CreateDefault();
        var t = new MemoryTable("t", new TableSchema([
            new ColumnDef("g", RainDbType.Int32),
            new ColumnDef("v", RainDbType.Int32),
        ]));
        t.AppendBatch(new ColumnarBatch(3, [Int32Chunk([1, 2, 2]), Int32Chunk([5, 1, 9])]));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync(
            "SELECT g, SUM(v) FROM t GROUP BY g ORDER BY g DESC LIMIT 1");
        Assert.Equal(1, Assert.IsAssignableFrom<IColumnarQueryResult>(r).RowCount);
        Assert.Equal(2, ReadInt32Column(r, 0)[0]);
    }

    [Fact]
    public void Correlated_exists_logical_and_physical_bind_correlation()
    {
        const string sql = """
            SELECT quantity FROM order_lines
            WHERE EXISTS (
              SELECT 1 FROM rebate_tiers
              WHERE rebate_tiers.min_qty = order_lines.quantity
            )
            """;
        var plan = StrictSqlSubset.ParseLogicalPlan(sql);
        var scan = Assert.IsType<LogicalTableScan>(plan.Root);
        var pred = Assert.Single(scan.SubqueryPredicates!);
        var inner = Assert.IsType<LogicalTableScan>(pred.Subquery.Root);
        var w = Assert.Single(inner.WhereConjuncts!);
        Assert.NotNull(w.CompareColumn);

        var orders = new MemoryTable("order_lines", new TableSchema([new ColumnDef("quantity", RainDbType.Int32)]));
        var tiers = new MemoryTable("rebate_tiers", new TableSchema([new ColumnDef("min_qty", RainDbType.Int32)]));
        var catalog = new InMemoryCatalog();
        catalog.Register(orders);
        catalog.Register(tiers);
        var physical = StrictSqlSubset.CompilePhysicalPlan(sql, catalog);
        var v = Assert.IsType<VectorizedScanPhysicalPlan>(physical);
        var ex = Assert.Single(v.ExistsSubqueries!);
        Assert.NotNull(ex.Correlations);
        Assert.NotEmpty(ex.Correlations!);
    }

    [Fact]
    public async Task Correlated_exists_filters_outer_rows()
    {
        var engine = RainDbEngine.CreateDefault();
        var orders = new MemoryTable("order_lines", new TableSchema([
            new ColumnDef("quantity", RainDbType.Int32),
        ]));
        orders.AppendBatch(Int32Batch([5, 15, 20]));
        var tiers = new MemoryTable("rebate_tiers", new TableSchema([
            new ColumnDef("min_qty", RainDbType.Int32),
        ]));
        tiers.AppendBatch(Int32Batch([6, 12, 20]));
        engine.Catalog.Register(orders);
        engine.Catalog.Register(tiers);

        await using var r = await engine.ExecuteSqlAsync("""
            SELECT quantity FROM order_lines
            WHERE EXISTS (
              SELECT 1 FROM rebate_tiers
              WHERE rebate_tiers.min_qty = order_lines.quantity
            )
            ORDER BY quantity
            """);
        Assert.Equal([20], ReadInt32Column(r, 0));
    }

    [Fact]
    public async Task Correlated_in_filters_outer_rows()
    {
        var engine = RainDbEngine.CreateDefault();
        var orders = new MemoryTable("orders", new TableSchema([
            new ColumnDef("region", RainDbType.Int32),
        ]));
        orders.AppendBatch(Int32Batch([1, 2, 3]));
        var allowed = new MemoryTable("allowed_regions", new TableSchema([
            new ColumnDef("region", RainDbType.Int32),
            new ColumnDef("tier", RainDbType.Int32),
        ]));
        allowed.AppendBatch(new ColumnarBatch(3, [
            Int32Chunk([1, 2, 99]),
            Int32Chunk([1, 2, 3]),
        ]));
        engine.Catalog.Register(orders);
        engine.Catalog.Register(allowed);

        await using var r = await engine.ExecuteSqlAsync("""
            SELECT region FROM orders
            WHERE region IN (
              SELECT allowed_regions.region FROM allowed_regions
              WHERE allowed_regions.tier = orders.region
            )
            ORDER BY region
            """);
        Assert.Equal([1, 2], ReadInt32Column(r, 0));
    }

    [Fact]
    public async Task Full_outer_join_emits_unmatched_from_both_sides()
    {
        var engine = RainDbEngine.CreateDefault();
        var left = new MemoryTable("L", new TableSchema([new ColumnDef("id", RainDbType.Int32)]));
        var right = new MemoryTable("R", new TableSchema([
            new ColumnDef("id", RainDbType.Int32),
            new ColumnDef("b", RainDbType.Int64),
        ]));
        left.AppendBatch(new ColumnarBatch(2, [Int32Chunk([1, 2])]));
        right.AppendBatch(new ColumnarBatch(2, [Int32Chunk([1, 3]), Int64Chunk([9L, 7L])]));
        engine.Catalog.Register(left);
        engine.Catalog.Register(right);

        await using var r = await engine.ExecuteSqlAsync(
            "SELECT L.id, R.id FROM L FULL OUTER JOIN R ON L.id = R.id");
        var colR = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(3, colR.RowCount);
        var leftIds = ReadNullableInt32Column(colR, 0);
        var rightIds = ReadNullableInt32Column(colR, 1);
        Assert.Contains((1, 1), ZipPairs(leftIds, rightIds));
        Assert.Contains((2, 0), ZipPairs(leftIds, rightIds));
        Assert.Contains((0, 3), ZipPairs(leftIds, rightIds));
    }

    [Fact]
    public void Parse_full_outer_join()
    {
        var plan = StrictSqlSubset.ParseLogicalPlan("SELECT * FROM L FULL OUTER JOIN R ON L.id = R.id");
        var join = Assert.IsType<LogicalInnerJoin>(plan.Root);
        Assert.Equal(LogicalJoinSemantics.FullOuter, join.Semantics);
    }

    [Fact]
    public async Task Mixed_union_and_union_all_is_left_associative()
    {
        var engine = RainDbEngine.CreateDefault();
        var a = new MemoryTable("a", new TableSchema([new ColumnDef("x", RainDbType.Int32)]));
        a.AppendBatch(Int32Batch([1, 2]));
        var b = new MemoryTable("b", new TableSchema([new ColumnDef("x", RainDbType.Int32)]));
        b.AppendBatch(Int32Batch([2, 3]));
        var c = new MemoryTable("c", new TableSchema([new ColumnDef("x", RainDbType.Int32)]));
        c.AppendBatch(Int32Batch([3, 4]));
        engine.Catalog.Register(a);
        engine.Catalog.Register(b);
        engine.Catalog.Register(c);

        await using var r = await engine.ExecuteSqlAsync(
            "SELECT x FROM a UNION ALL SELECT x FROM b UNION SELECT x FROM c ORDER BY x");
        Assert.Equal([1, 2, 3, 4], ReadInt32Column(r, 0));
    }

    [Fact]
    public async Task Distinct_treats_nulls_as_equal()
    {
        var engine = RainDbEngine.CreateDefault();
        var t = new MemoryTable("t", new TableSchema([new ColumnDef("x", RainDbType.Int32)]));
        t.AppendBatch(new ColumnarBatch(2, [
            new FixedWidthColumnChunk(RainDbType.Int32, 2, new byte[8], new byte[] { 0b_0000_0011 }, true),
        ]));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT DISTINCT x FROM t");
        Assert.Equal(1, Assert.IsAssignableFrom<IColumnarQueryResult>(r).RowCount);
    }

    [Fact]
    public async Task Count_distinct_int64_column()
    {
        var engine = RainDbEngine.CreateDefault();
        var t = new MemoryTable("t", new TableSchema([
            new ColumnDef("g", RainDbType.Int32),
            new ColumnDef("v", RainDbType.Int64),
        ]));
        t.AppendBatch(new ColumnarBatch(3, [Int32Chunk([1, 1, 1]), Int64Chunk([10L, 10L, 20L])]));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT g, COUNT(DISTINCT v) FROM t GROUP BY g");
        Assert.Equal([2L], ReadInt64Column(r, 1));
    }

    [Fact]
    public async Task Table_alias_in_from_clause()
    {
        var engine = TestDataBuilders.CreateEngine();
        TestDataBuilders.RegisterAnalyticsDemoTables(engine);
        await using var r = await engine.ExecuteSqlAsync("SELECT o.region FROM order_lines o ORDER BY o.region LIMIT 1");
        Assert.Equal(1, Assert.IsAssignableFrom<IColumnarQueryResult>(r).RowCount);
    }

    [Fact]
    public async Task Correlated_not_exists_filters_outer_rows()
    {
        var engine = RainDbEngine.CreateDefault();
        var orders = new MemoryTable("order_lines", new TableSchema([
            new ColumnDef("quantity", RainDbType.Int32),
        ]));
        orders.AppendBatch(Int32Batch([6, 7, 20]));
        var tiers = new MemoryTable("rebate_tiers", new TableSchema([
            new ColumnDef("min_qty", RainDbType.Int32),
        ]));
        tiers.AppendBatch(Int32Batch([6, 20]));
        engine.Catalog.Register(orders);
        engine.Catalog.Register(tiers);

        await using var r = await engine.ExecuteSqlAsync("""
            SELECT quantity FROM order_lines
            WHERE NOT EXISTS (
              SELECT 1 FROM rebate_tiers
              WHERE rebate_tiers.min_qty = order_lines.quantity
            )
            ORDER BY quantity
            """);
        Assert.Equal([7], ReadInt32Column(r, 0));
    }

    [Fact]
    public async Task Demo_select_distinct_region()
    {
        var engine = TestDataBuilders.CreateEngine();
        TestDataBuilders.RegisterAnalyticsDemoTables(engine);
        await using var r = await engine.ExecuteSqlAsync("SELECT DISTINCT region FROM order_lines ORDER BY region");
        Assert.Equal(3, Assert.IsAssignableFrom<IColumnarQueryResult>(r).RowCount);
    }

    [Fact]
    public async Task Demo_count_distinct_quantity_by_region()
    {
        var engine = TestDataBuilders.CreateEngine();
        TestDataBuilders.RegisterAnalyticsDemoTables(engine);
        await using var r = await engine.ExecuteSqlAsync(
            "SELECT region, COUNT(DISTINCT quantity) FROM order_lines GROUP BY region ORDER BY region");
        var colR = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(3, colR.RowCount);
        Assert.Equal([2L, 3L, 2L], ReadInt64Column(r, 1));
    }

    private static void RegisterLR(RainDbEngine engine, int[] leftIds, int[] rightIds, long[] rightB)
    {
        var left = new MemoryTable("L", new TableSchema([new ColumnDef("id", RainDbType.Int32)]));
        var right = new MemoryTable("R", new TableSchema([
            new ColumnDef("id", RainDbType.Int32),
            new ColumnDef("b", RainDbType.Int64),
        ]));
        left.AppendBatch(new ColumnarBatch(leftIds.Length, [Int32Chunk(leftIds)]));
        right.AppendBatch(new ColumnarBatch(rightIds.Length, [Int32Chunk(rightIds), Int64Chunk(rightB)]));
        engine.Catalog.Register(left);
        engine.Catalog.Register(right);
    }

    private static HashSet<(int L, int R)> ZipPairs(List<int> left, List<int> right)
    {
        var set = new HashSet<(int, int)>();
        for (var i = 0; i < left.Count; i++)
            set.Add((left[i], right[i]));
        return set;
    }

    private static List<int> ReadNullableInt32Column(IColumnarQueryResult colR, int col)
    {
        var vals = new List<int>();
        foreach (var batch in colR.Batches)
        {
            var c = batch.Columns[col];
            var span = c.Values.Span;
            var nb = c.HasNulls ? c.NullBitmap.Span : ReadOnlySpan<byte>.Empty;
            for (var i = 0; i < batch.RowCount; i++)
            {
                if (c.HasNulls && (nb[i >> 3] & (1 << (i & 7))) != 0)
                {
                    vals.Add(0);
                    continue;
                }

                vals.Add(BinaryPrimitives.ReadInt32LittleEndian(span.Slice(i * sizeof(int), sizeof(int))));
            }
        }

        return vals;
    }

    private static List<int> ReadInt32Column(IQueryResult r, int col)
    {
        var colR = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        var vals = new List<int>();
        foreach (var batch in colR.Batches)
        {
            var span = batch.Columns[col].Values.Span;
            for (var i = 0; i < batch.RowCount; i++)
                vals.Add(BinaryPrimitives.ReadInt32LittleEndian(span.Slice(i * sizeof(int), sizeof(int))));
        }

        return vals;
    }

    private static List<long> ReadInt64Column(IQueryResult r, int col)
    {
        var colR = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        var vals = new List<long>();
        foreach (var batch in colR.Batches)
        {
            var span = batch.Columns[col].Values.Span;
            for (var i = 0; i < batch.RowCount; i++)
                vals.Add(BinaryPrimitives.ReadInt64LittleEndian(span.Slice(i * sizeof(long), sizeof(long))));
        }

        return vals;
    }

    private static ColumnarBatch Int32Batch(int[] values) =>
        new(values.Length, [Int32Chunk(values)]);

    private static FixedWidthColumnChunk Int32Chunk(int[] values)
    {
        var bytes = new byte[values.Length * sizeof(int)];
        for (var i = 0; i < values.Length; i++)
            BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(i * sizeof(int), sizeof(int)), values[i]);
        return new FixedWidthColumnChunk(RainDbType.Int32, values.Length, bytes, ReadOnlyMemory<byte>.Empty, false);
    }

    private static FixedWidthColumnChunk Int64Chunk(long[] values)
    {
        var bytes = new byte[values.Length * sizeof(long)];
        for (var i = 0; i < values.Length; i++)
            BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(i * sizeof(long), sizeof(long)), values[i]);
        return new FixedWidthColumnChunk(RainDbType.Int64, values.Length, bytes, ReadOnlyMemory<byte>.Empty, false);
    }
}
