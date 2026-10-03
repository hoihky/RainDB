using System.Buffers.Binary;
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

/// <summary>Parser/compile guards: malformed or edge-case SQL must fail cleanly, not throw unexpectedly.</summary>
public class SqlFuzzRobustnessTests
{
    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("SELECT")]
    [InlineData("SELECT FROM t")]
    [InlineData("SELECT * FROM")]
    [InlineData("SELECT * FROM t WHERE")]
    [InlineData("SELECT * FROM t GROUP BY")]
    [InlineData("SELECT * FROM t UNION")]
    public void Malformed_sql_throws_compile_exception(string sql)
    {
        Assert.ThrowsAny<Exception>(() => StrictSqlSubset.ParseLogicalPlan(sql));
    }

    [Theory]
    [InlineData("SELECT * FROM missing_table")]
    [InlineData("SELECT nocol FROM t")]
    public void Compile_with_bad_catalog_throws_compile_exception(string sql)
    {
        var cat = new InMemoryCatalog();
        cat.Register(new MemoryTable("t", new TableSchema([new ColumnDef("k", RainDbType.Int32)])));
        Assert.Throws<SqlCompileException>(() => StrictSqlSubset.CompilePhysicalPlan(sql, cat));
    }

    [Fact]
    public async Task Join_group_by_where_in_filters_before_aggregate()
    {
        var engine = RainDbEngine.CreateDefault();
        RegisterJoinLR(engine, [1, 2], [1, 2], [10L, 20L]);
        engine.Catalog.Register(LookupTable([2]));

        await using var r = await engine.ExecuteSqlAsync("""
            SELECT L.id, SUM(R.amt) FROM L INNER JOIN R ON L.id = R.id
            WHERE L.id IN (SELECT id FROM lookup)
            GROUP BY L.id
            """);
        Assert.Equal([2], ReadInt32Column(r, 0));
        Assert.Equal([20L], ReadInt64Column(r, 1));
    }

    [Fact]
    public async Task Join_group_by_with_order_by_limit()
    {
        var engine = RainDbEngine.CreateDefault();
        RegisterJoinLR(engine, [1, 2, 3], [1, 2, 3], [10L, 20L, 30L]);

        await using var r = await engine.ExecuteSqlAsync("""
            SELECT L.id, SUM(R.amt) FROM L INNER JOIN R ON L.id = R.id
            GROUP BY L.id ORDER BY L.id DESC LIMIT 2
            """);
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(2, col.RowCount);
        Assert.Equal([3, 2], ReadInt32Column(r, 0));
    }

    [Fact]
    public async Task Derived_table_where_in_subquery()
    {
        var engine = RainDbEngine.CreateDefault();
        engine.Catalog.Register(ItemsTable([1, 2, 3], [10, 20, 30]));
        engine.Catalog.Register(LookupTable([2, 3]));

        await using var r = await engine.ExecuteSqlAsync("""
            SELECT d.id FROM (SELECT id FROM items) d
            WHERE d.id IN (SELECT id FROM lookup)
            ORDER BY d.id
            """);
        Assert.Equal([2, 3], ReadInt32Column(r, 0));
    }

    [Fact]
    public void Join_group_by_with_table_alias_binds()
    {
        var cat = new InMemoryCatalog();
        cat.Register(new MemoryTable("L", new TableSchema([new ColumnDef("id", RainDbType.Int32)])));
        cat.Register(new MemoryTable("R", new TableSchema([
            new ColumnDef("id", RainDbType.Int32),
            new ColumnDef("amt", RainDbType.Int64),
        ])));
        var plan = StrictSqlSubset.CompilePhysicalPlan(
            "SELECT l.id, SUM(r.amt) FROM L l INNER JOIN R r ON l.id = r.id GROUP BY l.id",
            cat);
        Assert.IsAssignableFrom<GroupedJoinPhysicalPlan>(plan);
    }

    private static void RegisterJoinLR(RainDbEngine engine, int[] leftIds, int[] rightIds, long[] amts)
    {
        var left = new MemoryTable("L", new TableSchema([new ColumnDef("id", RainDbType.Int32)]));
        var right = new MemoryTable("R", new TableSchema([
            new ColumnDef("id", RainDbType.Int32),
            new ColumnDef("amt", RainDbType.Int64),
        ]));
        left.AppendBatch(new ColumnarBatch(leftIds.Length, [Int32Chunk(leftIds)]));
        right.AppendBatch(new ColumnarBatch(rightIds.Length, [Int32Chunk(rightIds), Int64Chunk(amts)]));
        engine.Catalog.Register(left);
        engine.Catalog.Register(right);
    }

    private static MemoryTable LookupTable(int[] ids)
    {
        var t = new MemoryTable("lookup", new TableSchema([new ColumnDef("id", RainDbType.Int32)]));
        if (ids.Length > 0)
            t.AppendBatch(new ColumnarBatch(ids.Length, [Int32Chunk(ids)]));
        return t;
    }

    private static MemoryTable ItemsTable(int[] ids, int[] qty)
    {
        var t = new MemoryTable("items", new TableSchema([
            new ColumnDef("id", RainDbType.Int32),
            new ColumnDef("qty", RainDbType.Int32),
        ]));
        t.AppendBatch(new ColumnarBatch(ids.Length, [Int32Chunk(ids), Int32Chunk(qty)]));
        return t;
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
