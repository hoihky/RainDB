using System.Buffers.Binary;
using RainDB;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Core.Tables;
using RainDB.Execution;
using RainDB.Schema;
using RainDB.Sql;

namespace RainDB.Tests;

/// <summary>Cross-cutting SQL compile/execute guards (WHERE, GROUP BY, subqueries).</summary>
public class SqlRobustnessTests
{
    [Fact]
    public void Column_to_column_where_on_scan_is_rejected_at_compile()
    {
        var t = new MemoryTable("t", new TableSchema([
            new ColumnDef("a", RainDbType.Int32),
            new ColumnDef("b", RainDbType.Int32),
        ]));
        var cat = new RainDB.Core.Catalog.InMemoryCatalog();
        cat.Register(t);
        var ex = Assert.Throws<SqlCompileException>(() =>
            StrictSqlSubset.CompilePhysicalPlan("SELECT a FROM t WHERE t.a = t.b", cat));
        Assert.Contains("correlated", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Group_by_where_in_subquery_filters_input_rows()
    {
        var engine = RainDbEngine.CreateDefault();
        var t = new MemoryTable("t", new TableSchema([
            new ColumnDef("g", RainDbType.Int32),
            new ColumnDef("v", RainDbType.Int32),
        ]));
        t.AppendBatch(new ColumnarBatch(4, [
            Int32Chunk([1, 1, 2, 2]),
            Int32Chunk([10, 20, 30, 40]),
        ]));
        var lookup = new MemoryTable("lookup", new TableSchema([new ColumnDef("v", RainDbType.Int32)]));
        lookup.AppendBatch(new ColumnarBatch(2, [Int32Chunk([20, 40])]));
        engine.Catalog.Register(t);
        engine.Catalog.Register(lookup);

        await using var r = await engine.ExecuteSqlAsync(
            "SELECT g, SUM(v) FROM t WHERE v IN (SELECT v FROM lookup) GROUP BY g ORDER BY g");
        Assert.Equal([1, 2], ReadInt32Column(r, 0));
        Assert.Equal([20L, 40L], ReadInt64Column(r, 1));
    }

    [Fact]
    public async Task Group_by_correlated_exists_is_rejected_at_compile()
    {
        var engine = RainDbEngine.CreateDefault();
        var t = new MemoryTable("t", new TableSchema([new ColumnDef("g", RainDbType.Int32)]));
        t.AppendBatch(new ColumnarBatch(1, [Int32Chunk([1])]));
        engine.Catalog.Register(t);
        var u = new MemoryTable("u", new TableSchema([new ColumnDef("x", RainDbType.Int32)]));
        u.AppendBatch(new ColumnarBatch(1, [Int32Chunk([1])]));
        engine.Catalog.Register(u);

        var ex = await Assert.ThrowsAsync<SqlCompileException>(async () =>
        {
            await using var _ = await engine.ExecuteSqlAsync("""
                SELECT g, COUNT(*) FROM t
                WHERE EXISTS (SELECT 1 FROM u WHERE u.x = t.g)
                GROUP BY g
                """);
        });
        Assert.Contains("GROUP BY", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Utf8_group_by_having_filters_groups()
    {
        var engine = RainDbEngine.CreateDefault();
        var t = new MemoryTable("t", new TableSchema([
            new ColumnDef("region", RainDbType.Utf8),
            new ColumnDef("v", RainDbType.Int32),
        ]));
        t.AppendBatch(new ColumnarBatch(3, [
            Utf8Chunk(["A", "A", "B"]),
            Int32Chunk([1, 9, 5]),
        ]));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync(
            "SELECT region, SUM(v) FROM t GROUP BY region HAVING SUM(v) > 5 ORDER BY region");
        Assert.Equal(1, Assert.IsAssignableFrom<IColumnarQueryResult>(r).RowCount);
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

    private static Utf8ColumnChunk Utf8Chunk(string[] values)
    {
        var offsets = new int[values.Length + 1];
        var blob = new List<byte>();
        for (var i = 0; i < values.Length; i++)
        {
            offsets[i] = blob.Count;
            blob.AddRange(System.Text.Encoding.UTF8.GetBytes(values[i]));
        }

        offsets[values.Length] = blob.Count;
        return new Utf8ColumnChunk(values.Length, offsets, blob.ToArray(), ReadOnlyMemory<byte>.Empty, false);
    }
}
