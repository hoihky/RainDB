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

public class PhaseD1D2Tests
{
    [Fact]
    public async Task Select_int32_arithmetic_expression()
    {
        var engine = RainDbEngine.CreateDefault();
        var t = new MemoryTable("t", new TableSchema([new ColumnDef("a", RainDbType.Int32)]));
        t.AppendBatch(SingleInt32(7));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT a + 3 FROM t");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(10, BinaryPrimitives.ReadInt32LittleEndian(col.Batches[0].Columns[0].Values.Span));
    }

    [Fact]
    public async Task Where_int32_expression_predicate()
    {
        var engine = RainDbEngine.CreateDefault();
        var t = new MemoryTable("t", new TableSchema([
            new ColumnDef("a", RainDbType.Int32),
            new ColumnDef("b", RainDbType.Int32),
        ]));
        var a = new byte[] { 1, 0, 0, 0, 5, 0, 0, 0 };
        var b = new byte[] { 2, 0, 0, 0, 1, 0, 0, 0 };
        t.AppendBatch(new ColumnarBatch(2, [
            new FixedWidthColumnChunk(RainDbType.Int32, 2, a, ReadOnlyMemory<byte>.Empty, false),
            new FixedWidthColumnChunk(RainDbType.Int32, 2, b, ReadOnlyMemory<byte>.Empty, false),
        ]));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT a FROM t WHERE a + b > 4");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(1, col.RowCount);
        Assert.Equal(5, BinaryPrimitives.ReadInt32LittleEndian(col.Batches[0].Columns[0].Values.Span));
    }

    [Fact]
    public async Task Case_scalar_in_select()
    {
        var engine = RainDbEngine.CreateDefault();
        var t = new MemoryTable("t", new TableSchema([new ColumnDef("x", RainDbType.Int32)]));
        t.AppendBatch(SingleInt32(0));
        t.AppendBatch(SingleInt32(5));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync(
            "SELECT CASE WHEN x < 1 THEN 100 ELSE 200 END FROM t");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(2, col.RowCount);
        var values = new HashSet<int>();
        foreach (var batch in col.Batches)
        {
            var span = batch.Columns[0].Values.Span;
            for (var i = 0; i < batch.RowCount; i++)
                values.Add(BinaryPrimitives.ReadInt32LittleEndian(span.Slice(i * sizeof(int), sizeof(int))));
        }

        Assert.Contains(100, values);
        Assert.Contains(200, values);
    }

    [Fact]
    public async Task Having_filters_groups_by_aggregate()
    {
        var engine = RainDbEngine.CreateDefault();
        var t = new MemoryTable("m", new TableSchema([
            new ColumnDef("k", RainDbType.Int32),
            new ColumnDef("v", RainDbType.Int64),
        ]));
        var kb = new byte[] { 1, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0 };
        var vb = new byte[24];
        BinaryPrimitives.WriteInt64LittleEndian(vb.AsSpan(0, 8), 10);
        BinaryPrimitives.WriteInt64LittleEndian(vb.AsSpan(8, 8), 20);
        BinaryPrimitives.WriteInt64LittleEndian(vb.AsSpan(16, 8), 100);
        t.AppendBatch(new ColumnarBatch(3, [
            new FixedWidthColumnChunk(RainDbType.Int32, 3, kb, ReadOnlyMemory<byte>.Empty, false),
            new FixedWidthColumnChunk(RainDbType.Int64, 3, vb, ReadOnlyMemory<byte>.Empty, false),
        ]));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync(
            "SELECT k, SUM(v) FROM m GROUP BY k HAVING SUM(v) > 50");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(1, col.Batches[0].RowCount);
        Assert.Equal(2, BinaryPrimitives.ReadInt32LittleEndian(col.Batches[0].Columns[0].Values.Span));
        Assert.Equal(100L, BinaryPrimitives.ReadInt64LittleEndian(col.Batches[0].Columns[1].Values.Span));
    }

    [Fact]
    public async Task Min_max_int32_per_group()
    {
        var engine = RainDbEngine.CreateDefault();
        var t = new MemoryTable("t", new TableSchema([
            new ColumnDef("k", RainDbType.Int32),
            new ColumnDef("v", RainDbType.Int32),
        ]));
        var kb = new byte[] { 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0 };
        var vb = new byte[] { 9, 0, 0, 0, 3, 0, 0, 0, 7, 0, 0, 0 };
        t.AppendBatch(new ColumnarBatch(3, [
            new FixedWidthColumnChunk(RainDbType.Int32, 3, kb, ReadOnlyMemory<byte>.Empty, false),
            new FixedWidthColumnChunk(RainDbType.Int32, 3, vb, ReadOnlyMemory<byte>.Empty, false),
        ]));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync(
            "SELECT k, MIN(v), MAX(v) FROM t GROUP BY k");
        var batch = Assert.IsAssignableFrom<IColumnarQueryResult>(r).Batches[0];
        Assert.Equal(1, batch.RowCount);
        Assert.Equal(3, BinaryPrimitives.ReadInt32LittleEndian(batch.Columns[1].Values.Span));
        Assert.Equal(9, BinaryPrimitives.ReadInt32LittleEndian(batch.Columns[2].Values.Span));
    }

    [Fact]
    public async Task Min_max_utf8_lexicographic()
    {
        var engine = RainDbEngine.CreateDefault();
        var t = new MemoryTable("t", new TableSchema([
            new ColumnDef("k", RainDbType.Int32),
            new ColumnDef("s", RainDbType.Utf8),
        ]));
        var kb = new byte[] { 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0 };
        var utf = Utf8Chunk("zebra", "apple", "mango");
        t.AppendBatch(new ColumnarBatch(3, [
            new FixedWidthColumnChunk(RainDbType.Int32, 3, kb, ReadOnlyMemory<byte>.Empty, false),
            utf,
        ]));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT k, MIN(s), MAX(s) FROM t GROUP BY k");
        var batch = Assert.IsAssignableFrom<IColumnarQueryResult>(r).Batches[0];
        Assert.Equal(1, batch.RowCount);
        Assert.Equal("apple", ReadUtf8(batch.Columns[1], 0));
        Assert.Equal("zebra", ReadUtf8(batch.Columns[2], 0));
    }

    [Fact]
    public void CompilePhysicalPlan_having_binds_filters()
    {
        var cat = new InMemoryCatalog();
        cat.Register(new MemoryTable("m", new TableSchema([
            new ColumnDef("k", RainDbType.Int32),
            new ColumnDef("v", RainDbType.Int64),
        ])));

        var plan = StrictSqlSubset.CompilePhysicalPlan(
            "SELECT k, SUM(v) FROM m GROUP BY k HAVING SUM(v) > 0", cat);
        var ha = Assert.IsType<HashAggregatePhysicalPlan>(plan);
        Assert.NotNull(ha.HavingFilters);
        Assert.Single(ha.HavingFilters);
    }

    [Fact]
    public async Task Order_by_int32_expression()
    {
        var engine = RainDbEngine.CreateDefault();
        var t = new MemoryTable("t", new TableSchema([new ColumnDef("x", RainDbType.Int32)]));
        t.AppendBatch(new ColumnarBatch(3, [
            new FixedWidthColumnChunk(RainDbType.Int32, 3, new byte[] { 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0 }, ReadOnlyMemory<byte>.Empty, false),
        ]));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT x FROM t ORDER BY x + 10 DESC LIMIT 1");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(1, col.RowCount);
        Assert.Equal(3, BinaryPrimitives.ReadInt32LittleEndian(col.Batches[0].Columns[0].Values.Span));
    }

    private static Utf8ColumnChunk Utf8Chunk(params string[] rows)
    {
        var offsets = new int[rows.Length + 1];
        var blob = new List<byte>();
        for (var i = 0; i < rows.Length; i++)
        {
            offsets[i] = blob.Count;
            blob.AddRange(Encoding.UTF8.GetBytes(rows[i]));
        }

        offsets[rows.Length] = blob.Count;
        return new Utf8ColumnChunk(rows.Length, offsets, blob.ToArray(), ReadOnlyMemory<byte>.Empty, false);
    }

    private static ColumnarBatch SingleInt32(int v) =>
        new(1, [new FixedWidthColumnChunk(
            RainDbType.Int32,
            1,
            BitConverter.GetBytes(v),
            ReadOnlyMemory<byte>.Empty,
            false)]);

    private static string ReadUtf8(IColumnChunk col, int row)
    {
        ReadOnlySpan<byte> span = col switch
        {
            Utf8ColumnChunk u => u.Values.Span[u.Offsets.Span[row]..u.Offsets.Span[row + 1]],
            Utf8LengthPrefixedColumnChunk lp => lp.GetPayloadSpan(row),
            _ => throw new InvalidOperationException(),
        };
        return Encoding.UTF8.GetString(span);
    }
}
