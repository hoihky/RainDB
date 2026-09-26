using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Core.Tables;
using RainDB.Execution;
using RainDB.Query.Execution.Sorting;
using RainDB.Query.Plans;
using RainDB.Query.Vectorized;
using RainDB.Schema;

namespace RainDB.Tests;

public class SortTopNHeapTests
{
    private static readonly SelectionEvaluator Selection = new();

    [Fact]
    public void BoundedTopKHeap_selects_k_smallest_integers()
    {
        var schema = new TableSchema([new ColumnDef("k", RainDbType.Int32)]);
        var table = BuildInt32Table("t", schema, Enumerable.Range(0, 20).Reverse().ToArray());
        var batches = table.Batches;
        var comparer = new SchemaRowLocationComparer(
            schema,
            [new SortKeyPhysicalSpec(0, Descending: false)],
            batches,
            Selection);
        var all = AllRowLocations(batches);
        var top = new BoundedTopKHeap().Select(all, 3, comparer);
        Array.Sort(top, comparer);
        Assert.Equal(3, top.Length);
        Assert.Equal(0, ReadKey(batches, top[0]));
        Assert.Equal(1, ReadKey(batches, top[1]));
        Assert.Equal(2, ReadKey(batches, top[2]));
    }

    [Fact]
    public void BoundedTopKHeap_selects_k_largest_when_descending()
    {
        var schema = new TableSchema([new ColumnDef("k", RainDbType.Int32)]);
        var table = BuildInt32Table("t", schema, Enumerable.Range(0, 15).ToArray());
        var batches = table.Batches;
        var comparer = new SchemaRowLocationComparer(
            schema,
            [new SortKeyPhysicalSpec(0, Descending: true)],
            batches,
            Selection);
        var all = AllRowLocations(batches);
        var top = new BoundedTopKHeap().Select(all, 2, comparer);
        Array.Sort(top, comparer);
        Assert.Equal(2, top.Length);
        Assert.Equal(14, ReadKey(batches, top[0]));
        Assert.Equal(13, ReadKey(batches, top[1]));
    }

    [Fact]
    public void SortTopNRowSelection_heap_path_matches_full_sort_for_limit()
    {
        var schema = new TableSchema([
            new ColumnDef("a", RainDbType.Int32),
            new ColumnDef("b", RainDbType.Int32),
        ]);
        var values = new (int a, int b)[100];
        for (var i = 0; i < values.Length; i++)
            values[i] = (i, i % 7);

        var table = BuildTwoIntTable("t", schema, values);
        var batches = table.Batches;
        var keys = new[]
        {
            new SortKeyPhysicalSpec(0, false),
            new SortKeyPhysicalSpec(1, true),
        };
        const int k = 7;
        var all = AllRowLocations(batches);
        var reference = (RowLocation[])all.Clone();
        var comparer = new SchemaRowLocationComparer(schema, keys, batches, Selection);
        Array.Sort(reference, comparer);
        var expected = reference.AsSpan(0, k).ToArray();

        var selector = new SortTopNRowSelector(Selection);
        var actual = selector.SelectInSortOrder((RowLocation[])all.Clone(), keys, k, schema, batches);

        Assert.Equal(expected.Length, actual.Length);
        for (var i = 0; i < expected.Length; i++)
        {
            Assert.Equal(ReadA(batches, expected[i]), ReadA(batches, actual[i]));
            Assert.Equal(ReadB(batches, expected[i]), ReadB(batches, actual[i]));
        }
    }

    [Fact]
    public async Task Large_table_order_by_limit_returns_correct_top_values()
    {
        const int n = 5000;
        const int k = 5;
        var engine = TestDataBuilders.CreateEngine();
        var schema = new TableSchema([new ColumnDef("score", RainDbType.Int32)]);
        var table = new MemoryTable("scores", schema);
        var bytes = new byte[n * sizeof(int)];
        for (var i = 0; i < n; i++)
            System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(i * 4, 4), i);
        table.AppendBatch(new ColumnarBatch(n, [
            new FixedWidthColumnChunk(RainDbType.Int32, n, bytes, ReadOnlyMemory<byte>.Empty, false),
        ]));
        engine.Catalog.Register(table);

        await using var r = await engine.ExecuteSqlAsync($"SELECT score FROM scores ORDER BY score DESC LIMIT {k}");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(k, col.RowCount);
        var chunk = col.Batches[0].Columns[0];
        for (var i = 0; i < k; i++)
        {
            var expected = n - 1 - i;
            var actual = System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(
                chunk.Values.Span.Slice(i * 4, 4));
            Assert.Equal(expected, actual);
        }
    }

    [Fact]
    public async Task Order_by_without_limit_still_returns_full_sorted_set()
    {
        var engine = TestDataBuilders.CreateEngine();
        var schema = new TableSchema([new ColumnDef("k", RainDbType.Int32)]);
        var table = BuildInt32Table("m", schema, [3, 1, 2]);
        engine.Catalog.Register(table);

        await using var r = await engine.ExecuteSqlAsync("SELECT k FROM m ORDER BY k ASC");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(3, col.RowCount);
        var v = col.Batches[0].Columns[0].Values.Span;
        Assert.Equal(1, System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(v));
        Assert.Equal(2, System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(v.Slice(4)));
        Assert.Equal(3, System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(v.Slice(8)));
    }

    [Fact]
    public async Task Limit_without_order_by_preserves_batch_row_order()
    {
        var engine = TestDataBuilders.CreateEngine();
        var schema = new TableSchema([new ColumnDef("k", RainDbType.Int32)]);
        var table = new MemoryTable("m", schema);
        table.AppendBatch(TestDataBuilders.SingleInt32Batch(10));
        table.AppendBatch(TestDataBuilders.SingleInt32Batch(20));
        table.AppendBatch(TestDataBuilders.SingleInt32Batch(30));
        engine.Catalog.Register(table);

        await using var r = await engine.ExecuteSqlAsync("SELECT k FROM m LIMIT 2");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(2, col.RowCount);
        Assert.Equal(10, TestDataBuilders.ReadInt32(col.Batches[0].Columns[0], 0));
        Assert.Equal(20, TestDataBuilders.ReadInt32(col.Batches[0].Columns[0], 1));
    }

    [Fact]
    public async Task Limit_larger_than_row_count_returns_all_rows_sorted()
    {
        var engine = TestDataBuilders.CreateEngine();
        var table = BuildInt32Table("m", new TableSchema([new ColumnDef("k", RainDbType.Int32)]), [5, 1, 4]);
        engine.Catalog.Register(table);

        await using var r = await engine.ExecuteSqlAsync("SELECT k FROM m ORDER BY k LIMIT 10");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(3, col.RowCount);
    }

    [Fact]
    public async Task Utf8_order_by_limit_lexicographic()
    {
        var engine = TestDataBuilders.CreateEngine();
        var schema = new TableSchema([new ColumnDef("name", RainDbType.Utf8)]);
        var table = new MemoryTable("t", schema);
        table.AppendBatch(new ColumnarBatch(4, [
            TestDataBuilders.Utf8Column(["pear", "apple", "banana", "apricot"]),
        ]));
        engine.Catalog.Register(table);

        await using var r = await engine.ExecuteSqlAsync("SELECT name FROM t ORDER BY name LIMIT 2");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(2, col.RowCount);
        var u = (Utf8ColumnChunk)col.Batches[0].Columns[0];
        Assert.Equal("apple", Utf8At(u, 0));
        Assert.Equal("apricot", Utf8At(u, 1));
    }

    [Fact]
    public async Task Order_by_float64_with_nulls_limit()
    {
        var engine = TestDataBuilders.CreateEngine();
        var schema = new TableSchema([new ColumnDef("v", RainDbType.Float64)]);
        var vals = new byte[24];
        TestDataBuilders.WriteF64(vals, 0, 3.0);
        TestDataBuilders.WriteF64(vals, 8, 1.0);
        TestDataBuilders.WriteF64(vals, 16, 2.0);
        var nb = new byte[] { 0b0000_0010 };
        var table = new MemoryTable("t", schema);
        table.AppendBatch(new ColumnarBatch(3, [
            new FixedWidthColumnChunk(RainDbType.Float64, 3, vals, nb, hasNulls: true),
        ]));
        engine.Catalog.Register(table);

        await using var r = await engine.ExecuteSqlAsync("SELECT v FROM t ORDER BY v LIMIT 2");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(2, col.RowCount);
        var chunk = col.Batches[0].Columns[0];
        Assert.True(chunk.HasNulls);
    }

    [Fact]
    public async Task Where_and_order_by_limit_combined()
    {
        var engine = TestDataBuilders.CreateEngine();
        var schema = new TableSchema([new ColumnDef("k", RainDbType.Int32), new ColumnDef("tag", RainDbType.Int32)]);
        var table = BuildTwoIntTable("t", schema, [(1, 0), (2, 1), (3, 1), (4, 1), (5, 0)]);
        engine.Catalog.Register(table);

        await using var r = await engine.ExecuteSqlAsync(
            "SELECT k FROM t WHERE tag = 1 ORDER BY k DESC LIMIT 2");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(2, col.RowCount);
        Assert.Equal(4, TestDataBuilders.ReadInt32(col.Batches[0].Columns[0], 0));
        Assert.Equal(3, TestDataBuilders.ReadInt32(col.Batches[0].Columns[0], 1));
    }

    private static MemoryTable BuildInt32Table(string name, TableSchema schema, int[] values)
    {
        var table = new MemoryTable(name, schema);
        table.AppendBatch(new ColumnarBatch(values.Length, [TestDataBuilders.Int32Column(values)]));
        return table;
    }

    private static MemoryTable BuildTwoIntTable(string name, TableSchema schema, (int a, int b)[] rows)
    {
        var a = new int[rows.Length];
        var b = new int[rows.Length];
        for (var i = 0; i < rows.Length; i++)
        {
            a[i] = rows[i].a;
            b[i] = rows[i].b;
        }

        var table = new MemoryTable(name, schema);
        table.AppendBatch(new ColumnarBatch(rows.Length, [
            TestDataBuilders.Int32Column(a),
            TestDataBuilders.Int32Column(b),
        ]));
        return table;
    }

    private static RowLocation[] AllRowLocations(IReadOnlyList<IColumnarBatch> batches)
    {
        var list = new List<RowLocation>();
        for (var bi = 0; bi < batches.Count; bi++)
        {
            for (var r = 0; r < batches[bi].RowCount; r++)
                list.Add(new RowLocation(bi, r));
        }

        return list.ToArray();
    }

    private static int ReadKey(IReadOnlyList<IColumnarBatch> batches, RowLocation loc) =>
        TestDataBuilders.ReadInt32(batches[loc.BatchIndex].Columns[0], loc.RowIndex);

    private static int ReadA(IReadOnlyList<IColumnarBatch> batches, RowLocation loc) =>
        TestDataBuilders.ReadInt32(batches[loc.BatchIndex].Columns[0], loc.RowIndex);

    private static int ReadB(IReadOnlyList<IColumnarBatch> batches, RowLocation loc) =>
        TestDataBuilders.ReadInt32(batches[loc.BatchIndex].Columns[1], loc.RowIndex);

    private static string Utf8At(Utf8ColumnChunk chunk, int row)
    {
        var start = chunk.Offsets.Span[row];
        var end = chunk.Offsets.Span[row + 1];
        return System.Text.Encoding.UTF8.GetString(chunk.Values.Span.Slice(start, end - start));
    }
}
