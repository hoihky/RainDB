using System.Buffers.Binary;
using System.Text;
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

public class PhaseD3Tests
{
    [Fact]
    public void Parse_left_join_sets_semantics()
    {
        var plan = StrictSqlSubset.ParseLogicalPlan(
            "SELECT L.id, R.b FROM L LEFT JOIN R ON L.id = R.id");
        var join = Assert.IsType<LogicalInnerJoin>(plan.Root);
        Assert.Equal(LogicalJoinSemantics.LeftOuter, join.Semantics);
    }

    [Fact]
    public async Task Left_join_preserves_unmatched_probe_rows()
    {
        var engine = RainDbEngine.CreateDefault();
        RegisterLR(engine, leftIds: [1, 2], rightIds: [1], rightB: [100L]);

        await using var r = await engine.ExecuteSqlAsync(
            "SELECT L.id, R.b FROM L LEFT JOIN R ON L.id = R.id ORDER BY L.id");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(2, col.RowCount);
        var ids = col.Batches[0].Columns[0].Values.Span;
        Assert.Equal(1, BinaryPrimitives.ReadInt32LittleEndian(ids));
        Assert.Equal(2, BinaryPrimitives.ReadInt32LittleEndian(ids.Slice(4, 4)));
        var bCol = col.Batches[0].Columns[1];
        Assert.True(bCol.HasNulls);
        Assert.False(IsNullRow(bCol.NullBitmap.Span, 0));
        Assert.True(IsNullRow(bCol.NullBitmap.Span, 1));
        Assert.Equal(100L, BinaryPrimitives.ReadInt64LittleEndian(bCol.Values.Span));
    }

    [Fact]
    public async Task Left_join_utf8_key_nulls_right_columns()
    {
        var engine = RainDbEngine.CreateDefault();
        var left = new MemoryTable("L", new TableSchema([
            new ColumnDef("k", RainDbType.Utf8),
            new ColumnDef("v", RainDbType.Int32),
        ]));
        left.AppendBatch(new ColumnarBatch(2, [
            Utf8Chunk("a", "z"),
            Int32Chunk([1, 2]),
        ]));
        var right = new MemoryTable("R", new TableSchema([
            new ColumnDef("k", RainDbType.Utf8),
        ]));
        right.AppendBatch(new ColumnarBatch(1, [Utf8Chunk("a")]));
        engine.Catalog.Register(left);
        engine.Catalog.Register(right);

        await using var r = await engine.ExecuteSqlAsync("SELECT L.k, R.k FROM L LEFT JOIN R ON L.k = R.k");
        var batch = Assert.IsAssignableFrom<IColumnarQueryResult>(r).Batches[0];
        Assert.Equal(2, batch.RowCount);
        Assert.Equal("z", ReadUtf8(batch.Columns[0], 1));
        Assert.True(batch.Columns[1].HasNulls);
        Assert.True(IsNullRow(batch.Columns[1].NullBitmap.Span, 1));
    }

    [Fact]
    public void CompilePhysicalPlan_left_join_carries_semantics()
    {
        var engine = RainDbEngine.CreateDefault();
        RegisterLR(engine, [1], [2], [9L]);
        var plan = StrictSqlSubset.CompilePhysicalPlan(
            "SELECT * FROM L LEFT JOIN R ON L.id = R.id", engine.Catalog);
        var semantics = plan switch
        {
            JoinPhysicalPlan j => j.Semantics,
            JoinSortTopNPhysicalPlan jst => jst.Join.Semantics,
            _ => throw new InvalidOperationException($"Unexpected plan {plan.GetType().Name}"),
        };
        Assert.Equal(LogicalJoinSemantics.LeftOuter, semantics);
    }

    [Fact]
    public void Parse_union_all_builds_logical_union()
    {
        var plan = StrictSqlSubset.ParseLogicalPlan(
            "SELECT x FROM a UNION ALL SELECT x FROM b");
        var union = Assert.IsType<LogicalUnionAll>(plan.Root);
        Assert.Equal(2, union.Branches.Count);
        Assert.IsType<LogicalTableScan>(union.Branches[0]);
    }

    [Fact]
    public void Union_distinct_parses_without_all_keyword()
    {
        var plan = StrictSqlSubset.ParseLogicalPlan("SELECT x FROM a UNION SELECT x FROM b");
        var union = Assert.IsType<LogicalUnionAll>(plan.Root);
        Assert.False(union.UnionAll);
    }

    [Fact]
    public async Task Union_all_concatenates_row_sets()
    {
        var engine = RainDbEngine.CreateDefault();
        var t1 = new MemoryTable("a", new TableSchema([new ColumnDef("x", RainDbType.Int32)]));
        t1.AppendBatch(Int32Batch([1, 2]));
        var t2 = new MemoryTable("b", new TableSchema([new ColumnDef("x", RainDbType.Int32)]));
        t2.AppendBatch(Int32Batch([3]));
        engine.Catalog.Register(t1);
        engine.Catalog.Register(t2);

        await using var r = await engine.ExecuteSqlAsync("SELECT x FROM a UNION ALL SELECT x FROM b");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(3, col.RowCount);
        var vals = new List<int>();
        foreach (var batch in col.Batches)
        {
            var span = batch.Columns[0].Values.Span;
            for (var i = 0; i < batch.RowCount; i++)
                vals.Add(BinaryPrimitives.ReadInt32LittleEndian(span.Slice(i * sizeof(int), sizeof(int))));
        }

        Assert.Equal([1, 2, 3], vals);
    }

    [Fact]
    public void Union_all_rejects_incompatible_types()
    {
        var cat = new InMemoryCatalog();
        cat.Register(new MemoryTable("a", new TableSchema([new ColumnDef("x", RainDbType.Int32)])));
        cat.Register(new MemoryTable("b", new TableSchema([new ColumnDef("x", RainDbType.Int64)])));
        Assert.Throws<SqlCompileException>(() =>
            StrictSqlSubset.CompilePhysicalPlan("SELECT x FROM a UNION ALL SELECT x FROM b", cat));
    }

    [Fact]
    public void CompilePhysicalPlan_union_all_produces_union_operator()
    {
        var cat = new InMemoryCatalog();
        cat.Register(new MemoryTable("a", new TableSchema([new ColumnDef("x", RainDbType.Int32)])));
        cat.Register(new MemoryTable("b", new TableSchema([new ColumnDef("y", RainDbType.Int32)])));
        var plan = StrictSqlSubset.CompilePhysicalPlan("SELECT x FROM a UNION ALL SELECT y FROM b", cat);
        var union = Assert.IsType<UnionAllPhysicalPlan>(plan);
        Assert.Equal(2, union.Inputs.Length);
    }

    private static void RegisterLR(RainDbEngine engine, int[] leftIds, int[] rightIds, long[] rightB)
    {
        var left = new MemoryTable("L", new TableSchema([
            new ColumnDef("id", RainDbType.Int32),
            new ColumnDef("a", RainDbType.Int64),
        ]));
        var right = new MemoryTable("R", new TableSchema([
            new ColumnDef("id", RainDbType.Int32),
            new ColumnDef("b", RainDbType.Int64),
        ]));
        left.AppendBatch(new ColumnarBatch(leftIds.Length, [
            Int32Chunk(leftIds),
            new FixedWidthColumnChunk(RainDbType.Int64, leftIds.Length, new byte[leftIds.Length * 8], ReadOnlyMemory<byte>.Empty, false),
        ]));
        right.AppendBatch(new ColumnarBatch(rightIds.Length, [
            Int32Chunk(rightIds),
            Int64Chunk(rightB),
        ]));
        engine.Catalog.Register(left);
        engine.Catalog.Register(right);
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

    private static bool IsNullRow(ReadOnlySpan<byte> nullBitmap, int row) =>
        (nullBitmap[row >> 3] & (byte)(1 << (row & 7))) != 0;

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
