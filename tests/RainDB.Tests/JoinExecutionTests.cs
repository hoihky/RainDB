using System.Buffers.Binary;
using System.Text;
using RainDB;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Core.Tables;
using RainDB.Execution;
using RainDB.Logical;
using RainDB.Query.Plans;
using RainDB.Schema;
using RainDB.Sql;
using RainDB.Sql.Compilation;

namespace RainDB.Tests;

public class JoinExecutionTests
{
    [Fact]
    public void Parse_inner_join_logical()
    {
        var plan = StrictSqlSubset.ParseLogicalPlan("SELECT * FROM a INNER JOIN b ON a.x = b.y");
        var join = Assert.IsType<LogicalInnerJoin>(plan.Root);
        Assert.Equal("a", join.LeftTableName);
        Assert.Equal("b", join.RightTableName);
        Assert.Single(join.LeftKeyColumns);
        Assert.Equal("x", join.LeftKeyColumns[0].ColumnName);
        Assert.Equal("y", join.RightKeyColumns[0].ColumnName);
    }

    [Fact]
    public void Parse_inner_join_swapped_on_operands()
    {
        var plan = StrictSqlSubset.ParseLogicalPlan("SELECT * FROM a INNER JOIN b ON b.y = a.x");
        var join = Assert.IsType<LogicalInnerJoin>(plan.Root);
        Assert.Equal("x", join.LeftKeyColumns[0].ColumnName);
        Assert.Equal("y", join.RightKeyColumns[0].ColumnName);
    }

    [Fact]
    public async Task ExecuteSql_inner_join_matches_rows()
    {
        var engine = RainDbEngine.CreateDefault();
        var leftSchema = new TableSchema([
            new ColumnDef("id", RainDbType.Int32),
            new ColumnDef("a", RainDbType.Int64),
        ]);
        var rightSchema = new TableSchema([
            new ColumnDef("id", RainDbType.Int32),
            new ColumnDef("b", RainDbType.Int64),
        ]);
        var left = new MemoryTable("L", leftSchema);
        var right = new MemoryTable("R", rightSchema);

        var lid = new byte[8];
        BinaryPrimitives.WriteInt32LittleEndian(lid.AsSpan(0, 4), 1);
        BinaryPrimitives.WriteInt32LittleEndian(lid.AsSpan(4, 4), 2);
        var la = new byte[16];
        left.AppendBatch(new ColumnarBatch(2, new IColumnChunk[]
        {
            new FixedWidthColumnChunk(RainDbType.Int32, 2, lid, ReadOnlyMemory<byte>.Empty, false),
            new FixedWidthColumnChunk(RainDbType.Int64, 2, la, ReadOnlyMemory<byte>.Empty, false),
        }));

        var rid = new byte[8];
        BinaryPrimitives.WriteInt32LittleEndian(rid.AsSpan(0, 4), 1);
        BinaryPrimitives.WriteInt32LittleEndian(rid.AsSpan(4, 4), 3);
        var rb = new byte[16];
        right.AppendBatch(new ColumnarBatch(2, new IColumnChunk[]
        {
            new FixedWidthColumnChunk(RainDbType.Int32, 2, rid, ReadOnlyMemory<byte>.Empty, false),
            new FixedWidthColumnChunk(RainDbType.Int64, 2, rb, ReadOnlyMemory<byte>.Empty, false),
        }));

        engine.Catalog.Register(left);
        engine.Catalog.Register(right);

        await using var r = await engine.ExecuteSqlAsync("SELECT * FROM L INNER JOIN R ON L.id = R.id");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(1, col.RowCount);
        Assert.Single(col.Batches);
        Assert.Equal(4, col.Batches[0].Columns.Count);
    }

    [Fact]
    public async Task ExecuteSql_inner_join_explicit_select_list()
    {
        var engine = RainDbEngine.CreateDefault();
        var leftSchema = new TableSchema([
            new ColumnDef("id", RainDbType.Int32),
            new ColumnDef("a", RainDbType.Int64),
        ]);
        var rightSchema = new TableSchema([
            new ColumnDef("id", RainDbType.Int32),
            new ColumnDef("b", RainDbType.Int64),
        ]);
        var left = new MemoryTable("L", leftSchema);
        var right = new MemoryTable("R", rightSchema);

        var lid = new byte[8];
        BinaryPrimitives.WriteInt32LittleEndian(lid.AsSpan(0, 4), 1);
        BinaryPrimitives.WriteInt32LittleEndian(lid.AsSpan(4, 4), 2);
        var la = new byte[16];
        left.AppendBatch(new ColumnarBatch(2, new IColumnChunk[]
        {
            new FixedWidthColumnChunk(RainDbType.Int32, 2, lid, ReadOnlyMemory<byte>.Empty, false),
            new FixedWidthColumnChunk(RainDbType.Int64, 2, la, ReadOnlyMemory<byte>.Empty, false),
        }));

        var rid = new byte[8];
        BinaryPrimitives.WriteInt32LittleEndian(rid.AsSpan(0, 4), 1);
        BinaryPrimitives.WriteInt32LittleEndian(rid.AsSpan(4, 4), 3);
        var rb = new byte[16];
        right.AppendBatch(new ColumnarBatch(2, new IColumnChunk[]
        {
            new FixedWidthColumnChunk(RainDbType.Int32, 2, rid, ReadOnlyMemory<byte>.Empty, false),
            new FixedWidthColumnChunk(RainDbType.Int64, 2, rb, ReadOnlyMemory<byte>.Empty, false),
        }));

        engine.Catalog.Register(left);
        engine.Catalog.Register(right);

        await using var r = await engine.ExecuteSqlAsync("SELECT L.id, R.id FROM L INNER JOIN R ON L.id = R.id");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(1, col.RowCount);
        Assert.Single(col.Batches);
        Assert.Equal(2, col.Batches[0].Columns.Count);
        var lcol = (FixedWidthColumnChunk)col.Batches[0].Columns[0];
        var rcol = (FixedWidthColumnChunk)col.Batches[0].Columns[1];
        Assert.Equal(1, BinaryPrimitives.ReadInt32LittleEndian(lcol.Values.Span));
        Assert.Equal(1, BinaryPrimitives.ReadInt32LittleEndian(rcol.Values.Span));
    }

    [Fact]
    public async Task SortMerge_join_matches_same_as_hash()
    {
        var engine = RainDbEngine.CreateDefault();
        var leftSchema = new TableSchema([new ColumnDef("k", RainDbType.Int32)]);
        var rightSchema = new TableSchema([new ColumnDef("k", RainDbType.Int32)]);
        var left = new MemoryTable("A", leftSchema);
        var right = new MemoryTable("B", rightSchema);

        var lv = new byte[12];
        BinaryPrimitives.WriteInt32LittleEndian(lv.AsSpan(0, 4), 3);
        BinaryPrimitives.WriteInt32LittleEndian(lv.AsSpan(4, 4), 1);
        BinaryPrimitives.WriteInt32LittleEndian(lv.AsSpan(8, 4), 2);
        left.AppendBatch(new ColumnarBatch(3, new IColumnChunk[]
        {
            new FixedWidthColumnChunk(RainDbType.Int32, 3, lv, ReadOnlyMemory<byte>.Empty, false),
        }));

        var rv = new byte[8];
        BinaryPrimitives.WriteInt32LittleEndian(rv.AsSpan(0, 4), 2);
        BinaryPrimitives.WriteInt32LittleEndian(rv.AsSpan(4, 4), 2);
        right.AppendBatch(new ColumnarBatch(2, new IColumnChunk[]
        {
            new FixedWidthColumnChunk(RainDbType.Int32, 2, rv, ReadOnlyMemory<byte>.Empty, false),
        }));

        engine.Catalog.Register(left);
        engine.Catalog.Register(right);

        var logical = StrictSqlSubset.ParseLogicalPlan("SELECT * FROM A INNER JOIN B ON A.k = B.k");
        var join = Assert.IsType<LogicalInnerJoin>(logical.Root);
        var hashPlan = LogicalJoinBinder.BindAndLower(join, engine.Catalog, PhysicalJoinAlgorithm.Hash);
        var sortPlan = LogicalJoinBinder.BindAndLower(join, engine.Catalog, PhysicalJoinAlgorithm.SortMerge);

        await using var rh = await engine.ExecutePhysicalAsync(hashPlan);
        await using var rs = await engine.ExecutePhysicalAsync(sortPlan);
        var ch = Assert.IsAssignableFrom<IColumnarQueryResult>(rh);
        var cs = Assert.IsAssignableFrom<IColumnarQueryResult>(rs);
        Assert.Equal(ch.RowCount, cs.RowCount);
        Assert.Equal(2, ch.RowCount);
    }

    [Fact]
    public async Task Inner_join_where_filters_probe_side()
    {
        var engine = RainDbEngine.CreateDefault();
        var leftSchema = new TableSchema([
            new ColumnDef("id", RainDbType.Int32),
            new ColumnDef("qty", RainDbType.Int32),
        ]);
        var rightSchema = new TableSchema([new ColumnDef("id", RainDbType.Int32)]);
        var left = new MemoryTable("L", leftSchema);
        var right = new MemoryTable("R", rightSchema);

        var lid = new byte[8];
        BinaryPrimitives.WriteInt32LittleEndian(lid.AsSpan(0, 4), 1);
        BinaryPrimitives.WriteInt32LittleEndian(lid.AsSpan(4, 4), 2);
        var lqty = new byte[8];
        BinaryPrimitives.WriteInt32LittleEndian(lqty.AsSpan(0, 4), 1);
        BinaryPrimitives.WriteInt32LittleEndian(lqty.AsSpan(4, 4), 5);
        left.AppendBatch(new ColumnarBatch(2, new IColumnChunk[]
        {
            new FixedWidthColumnChunk(RainDbType.Int32, 2, lid, ReadOnlyMemory<byte>.Empty, false),
            new FixedWidthColumnChunk(RainDbType.Int32, 2, lqty, ReadOnlyMemory<byte>.Empty, false),
        }));

        var rid = new byte[8];
        BinaryPrimitives.WriteInt32LittleEndian(rid.AsSpan(0, 4), 1);
        BinaryPrimitives.WriteInt32LittleEndian(rid.AsSpan(4, 4), 2);
        right.AppendBatch(new ColumnarBatch(2, new IColumnChunk[]
        {
            new FixedWidthColumnChunk(RainDbType.Int32, 2, rid, ReadOnlyMemory<byte>.Empty, false),
        }));

        engine.Catalog.Register(left);
        engine.Catalog.Register(right);

        await using var r = await engine.ExecuteSqlAsync(
            "SELECT * FROM L INNER JOIN R ON L.id = R.id WHERE L.qty > 1");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(1, col.RowCount);
        var batch = col.Batches[0];
        var idCol = (FixedWidthColumnChunk)batch.Columns[0];
        Assert.Equal(2, BinaryPrimitives.ReadInt32LittleEndian(idCol.Values.Span));
    }

    [Fact]
    public async Task Inner_join_preserves_utf8_arrow_column()
    {
        var engine = RainDbEngine.CreateDefault();
        var leftSchema = new TableSchema([
            new ColumnDef("id", RainDbType.Int32),
            new ColumnDef("label", RainDbType.Utf8),
        ]);
        var rightSchema = new TableSchema([new ColumnDef("id", RainDbType.Int32)]);
        var left = new MemoryTable("L", leftSchema);
        var right = new MemoryTable("R", rightSchema);

        var lid = new byte[8];
        BinaryPrimitives.WriteInt32LittleEndian(lid.AsSpan(0, 4), 1);
        BinaryPrimitives.WriteInt32LittleEndian(lid.AsSpan(4, 4), 2);
        left.AppendBatch(new ColumnarBatch(2, new IColumnChunk[]
        {
            new FixedWidthColumnChunk(RainDbType.Int32, 2, lid, ReadOnlyMemory<byte>.Empty, false),
            Utf8Chunk("hello", "world"),
        }));

        var rid = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(rid, 2);
        right.AppendBatch(new ColumnarBatch(1, new IColumnChunk[]
        {
            new FixedWidthColumnChunk(RainDbType.Int32, 1, rid, ReadOnlyMemory<byte>.Empty, false),
        }));

        engine.Catalog.Register(left);
        engine.Catalog.Register(right);

        await using var r = await engine.ExecuteSqlAsync("SELECT * FROM L INNER JOIN R ON L.id = R.id");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(1, col.RowCount);
        var utf8 = Assert.IsType<Utf8ColumnChunk>(col.Batches[0].Columns[1]);
        var span = utf8.Values.Span;
        var start = utf8.Offsets.Span[0];
        var end = utf8.Offsets.Span[1];
        Assert.Equal("world", Encoding.UTF8.GetString(span.Slice(start, end - start)));
    }

    [Fact]
    public void Join_where_ambiguous_column_requires_qualifier()
    {
        var engine = RainDbEngine.CreateDefault();
        var s = new TableSchema([new ColumnDef("id", RainDbType.Int32)]);
        engine.Catalog.Register(new MemoryTable("L", s));
        engine.Catalog.Register(new MemoryTable("R", s));

        var ex = Assert.Throws<SqlCompileException>(() =>
            StrictSqlSubset.CompilePhysicalPlan(
                "SELECT * FROM L INNER JOIN R ON L.id = R.id WHERE id > 0",
                engine.Catalog));
        Assert.Contains("ambiguous", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Inner_join_utf8_equi_key_matches_rows()
    {
        var engine = RainDbEngine.CreateDefault();
        var leftSchema = new TableSchema([
            new ColumnDef("tag", RainDbType.Utf8),
            new ColumnDef("lid", RainDbType.Int32),
        ]);
        var rightSchema = new TableSchema([
            new ColumnDef("tag", RainDbType.Utf8),
            new ColumnDef("rid", RainDbType.Int32),
        ]);
        var left = new MemoryTable("A", leftSchema);
        var right = new MemoryTable("B", rightSchema);

        left.AppendBatch(new ColumnarBatch(2, new IColumnChunk[]
        {
            Utf8Chunk("east", "west"),
            Int32Chunk([1, 2]),
        }));
        right.AppendBatch(new ColumnarBatch(2, new IColumnChunk[]
        {
            Utf8Chunk("west", "east"),
            Int32Chunk([30, 40]),
        }));

        engine.Catalog.Register(left);
        engine.Catalog.Register(right);

        await using var r = await engine.ExecuteSqlAsync(
            "SELECT A.lid, B.rid FROM A INNER JOIN B ON A.tag = B.tag");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(2, col.RowCount);
    }

    [Fact]
    public async Task SortMerge_utf8_join_matches_hash()
    {
        var engine = RainDbEngine.CreateDefault();
        var leftSchema = new TableSchema([
            new ColumnDef("tag", RainDbType.Utf8),
            new ColumnDef("lid", RainDbType.Int32),
        ]);
        var rightSchema = new TableSchema([
            new ColumnDef("tag", RainDbType.Utf8),
            new ColumnDef("rid", RainDbType.Int32),
        ]);
        var left = new MemoryTable("A", leftSchema);
        var right = new MemoryTable("B", rightSchema);

        left.AppendBatch(new ColumnarBatch(2, new IColumnChunk[]
        {
            Utf8Chunk("east", "west"),
            Int32Chunk([1, 2]),
        }));
        right.AppendBatch(new ColumnarBatch(2, new IColumnChunk[]
        {
            Utf8Chunk("west", "east"),
            Int32Chunk([30, 40]),
        }));

        engine.Catalog.Register(left);
        engine.Catalog.Register(right);

        var logical = StrictSqlSubset.ParseLogicalPlan("SELECT * FROM A INNER JOIN B ON A.tag = B.tag");
        var join = Assert.IsType<LogicalInnerJoin>(logical.Root);
        var hashPlan = LogicalJoinBinder.BindAndLower(join, engine.Catalog, PhysicalJoinAlgorithm.Hash);
        var sortPlan = LogicalJoinBinder.BindAndLower(join, engine.Catalog, PhysicalJoinAlgorithm.SortMerge);

        await using var rh = await engine.ExecutePhysicalAsync(hashPlan);
        await using var rs = await engine.ExecutePhysicalAsync(sortPlan);
        var ch = Assert.IsAssignableFrom<IColumnarQueryResult>(rh);
        var cs = Assert.IsAssignableFrom<IColumnarQueryResult>(rs);
        Assert.Equal(ch.RowCount, cs.RowCount);
        Assert.Equal(2, ch.RowCount);
    }

    [Fact]
    public async Task Inner_join_composite_int_and_utf8_keys()
    {
        var engine = RainDbEngine.CreateDefault();
        var leftSchema = new TableSchema([
            new ColumnDef("kid", RainDbType.Int32),
            new ColumnDef("tag", RainDbType.Utf8),
        ]);
        var rightSchema = new TableSchema([
            new ColumnDef("kid", RainDbType.Int32),
            new ColumnDef("tag", RainDbType.Utf8),
        ]);
        var left = new MemoryTable("A", leftSchema);
        var right = new MemoryTable("B", rightSchema);

        left.AppendBatch(new ColumnarBatch(2, new IColumnChunk[]
        {
            Int32Chunk([1, 2]),
            Utf8Chunk("p", "q"),
        }));
        right.AppendBatch(new ColumnarBatch(2, new IColumnChunk[]
        {
            Int32Chunk([1, 2]),
            Utf8Chunk("p", "q"),
        }));

        engine.Catalog.Register(left);
        engine.Catalog.Register(right);

        await using var r = await engine.ExecuteSqlAsync(
            "SELECT A.kid FROM A INNER JOIN B ON A.kid = B.kid AND A.tag = B.tag");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(2, col.RowCount);
    }

    [Fact]
    public async Task Inner_join_group_by_sum_utf8_and_int_key()
    {
        var engine = RainDbEngine.CreateDefault();
        var leftSchema = new TableSchema([
            new ColumnDef("id", RainDbType.Int32),
            new ColumnDef("tag", RainDbType.Utf8),
        ]);
        var rightSchema = new TableSchema([
            new ColumnDef("id", RainDbType.Int32),
            new ColumnDef("amt", RainDbType.Int64),
        ]);
        var left = new MemoryTable("L", leftSchema);
        var right = new MemoryTable("R", rightSchema);

        left.AppendBatch(new ColumnarBatch(2, new IColumnChunk[]
        {
            Int32Chunk([1, 2]),
            Utf8Chunk("east", "west"),
        }));

        var rid = new byte[12];
        BinaryPrimitives.WriteInt32LittleEndian(rid.AsSpan(0, 4), 1);
        BinaryPrimitives.WriteInt32LittleEndian(rid.AsSpan(4, 4), 1);
        BinaryPrimitives.WriteInt32LittleEndian(rid.AsSpan(8, 4), 2);
        var amts = new byte[24];
        BinaryPrimitives.WriteInt64LittleEndian(amts.AsSpan(0, 8), 10);
        BinaryPrimitives.WriteInt64LittleEndian(amts.AsSpan(8, 8), 20);
        BinaryPrimitives.WriteInt64LittleEndian(amts.AsSpan(16, 8), 5);
        right.AppendBatch(new ColumnarBatch(3, new IColumnChunk[]
        {
            new FixedWidthColumnChunk(RainDbType.Int32, 3, rid, ReadOnlyMemory<byte>.Empty, false),
            new FixedWidthColumnChunk(RainDbType.Int64, 3, amts, ReadOnlyMemory<byte>.Empty, false),
        }));

        engine.Catalog.Register(left);
        engine.Catalog.Register(right);

        await using var r = await engine.ExecuteSqlAsync(
            "SELECT L.id, L.tag, SUM(R.amt) FROM L INNER JOIN R ON L.id = R.id GROUP BY L.id, L.tag");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Single(col.Batches);
        Assert.Equal(2, col.Batches[0].RowCount);
        long sumAgg = 0;
        for (var row = 0; row < col.Batches[0].RowCount; row++)
            sumAgg += BinaryPrimitives.ReadInt64LittleEndian(col.Batches[0].Columns[2].Values.Span.Slice(row * sizeof(long), sizeof(long)));
        Assert.Equal(35L, sumAgg);
    }

    private static Utf8ColumnChunk Utf8Chunk(params string[] rows)
    {
        var offsets = new int[rows.Length + 1];
        var blob = new List<byte>();
        for (var i = 0; i < rows.Length; i++)
        {
            offsets[i] = blob.Count;
            foreach (var b in Encoding.UTF8.GetBytes(rows[i]))
                blob.Add(b);
        }

        offsets[^1] = blob.Count;
        return new Utf8ColumnChunk(rows.Length, offsets, blob.ToArray(), ReadOnlyMemory<byte>.Empty, false);
    }

    private static FixedWidthColumnChunk Int32Chunk(int[] values)
    {
        var bytes = new byte[values.Length * sizeof(int)];
        for (var i = 0; i < values.Length; i++)
            BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(i * sizeof(int)), values[i]);
        return new FixedWidthColumnChunk(RainDbType.Int32, values.Length, bytes, ReadOnlyMemory<byte>.Empty, false);
    }
}
