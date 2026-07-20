using System.Buffers.Binary;
using System.Text;
using RainDB;
using RainDB.Catalog;
using RainDB.Columnar;
using RainDB.Core.Catalog;
using RainDB.Core.Columnar;
using RainDB.Core.Tables;
using RainDB.Execution;
using RainDB.Logical;
using RainDB.Schema;
using RainDB.Sql;

namespace RainDB.Tests;

public class SqlStrictSubsetCompilerTests
{
    [Fact]
    public void ParseLogicalPlan_star_where_explain_contains_table()
    {
        var sql = "SELECT * FROM sales WHERE qty > 1";
        var plan = StrictSqlSubset.ParseLogicalPlan(sql);
        var e = plan.Explain();
        Assert.Contains("sales", e, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("qty", e, StringComparison.OrdinalIgnoreCase);
        var scan = Assert.IsType<LogicalTableScan>(plan.Root);
        Assert.Null(scan.Projection);
        Assert.NotNull(scan.WhereConjuncts);
        Assert.Single(scan.WhereConjuncts);
    }

    [Fact]
    public void ParseLogicalPlan_aggregate_sum()
    {
        var plan = StrictSqlSubset.ParseLogicalPlan("SELECT SUM(amount) FROM sales");
        var scan = Assert.IsType<LogicalTableScan>(plan.Root);
        Assert.NotNull(scan.Aggregate);
        Assert.Equal(AggregateKind.Sum, scan.Aggregate!.Kind);
        Assert.Equal("amount", scan.Aggregate.ColumnName);
    }

    [Fact]
    public void ParseLogicalPlan_qualified_where_table()
    {
        var plan = StrictSqlSubset.ParseLogicalPlan("SELECT * FROM sales WHERE sales.qty > 1");
        var scan = Assert.IsType<LogicalTableScan>(plan.Root);
        Assert.NotNull(scan.WhereConjuncts);
        Assert.Single(scan.WhereConjuncts);
        var w = scan.WhereConjuncts![0];
        Assert.Equal("sales", w.QualifierTableName);
        Assert.Equal("qty", w.ColumnName);
    }

    [Fact]
    public void ParseLogicalPlan_where_and_chain()
    {
        var plan = StrictSqlSubset.ParseLogicalPlan("SELECT * FROM sales WHERE qty > 1 AND amt < 100");
        var scan = Assert.IsType<LogicalTableScan>(plan.Root);
        Assert.NotNull(scan.WhereConjuncts);
        Assert.Equal(2, scan.WhereConjuncts!.Count);
        Assert.Equal("qty", scan.WhereConjuncts[0].ColumnName);
        Assert.Equal("amt", scan.WhereConjuncts[1].ColumnName);
    }

    [Fact]
    public void ParseLogicalPlan_utf8_where_literal()
    {
        var plan = StrictSqlSubset.ParseLogicalPlan("SELECT * FROM t WHERE name = 'hello''world'");
        var scan = Assert.IsType<LogicalTableScan>(plan.Root);
        Assert.Single(scan.WhereConjuncts!);
        Assert.Equal(SqlLiteralKind.String, scan.WhereConjuncts![0].Literal.Kind);
        Assert.Equal("hello'world", scan.WhereConjuncts[0].Literal.Text);
    }

    [Fact]
    public void ParseLogicalPlan_group_by_preserves_where_conjuncts()
    {
        var plan = StrictSqlSubset.ParseLogicalPlan("SELECT k, SUM(v) FROM m WHERE k > 0 GROUP BY k");
        var scan = Assert.IsType<LogicalTableScan>(plan.Root);
        Assert.NotNull(scan.WhereConjuncts);
        Assert.Single(scan.WhereConjuncts);
        Assert.Equal("k", scan.WhereConjuncts![0].ColumnName);
    }

    [Fact]
    public void ParseLogicalPlan_join_explicit_columns()
    {
        var plan = StrictSqlSubset.ParseLogicalPlan("SELECT a.x, b.y FROM a INNER JOIN b ON a.k = b.k");
        var join = Assert.IsType<LogicalInnerJoin>(plan.Root);
        Assert.NotNull(join.SelectProjection);
        Assert.Equal(2, join.SelectProjection!.Count);
        Assert.Equal("x", join.SelectProjection[0].ColumnName);
        Assert.Equal("a", join.SelectProjection[0].QualifierTableName);
        Assert.Equal("y", join.SelectProjection[1].ColumnName);
        Assert.Equal("b", join.SelectProjection[1].QualifierTableName);
    }

    [Fact]
    public void CompilePhysicalPlan_unknown_table_throws()
    {
        var cat = new InMemoryCatalog();
        Assert.Throws<SqlCompileException>(() =>
            StrictSqlSubset.CompilePhysicalPlan("SELECT * FROM missing", cat));
    }

    [Fact]
    public void ParseLogicalPlan_utf8_where_ne_angle_brackets()
    {
        var plan = StrictSqlSubset.ParseLogicalPlan("SELECT * FROM t WHERE name <> 'x'");
        var scan = Assert.IsType<LogicalTableScan>(plan.Root);
        Assert.Single(scan.WhereConjuncts!);
        Assert.Equal(ScalarCompareOp.Ne, scan.WhereConjuncts![0].Operator);
    }

    [Fact]
    public async Task ExecuteSqlAsync_utf8_where_eq()
    {
        var engine = RainDbEngine.CreateDefault();
        var schema = new TableSchema([
            new ColumnDef("label", RainDbType.Utf8),
            new ColumnDef("n", RainDbType.Int32),
        ]);
        var t = new MemoryTable("u", schema);
        var nb = new byte[12];
        BinaryPrimitives.WriteInt32LittleEndian(nb.AsSpan(0, 4), 1);
        BinaryPrimitives.WriteInt32LittleEndian(nb.AsSpan(4, 4), 2);
        BinaryPrimitives.WriteInt32LittleEndian(nb.AsSpan(8, 4), 3);
        t.AppendBatch(new ColumnarBatch(3, new IColumnChunk[]
        {
            Utf8Col(["a", "b", "a"]),
            new FixedWidthColumnChunk(RainDbType.Int32, 3, nb, ReadOnlyMemory<byte>.Empty, false),
        }));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT n FROM u WHERE label = 'a'");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(2, col.RowCount);
    }

    [Fact]
    public async Task ExecuteSqlAsync_utf8_where_ne_angle_brackets()
    {
        var engine = RainDbEngine.CreateDefault();
        var schema = new TableSchema([
            new ColumnDef("label", RainDbType.Utf8),
            new ColumnDef("n", RainDbType.Int32),
        ]);
        var t = new MemoryTable("u", schema);
        var nb = new byte[12];
        BinaryPrimitives.WriteInt32LittleEndian(nb.AsSpan(0, 4), 1);
        BinaryPrimitives.WriteInt32LittleEndian(nb.AsSpan(4, 4), 2);
        BinaryPrimitives.WriteInt32LittleEndian(nb.AsSpan(8, 4), 3);
        t.AppendBatch(new ColumnarBatch(3, new IColumnChunk[]
        {
            Utf8Col(["a", "b", "a"]),
            new FixedWidthColumnChunk(RainDbType.Int32, 3, nb, ReadOnlyMemory<byte>.Empty, false),
        }));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT n FROM u WHERE label <> 'a'");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(1, col.RowCount);
        Assert.Equal(2, BinaryPrimitives.ReadInt32LittleEndian(col.Batches[0].Columns[0].Values.Span));
    }

    [Fact]
    public async Task ExecuteSqlAsync_utf8_where_ne_bang_equals()
    {
        var engine = RainDbEngine.CreateDefault();
        var schema = new TableSchema([
            new ColumnDef("label", RainDbType.Utf8),
            new ColumnDef("n", RainDbType.Int32),
        ]);
        var t = new MemoryTable("u", schema);
        var nb = new byte[12];
        BinaryPrimitives.WriteInt32LittleEndian(nb.AsSpan(0, 4), 1);
        BinaryPrimitives.WriteInt32LittleEndian(nb.AsSpan(4, 4), 2);
        BinaryPrimitives.WriteInt32LittleEndian(nb.AsSpan(8, 4), 3);
        t.AppendBatch(new ColumnarBatch(3, new IColumnChunk[]
        {
            Utf8Col(["a", "b", "a"]),
            new FixedWidthColumnChunk(RainDbType.Int32, 3, nb, ReadOnlyMemory<byte>.Empty, false),
        }));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT n FROM u WHERE label != 'a'");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(1, col.RowCount);
    }

    [Fact]
    public async Task ExecuteSqlAsync_where_and_chain()
    {
        var engine = RainDbEngine.CreateDefault();
        var schema = new TableSchema([
            new ColumnDef("k", RainDbType.Int32),
            new ColumnDef("v", RainDbType.Int32),
        ]);
        var t = new MemoryTable("m", schema);
        var kb = new byte[12];
        BinaryPrimitives.WriteInt32LittleEndian(kb.AsSpan(0, 4), 1);
        BinaryPrimitives.WriteInt32LittleEndian(kb.AsSpan(4, 4), 5);
        BinaryPrimitives.WriteInt32LittleEndian(kb.AsSpan(8, 4), 8);
        var vb = new byte[12];
        BinaryPrimitives.WriteInt32LittleEndian(vb.AsSpan(0, 4), 10);
        BinaryPrimitives.WriteInt32LittleEndian(vb.AsSpan(4, 4), 20);
        BinaryPrimitives.WriteInt32LittleEndian(vb.AsSpan(8, 4), 30);
        t.AppendBatch(new ColumnarBatch(3, new IColumnChunk[]
        {
            new FixedWidthColumnChunk(RainDbType.Int32, 3, kb, ReadOnlyMemory<byte>.Empty, false),
            new FixedWidthColumnChunk(RainDbType.Int32, 3, vb, ReadOnlyMemory<byte>.Empty, false),
        }));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT k FROM m WHERE k > 1 AND v < 25");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Single(col.Batches);
        Assert.Equal(1, col.Batches[0].RowCount);
        Assert.Equal(5, BinaryPrimitives.ReadInt32LittleEndian(col.Batches[0].Columns[0].Values.Span));
    }

    [Fact]
    public void CompilePhysicalPlan_where_wrong_table_qualifier_throws()
    {
        var cat = new InMemoryCatalog();
        var schema = new TableSchema([new ColumnDef("qty", RainDbType.Int32)]);
        cat.Register(new MemoryTable("sales", schema));
        Assert.Throws<SqlCompileException>(() =>
            StrictSqlSubset.CompilePhysicalPlan("SELECT * FROM sales WHERE other.qty > 1", cat));
    }

    [Fact]
    public async Task ExecuteSqlAsync_sum_matches_engine()
    {
        var engine = RainDbEngine.CreateDefault();
        var schema = new TableSchema([new ColumnDef("x", RainDbType.Float64)]);
        var t = new MemoryTable("t", schema);
        var col = new FixedWidthColumnChunk(
            RainDbType.Float64,
            2,
            BitConverter.GetBytes(1d).Concat(BitConverter.GetBytes(2d)).ToArray(),
            ReadOnlyMemory<byte>.Empty,
            false);
        t.AppendBatch(new ColumnarBatch(2, new IColumnChunk[] { col }));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT SUM(x) FROM t");
        var agg = Assert.IsAssignableFrom<IAggregateQueryResult>(r);
        Assert.Equal(3d, agg.Float64Value);
    }

    [Fact]
    public async Task ExecuteSqlAsync_where_int_project()
    {
        var engine = RainDbEngine.CreateDefault();
        var schema = new TableSchema([
            new ColumnDef("k", RainDbType.Int32),
            new ColumnDef("v", RainDbType.Int64),
        ]);
        var t = new MemoryTable("m", schema);
        var k = new FixedWidthColumnChunk(RainDbType.Int32, 2, new byte[] { 1, 0, 0, 0, 5, 0, 0, 0 }, ReadOnlyMemory<byte>.Empty, false);
        var v = new FixedWidthColumnChunk(RainDbType.Int64, 2, new byte[16], ReadOnlyMemory<byte>.Empty, false);
        t.AppendBatch(new ColumnarBatch(2, new IColumnChunk[] { k, v }));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT v FROM m WHERE k > 1");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Single(col.Batches);
        Assert.Equal(1, col.Batches[0].RowCount);
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
}
