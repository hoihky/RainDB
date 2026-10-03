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

public class PhaseD4Tests
{
    [Fact]
    public void Parse_where_in_subquery()
    {
        var plan = StrictSqlSubset.ParseLogicalPlan(
            "SELECT id FROM items WHERE id IN (SELECT id FROM lookup)");
        var scan = Assert.IsType<LogicalTableScan>(plan.Root);
        Assert.NotNull(scan.SubqueryPredicates);
        var p = Assert.Single(scan.SubqueryPredicates!);
        Assert.Equal(LogicalUncorrelatedSubqueryPredicate.Kind.In, p.PredicateKind);
        Assert.Equal("id", p.Column!.ColumnName);
        Assert.IsType<LogicalTableScan>(p.Subquery.Root);
    }

    [Fact]
    public void Parse_where_not_in_and_exists()
    {
        var plan = StrictSqlSubset.ParseLogicalPlan(
            "SELECT id FROM items WHERE id NOT IN (SELECT id FROM lookup) AND EXISTS (SELECT 1 FROM lookup)");
        var scan = Assert.IsType<LogicalTableScan>(plan.Root);
        Assert.Equal(2, scan.SubqueryPredicates!.Count);
        Assert.Contains(scan.SubqueryPredicates!, p => p.PredicateKind == LogicalUncorrelatedSubqueryPredicate.Kind.NotIn);
        Assert.Contains(scan.SubqueryPredicates!, p => p.PredicateKind == LogicalUncorrelatedSubqueryPredicate.Kind.Exists);
    }

    [Fact]
    public void Parse_not_exists()
    {
        var plan = StrictSqlSubset.ParseLogicalPlan(
            "SELECT id FROM items WHERE NOT EXISTS (SELECT id FROM lookup)");
        var p = Assert.Single(Assert.IsType<LogicalTableScan>(plan.Root).SubqueryPredicates!);
        Assert.Equal(LogicalUncorrelatedSubqueryPredicate.Kind.NotExists, p.PredicateKind);
    }

    [Fact]
    public void Parse_derived_table_from_subquery()
    {
        var plan = StrictSqlSubset.ParseLogicalPlan(
            "SELECT d.id FROM (SELECT id FROM items) d WHERE d.id > 1");
        var derived = Assert.IsType<LogicalDerivedTableScan>(plan.Root);
        Assert.Equal("d", derived.Alias);
        Assert.IsType<LogicalTableScan>(derived.Subquery.Root);
        Assert.NotNull(derived.WhereConjuncts);
    }

    [Fact]
    public void Correlated_subquery_is_rejected_at_compile()
    {
        var cat = new InMemoryCatalog();
        cat.Register(ItemsTable([1], [10]));
        Assert.Throws<SqlCompileException>(() =>
            StrictSqlSubset.CompilePhysicalPlan(
                "SELECT id FROM items WHERE id IN (SELECT id FROM items WHERE qty > 0)", cat));
    }

    [Fact]
    public void In_subquery_must_return_one_column()
    {
        var cat = new InMemoryCatalog();
        cat.Register(ItemsTable([1], [10]));
        cat.Register(LookupTwoCol([1], [2]));
        Assert.Throws<SqlCompileException>(() =>
            StrictSqlSubset.CompilePhysicalPlan(
                "SELECT id FROM items WHERE id IN (SELECT id, qty FROM lookup)", cat));
    }

    [Fact]
    public async Task Where_in_filters_rows()
    {
        var engine = RainDbEngine.CreateDefault();
        engine.Catalog.Register(ItemsTable([1, 2, 3], [10, 20, 30]));
        engine.Catalog.Register(LookupTable([2, 3]));

        await using var r = await engine.ExecuteSqlAsync(
            "SELECT id FROM items WHERE id IN (SELECT id FROM lookup) ORDER BY id");
        var ids = ReadInt32Column(r, 0);
        Assert.Equal([2, 3], ids);
    }

    [Fact]
    public async Task Where_not_in_excludes_set()
    {
        var engine = RainDbEngine.CreateDefault();
        engine.Catalog.Register(ItemsTable([1, 2, 3], [10, 20, 30]));
        engine.Catalog.Register(LookupTable([2]));

        await using var r = await engine.ExecuteSqlAsync(
            "SELECT id FROM items WHERE id NOT IN (SELECT id FROM lookup) ORDER BY id");
        Assert.Equal([1, 3], ReadInt32Column(r, 0));
    }

    [Fact]
    public async Task Exists_true_when_subquery_has_rows()
    {
        var engine = RainDbEngine.CreateDefault();
        engine.Catalog.Register(ItemsTable([1, 2], [10, 20]));
        engine.Catalog.Register(LookupTable([99]));

        await using var r = await engine.ExecuteSqlAsync(
            "SELECT id FROM items WHERE EXISTS (SELECT id FROM lookup)");
        Assert.Equal(2, Assert.IsAssignableFrom<IColumnarQueryResult>(r).RowCount);
    }

    [Fact]
    public async Task Exists_false_yields_no_rows()
    {
        var engine = RainDbEngine.CreateDefault();
        engine.Catalog.Register(ItemsTable([1, 2], [10, 20]));
        engine.Catalog.Register(LookupTable(Array.Empty<int>()));

        await using var r = await engine.ExecuteSqlAsync(
            "SELECT id FROM items WHERE EXISTS (SELECT id FROM lookup)");
        Assert.Equal(0, Assert.IsAssignableFrom<IColumnarQueryResult>(r).RowCount);
    }

    [Fact]
    public async Task Not_exists_true_when_subquery_empty()
    {
        var engine = RainDbEngine.CreateDefault();
        engine.Catalog.Register(ItemsTable([1, 2], [10, 20]));
        engine.Catalog.Register(LookupTable(Array.Empty<int>()));

        await using var r = await engine.ExecuteSqlAsync(
            "SELECT id FROM items WHERE NOT EXISTS (SELECT id FROM lookup) ORDER BY id");
        Assert.Equal([1, 2], ReadInt32Column(r, 0));
    }

    [Fact]
    public async Task Not_exists_false_when_subquery_has_rows()
    {
        var engine = RainDbEngine.CreateDefault();
        engine.Catalog.Register(ItemsTable([1, 2], [10, 20]));
        engine.Catalog.Register(LookupTable([1]));

        await using var r = await engine.ExecuteSqlAsync(
            "SELECT id FROM items WHERE NOT EXISTS (SELECT id FROM lookup)");
        Assert.Equal(0, Assert.IsAssignableFrom<IColumnarQueryResult>(r).RowCount);
    }

    [Fact]
    public async Task In_combined_with_simple_where()
    {
        var engine = RainDbEngine.CreateDefault();
        engine.Catalog.Register(ItemsTable([1, 2, 3, 4], [10, 20, 30, 40]));
        engine.Catalog.Register(LookupTable([2, 3, 4]));

        await using var r = await engine.ExecuteSqlAsync(
            "SELECT id FROM items WHERE qty >= 20 AND id IN (SELECT id FROM lookup) ORDER BY id");
        Assert.Equal([2, 3, 4], ReadInt32Column(r, 0));
    }

    [Fact]
    public async Task Derived_table_with_outer_filter_and_limit()
    {
        var engine = RainDbEngine.CreateDefault();
        engine.Catalog.Register(ItemsTable([1, 2, 3, 4], [10, 20, 30, 40]));

        await using var r = await engine.ExecuteSqlAsync(
            "SELECT d.id FROM (SELECT id, qty FROM items) d WHERE d.qty > 15 ORDER BY d.id LIMIT 2");
        Assert.Equal([2, 3], ReadInt32Column(r, 0));
    }

    [Fact]
    public async Task Derived_table_star_select()
    {
        var engine = RainDbEngine.CreateDefault();
        engine.Catalog.Register(ItemsTable([5], [50]));

        await using var r = await engine.ExecuteSqlAsync("SELECT * FROM (SELECT id FROM items) t");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(1, col.RowCount);
        Assert.Equal(5, ReadInt32Column(r, 0)[0]);
    }

    [Fact]
    public async Task In_subquery_with_where_on_inner()
    {
        var engine = RainDbEngine.CreateDefault();
        engine.Catalog.Register(ItemsTable([1, 2, 3], [5, 15, 25]));
        engine.Catalog.Register(LookupTable([1, 2, 3]));

        await using var r = await engine.ExecuteSqlAsync(
            "SELECT id FROM items WHERE id IN (SELECT id FROM lookup WHERE id > 1) ORDER BY id");
        Assert.Equal([2, 3], ReadInt32Column(r, 0));
    }

    [Fact]
    public void CompilePhysicalPlan_in_subquery_attached_to_scan()
    {
        var cat = new InMemoryCatalog();
        cat.Register(ItemsTable([1], [10]));
        cat.Register(LookupTable([1]));
        var plan = StrictSqlSubset.CompilePhysicalPlan(
            "SELECT id FROM items WHERE id IN (SELECT id FROM lookup)", cat);
        switch (plan)
        {
            case VectorizedScanPhysicalPlan vs:
                Assert.NotNull(vs.InSubqueries);
                break;
            case SortTopNPhysicalPlan st:
                Assert.NotNull(st.InSubqueries);
                break;
            default:
                throw new InvalidOperationException($"Unexpected plan {plan.GetType().Name}");
        }
    }

    [Fact]
    public void CompilePhysicalPlan_derived_table_operator()
    {
        var cat = new InMemoryCatalog();
        cat.Register(ItemsTable([1, 2], [10, 20]));
        var plan = StrictSqlSubset.CompilePhysicalPlan(
            "SELECT t.id FROM (SELECT id FROM items) t WHERE t.id = 2", cat);
        Assert.IsType<DerivedTableScanPhysicalPlan>(plan);
    }

    [Fact]
    public async Task In_with_utf8_values()
    {
        var engine = RainDbEngine.CreateDefault();
        var items = new MemoryTable("items", new TableSchema([
            new ColumnDef("k", RainDbType.Utf8),
        ]));
        items.AppendBatch(new ColumnarBatch(3, [Utf8Chunk("a", "b", "c")]));
        var lookup = new MemoryTable("lookup", new TableSchema([
            new ColumnDef("k", RainDbType.Utf8),
        ]));
        lookup.AppendBatch(new ColumnarBatch(2, [Utf8Chunk("b", "c")]));
        engine.Catalog.Register(items);
        engine.Catalog.Register(lookup);

        await using var r = await engine.ExecuteSqlAsync(
            "SELECT k FROM items WHERE k IN (SELECT k FROM lookup) ORDER BY k");
        var batch = Assert.IsAssignableFrom<IColumnarQueryResult>(r).Batches[0];
        Assert.Equal(2, batch.RowCount);
        Assert.Equal("b", ReadUtf8(batch.Columns[0], 0));
        Assert.Equal("c", ReadUtf8(batch.Columns[0], 1));
    }

    [Fact]
    public void Parse_join_where_in_subquery()
    {
        var plan = StrictSqlSubset.ParseLogicalPlan(
            "SELECT L.id FROM L INNER JOIN R ON L.id = R.id WHERE L.id IN (SELECT id FROM lookup)");
        var join = Assert.IsType<LogicalInnerJoin>(plan.Root);
        Assert.NotNull(join.SubqueryPredicates);
        Assert.Equal(LogicalUncorrelatedSubqueryPredicate.Kind.In, join.SubqueryPredicates![0].PredicateKind);
    }

    [Fact]
    public async Task Join_where_in_on_probe_side()
    {
        var engine = RainDbEngine.CreateDefault();
        RegisterJoinLR(engine, leftIds: [1, 2, 3], rightIds: [2, 3, 3], rightB: [10L, 20L, 30L]);
        engine.Catalog.Register(LookupTable([2, 3]));

        await using var r = await engine.ExecuteSqlAsync(
            "SELECT L.id, R.b FROM L INNER JOIN R ON L.id = R.id WHERE L.id IN (SELECT id FROM lookup) ORDER BY L.id, R.b");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(3, col.RowCount);
        Assert.Equal([2, 3, 3], ReadInt32Column(r, 0));
    }

    [Fact]
    public async Task Join_where_not_in_on_build_side()
    {
        var engine = RainDbEngine.CreateDefault();
        RegisterJoinLR(engine, leftIds: [1, 2], rightIds: [1, 2], rightB: [100L, 200L]);
        engine.Catalog.Register(LookupTable([2]));

        await using var r = await engine.ExecuteSqlAsync(
            "SELECT L.id FROM L INNER JOIN R ON L.id = R.id WHERE R.id NOT IN (SELECT id FROM lookup) ORDER BY L.id");
        Assert.Equal([1], ReadInt32Column(r, 0));
    }

    [Fact]
    public async Task Join_where_exists()
    {
        var engine = RainDbEngine.CreateDefault();
        RegisterJoinLR(engine, [1], [1], [9L]);
        engine.Catalog.Register(LookupTable([99]));

        await using var r = await engine.ExecuteSqlAsync(
            "SELECT L.id FROM L INNER JOIN R ON L.id = R.id WHERE EXISTS (SELECT id FROM lookup)");
        Assert.Equal(1, Assert.IsAssignableFrom<IColumnarQueryResult>(r).RowCount);
    }

    [Fact]
    public void Join_correlated_subquery_in_where_is_rejected()
    {
        var engine = RainDbEngine.CreateDefault();
        RegisterJoinLR(engine, [1], [1], [9L]);
        Assert.Throws<SqlCompileException>(() =>
            StrictSqlSubset.CompilePhysicalPlan(
                "SELECT L.id FROM L INNER JOIN R ON L.id = R.id WHERE L.id IN (SELECT id FROM L WHERE R.id > 0)",
                engine.Catalog));
    }

    [Fact]
    public void Group_by_on_derived_table_is_rejected()
    {
        Assert.Throws<SqlCompileException>(() =>
            StrictSqlSubset.ParseLogicalPlan(
                "SELECT k, COUNT(*) FROM (SELECT k FROM items) d GROUP BY k"));
    }

    private static void RegisterJoinLR(RainDbEngine engine, int[] leftIds, int[] rightIds, long[] rightB)
    {
        var left = new MemoryTable("L", new TableSchema([
            new ColumnDef("id", RainDbType.Int32),
        ]));
        var right = new MemoryTable("R", new TableSchema([
            new ColumnDef("id", RainDbType.Int32),
            new ColumnDef("b", RainDbType.Int64),
        ]));
        left.AppendBatch(new ColumnarBatch(leftIds.Length, [Int32Chunk(leftIds)]));
        right.AppendBatch(new ColumnarBatch(rightIds.Length, [
            Int32Chunk(rightIds),
            Int64Chunk(rightB),
        ]));
        engine.Catalog.Register(left);
        engine.Catalog.Register(right);
    }

    private static FixedWidthColumnChunk Int64Chunk(long[] values)
    {
        var bytes = new byte[values.Length * sizeof(long)];
        for (var i = 0; i < values.Length; i++)
            BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(i * sizeof(long), sizeof(long)), values[i]);
        return new FixedWidthColumnChunk(RainDbType.Int64, values.Length, bytes, ReadOnlyMemory<byte>.Empty, false);
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

    private static MemoryTable LookupTable(int[] ids)
    {
        var t = new MemoryTable("lookup", new TableSchema([new ColumnDef("id", RainDbType.Int32)]));
        if (ids.Length > 0)
            t.AppendBatch(new ColumnarBatch(ids.Length, [Int32Chunk(ids)]));
        return t;
    }

    private static MemoryTable LookupTwoCol(int[] ids, int[] extra)
    {
        var t = new MemoryTable("lookup", new TableSchema([
            new ColumnDef("id", RainDbType.Int32),
            new ColumnDef("qty", RainDbType.Int32),
        ]));
        t.AppendBatch(new ColumnarBatch(ids.Length, [Int32Chunk(ids), Int32Chunk(extra)]));
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

    private static FixedWidthColumnChunk Int32Chunk(int[] values)
    {
        var bytes = new byte[values.Length * sizeof(int)];
        for (var i = 0; i < values.Length; i++)
            BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(i * sizeof(int), sizeof(int)), values[i]);
        return new FixedWidthColumnChunk(RainDbType.Int32, values.Length, bytes, ReadOnlyMemory<byte>.Empty, false);
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
