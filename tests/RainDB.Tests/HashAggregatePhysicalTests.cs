using System.Buffers.Binary;
using RainDB;
using RainDB.Columnar;
using RainDB.Core.Catalog;
using RainDB.Core.Columnar;
using RainDB.Core.Memory;
using RainDB.Core.Tables;
using RainDB.Execution;
using RainDB.Linq.Compilation;
using RainDB.Query.Execution;
using RainDB.Query.Plans;
using RainDB.Schema;
using RainDB.Sql.Compilation;

namespace RainDB.Tests;

public class HashAggregatePhysicalTests
{
    [Fact]
    public async Task HashAggregate_sum_int64_grouped_by_int32_parallel_batches()
    {
        var engine = RainDbEngine.CreateDefault();
        var schema = new TableSchema([
            new ColumnDef("k", RainDbType.Int32),
            new ColumnDef("v", RainDbType.Int64),
        ]);
        var table = new MemoryTable("t", schema);
        table.AppendBatch(MkBatch2(k: [1, 2], v: [100L, 200L]));
        table.AppendBatch(MkBatch2(k: [1, 2], v: [300L, 400L]));
        engine.Catalog.Register(table);

        var plan = new HashAggregatePhysicalPlan(
            table.Id,
            groupKeyColumnIndices: [0],
            aggregates: [new AggregateSpec(1, AggregateKind.Sum)],
            filters: null,
            options: new VectorizedScanExecutionOptions { MaxDegreeOfParallelism = 8 });

        await using var r = await engine.ExecutePhysicalAsync(plan);
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Single(col.Batches);
        Assert.Equal(2, col.Batches[0].RowCount);
        var keys = col.Batches[0].Columns[0];
        var sums = col.Batches[0].Columns[1];
        Assert.Equal(RainDbType.Int32, keys.PhysicalType);
        Assert.Equal(RainDbType.Int64, sums.PhysicalType);
        Assert.Equal(1, BinaryPrimitives.ReadInt32LittleEndian(keys.Values.Span));
        Assert.Equal(400L, BinaryPrimitives.ReadInt64LittleEndian(sums.Values.Span));
        Assert.Equal(2, BinaryPrimitives.ReadInt32LittleEndian(keys.Values.Span[sizeof(int)..]));
        Assert.Equal(600L, BinaryPrimitives.ReadInt64LittleEndian(sums.Values.Span[sizeof(long)..]));
    }

    [Fact]
    public async Task HashAggregate_multi_fixed_key_columns_and_sum_float64()
    {
        var engine = RainDbEngine.CreateDefault();
        var schema = new TableSchema([
            new ColumnDef("a", RainDbType.Int32),
            new ColumnDef("b", RainDbType.Boolean),
            new ColumnDef("amt", RainDbType.Float64),
        ]);
        var table = new MemoryTable("t", schema);
        table.AppendBatch(MkBatch3(
            a: [1, 1],
            b: [false, true],
            amt: [1d, 10d]));
        table.AppendBatch(MkBatch3(
            a: [1, 1],
            b: [false, true],
            amt: [2d, 20d]));
        engine.Catalog.Register(table);

        var plan = new HashAggregatePhysicalPlan(
            table.Id,
            groupKeyColumnIndices: [0, 1],
            aggregates: [new AggregateSpec(2, AggregateKind.Sum)],
            filters: null,
            options: new VectorizedScanExecutionOptions { MaxDegreeOfParallelism = 1 });

        await using var r = await engine.ExecutePhysicalAsync(plan);
        var result = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Single(result.Batches);
        Assert.Equal(2, result.Batches[0].RowCount);
        Assert.Equal(3, result.Batches[0].Columns.Count);

        var rowFalse = RowIndexWhere(result.Batches[0], a: 1, b: false);
        var rowTrue = RowIndexWhere(result.Batches[0], a: 1, b: true);
        var sumFalse = BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(
            result.Batches[0].Columns[2].Values.Span.Slice(rowFalse * sizeof(double), sizeof(double))));
        var sumTrue = BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(
            result.Batches[0].Columns[2].Values.Span.Slice(rowTrue * sizeof(double), sizeof(double))));
        Assert.Equal(3d, sumFalse, 9);
        Assert.Equal(30d, sumTrue, 9);
    }

    [Fact]
    public async Task HashAggregate_where_filters_before_grouping()
    {
        var engine = RainDbEngine.CreateDefault();
        var schema = new TableSchema([
            new ColumnDef("k", RainDbType.Int32),
            new ColumnDef("v", RainDbType.Int64),
        ]);
        var table = new MemoryTable("t", schema);
        table.AppendBatch(MkBatch2(k: [1, 2, 3], v: [10L, 20L, 30L]));
        engine.Catalog.Register(table);

        var plan = new HashAggregatePhysicalPlan(
            table.Id,
            groupKeyColumnIndices: [0],
            aggregates: [new AggregateSpec(1, AggregateKind.Sum)],
            filters: [new ColumnCompareFilter(1, ScalarCompareOp.Gt, 15)],
            options: default);

        await using var r = await engine.ExecutePhysicalAsync(plan);
        var result = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Single(result.Batches);
        Assert.Equal(2, result.Batches[0].RowCount);
    }

    [Fact]
    public async Task HashAggregate_min_max_float64_per_group()
    {
        var engine = RainDbEngine.CreateDefault();
        var schema = new TableSchema([
            new ColumnDef("k", RainDbType.Int64),
            new ColumnDef("x", RainDbType.Float64),
        ]);
        var table = new MemoryTable("t", schema);
        table.AppendBatch(MkBatch2LongFloat(k: [10L, 10L], x: [3d, 1d]));
        table.AppendBatch(MkBatch2LongFloat(k: [10L], x: [2d]));
        engine.Catalog.Register(table);

        var plan = new HashAggregatePhysicalPlan(
            table.Id,
            groupKeyColumnIndices: [0],
            aggregates: [new AggregateSpec(1, AggregateKind.Min), new AggregateSpec(1, AggregateKind.Max)],
            filters: null,
            options: new VectorizedScanExecutionOptions { MaxDegreeOfParallelism = 4 });

        await using var r = await engine.ExecutePhysicalAsync(plan);
        var result = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Single(result.Batches);
        Assert.Equal(1, result.Batches[0].RowCount);
        var mn = BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(result.Batches[0].Columns[1].Values.Span));
        var mx = BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(result.Batches[0].Columns[2].Values.Span));
        Assert.Equal(1d, mn);
        Assert.Equal(3d, mx);
    }

    [Fact]
    public async Task HashAggregate_empty_table_yields_zero_rows_with_schema_columns()
    {
        var engine = RainDbEngine.CreateDefault();
        var schema = new TableSchema([
            new ColumnDef("k", RainDbType.Int32),
            new ColumnDef("v", RainDbType.Int64),
        ]);
        var table = new MemoryTable("t", schema);
        engine.Catalog.Register(table);

        var plan = new HashAggregatePhysicalPlan(
            table.Id,
            groupKeyColumnIndices: [0],
            aggregates: [new AggregateSpec(1, AggregateKind.Sum)],
            filters: null,
            options: default);

        await using var r = await engine.ExecutePhysicalAsync(plan);
        var result = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Single(result.Batches);
        Assert.Equal(0, result.Batches[0].RowCount);
        Assert.Equal(2, result.Batches[0].Columns.Count);
    }

    [Fact]
    public async Task Spill_threshold_invokes_spill_chunk_on_large_partial()
    {
        var catalog = new InMemoryCatalog();
        var buffers = new HybridBufferPool();
        var spill = new CaptureSpillWriter();
        var engine = new RainDbEngine(catalog, buffers, buffers, new DefaultQueryExecutor(), new DefaultSqlCompiler(), new DefaultLinqCompiler(), spill);
        var schema = new TableSchema([
            new ColumnDef("k", RainDbType.Int32),
            new ColumnDef("v", RainDbType.Int64),
        ]);
        var table = new MemoryTable("t", schema);
        table.AppendBatch(MkBatch2(k: [1, 2, 3], v: [1L, 1L, 1L]));
        catalog.Register(table);

        var plan = new HashAggregatePhysicalPlan(
            table.Id,
            [0],
            [new AggregateSpec(1, AggregateKind.Sum)],
            spillPartialEntryThreshold: 2);
        await using var r = await engine.ExecutePhysicalAsync(plan);
        _ = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.NotEmpty(spill.Chunks);
    }

    private static int RowIndexWhere(IColumnarBatch batch, int a, bool b)
    {
        for (var r = 0; r < batch.RowCount; r++)
        {
            var ai = BinaryPrimitives.ReadInt32LittleEndian(batch.Columns[0].Values.Span.Slice(r * sizeof(int), sizeof(int)));
            var bi = batch.Columns[1].Values.Span[r] != 0;
            if (ai == a && bi == b)
                return r;
        }

        throw new InvalidOperationException($"No row with a={a}, b={b}.");
    }

    private static ColumnarBatch MkBatch2(int[] k, long[] v)
    {
        var kb = new byte[k.Length * sizeof(int)];
        for (var i = 0; i < k.Length; i++)
            BinaryPrimitives.WriteInt32LittleEndian(kb.AsSpan(i * sizeof(int), sizeof(int)), k[i]);
        var vb = new byte[v.Length * sizeof(long)];
        for (var i = 0; i < v.Length; i++)
            BinaryPrimitives.WriteInt64LittleEndian(vb.AsSpan(i * sizeof(long), sizeof(long)), v[i]);
        return new ColumnarBatch(k.Length, [
            new FixedWidthColumnChunk(RainDbType.Int32, k.Length, kb, ReadOnlyMemory<byte>.Empty, false),
            new FixedWidthColumnChunk(RainDbType.Int64, v.Length, vb, ReadOnlyMemory<byte>.Empty, false),
        ]);
    }

    private static ColumnarBatch MkBatch3(int[] a, bool[] b, double[] amt)
    {
        var ab = new byte[a.Length * sizeof(int)];
        for (var i = 0; i < a.Length; i++)
            BinaryPrimitives.WriteInt32LittleEndian(ab.AsSpan(i * sizeof(int), sizeof(int)), a[i]);
        var bb = new byte[b.Length];
        for (var i = 0; i < b.Length; i++)
            bb[i] = b[i] ? (byte)1 : (byte)0;
        var mb = new byte[amt.Length * sizeof(double)];
        for (var i = 0; i < amt.Length; i++)
            BinaryPrimitives.WriteInt64LittleEndian(mb.AsSpan(i * sizeof(double), sizeof(double)), BitConverter.DoubleToInt64Bits(amt[i]));
        return new ColumnarBatch(a.Length, [
            new FixedWidthColumnChunk(RainDbType.Int32, a.Length, ab, ReadOnlyMemory<byte>.Empty, false),
            new FixedWidthColumnChunk(RainDbType.Boolean, b.Length, bb, ReadOnlyMemory<byte>.Empty, false),
            new FixedWidthColumnChunk(RainDbType.Float64, amt.Length, mb, ReadOnlyMemory<byte>.Empty, false),
        ]);
    }

    private static ColumnarBatch MkBatch2LongFloat(long[] k, double[] x)
    {
        var kb = new byte[k.Length * sizeof(long)];
        for (var i = 0; i < k.Length; i++)
            BinaryPrimitives.WriteInt64LittleEndian(kb.AsSpan(i * sizeof(long), sizeof(long)), k[i]);
        var xb = new byte[x.Length * sizeof(double)];
        for (var i = 0; i < x.Length; i++)
            BinaryPrimitives.WriteInt64LittleEndian(xb.AsSpan(i * sizeof(double), sizeof(double)), BitConverter.DoubleToInt64Bits(x[i]));
        return new ColumnarBatch(k.Length, [
            new FixedWidthColumnChunk(RainDbType.Int64, k.Length, kb, ReadOnlyMemory<byte>.Empty, false),
            new FixedWidthColumnChunk(RainDbType.Float64, x.Length, xb, ReadOnlyMemory<byte>.Empty, false),
        ]);
    }

    private sealed class CaptureSpillWriter : ISpillWriter
    {
        public bool IsEnabled => true;

        public List<byte[]> Chunks { get; } = new();

        public ValueTask SpillChunkAsync(ReadOnlyMemory<byte> chunk, CancellationToken cancellationToken = default)
        {
            Chunks.Add(chunk.ToArray());
            return ValueTask.CompletedTask;
        }
    }
}
