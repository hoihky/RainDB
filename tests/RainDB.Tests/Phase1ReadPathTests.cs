using System.Buffers.Binary;
using RainDB;
using RainDB.Columnar;
using RainDB.Core.Catalog;
using RainDB.Core.Columnar;
using RainDB.Core.IO;
using RainDB.Core.Tables;
using RainDB.Execution;
using RainDB.Query.Plans;
using RainDB.Schema;

namespace RainDB.Tests;

public class Phase1ReadPathTests
{
    [Fact]
    public async Task VectorizedScan_filter_and_project_matches_expected_rows()
    {
        var engine = RainDbEngine.CreateDefault();
        var schema = new TableSchema([
            new ColumnDef("qty", RainDbType.Int32),
            new ColumnDef("amt", RainDbType.Float64),
        ]);
        var table = new MemoryTable("sales", schema);
        var qty = new FixedWidthColumnChunk(
            RainDbType.Int32,
            3,
            new byte[] { 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0 },
            ReadOnlyMemory<byte>.Empty,
            hasNulls: false);
        var amt = new FixedWidthColumnChunk(
            RainDbType.Float64,
            3,
            BitConverter.GetBytes(10d).Concat(BitConverter.GetBytes(20d)).Concat(BitConverter.GetBytes(30d)).ToArray(),
            ReadOnlyMemory<byte>.Empty,
            hasNulls: false);
        table.AppendBatch(new ColumnarBatch(3, new IColumnChunk[] { qty, amt }));
        engine.Catalog.Register(table);

        var plan = new VectorizedScanPhysicalPlan(
            table.Id,
            outputColumnIndices: [1],
            filters: [new ColumnCompareFilter(0, ScalarCompareOp.Gt, 1)],
            aggregate: null,
            options: new VectorizedScanExecutionOptions { MaxDegreeOfParallelism = 1 });

        await using var result = await engine.ExecutePhysicalAsync(plan);
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(result);
        Assert.Single(col.Batches);
        Assert.Equal(2, col.Batches[0].RowCount);
        var outAmt = col.Batches[0].Columns[0];
        Assert.Equal(BitConverter.ToDouble(outAmt.Values.Span[..8]), BitConverter.ToDouble(amt.Values.Span.Slice(8, 8)));
        Assert.Equal(BitConverter.ToDouble(outAmt.Values.Span.Slice(8, 8)), BitConverter.ToDouble(amt.Values.Span.Slice(16, 8)));
    }

    [Fact]
    public async Task VectorizedScan_conjunctive_filters()
    {
        var engine = RainDbEngine.CreateDefault();
        var schema = new TableSchema([
            new ColumnDef("qty", RainDbType.Int32),
            new ColumnDef("amt", RainDbType.Float64),
        ]);
        var table = new MemoryTable("sales", schema);
        var qty = new FixedWidthColumnChunk(
            RainDbType.Int32,
            3,
            new byte[] { 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0 },
            ReadOnlyMemory<byte>.Empty,
            hasNulls: false);
        var amt = new FixedWidthColumnChunk(
            RainDbType.Float64,
            3,
            BitConverter.GetBytes(10d).Concat(BitConverter.GetBytes(20d)).Concat(BitConverter.GetBytes(30d)).ToArray(),
            ReadOnlyMemory<byte>.Empty,
            hasNulls: false);
        table.AppendBatch(new ColumnarBatch(3, new IColumnChunk[] { qty, amt }));
        engine.Catalog.Register(table);

        var plan = new VectorizedScanPhysicalPlan(
            table.Id,
            outputColumnIndices: [1],
            filters:
            [
                new ColumnCompareFilter(0, ScalarCompareOp.Lt, 3),
                new ColumnCompareFilter(1, ScalarCompareOp.Gt, BitConverter.DoubleToInt64Bits(15)),
            ],
            aggregate: null,
            options: new VectorizedScanExecutionOptions { MaxDegreeOfParallelism = 1 });

        await using var result = await engine.ExecutePhysicalAsync(plan);
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(result);
        Assert.Single(col.Batches);
        Assert.Equal(1, col.Batches[0].RowCount);
        var outAmt = col.Batches[0].Columns[0];
        Assert.Equal(20d, BitConverter.ToDouble(outAmt.Values.Span[..8]));
    }

    [Fact]
    public async Task VectorizedScan_parallel_merge_preserves_batch_order()
    {
        var engine = RainDbEngine.CreateDefault();
        var schema = new TableSchema([new ColumnDef("x", RainDbType.Int32)]);
        var table = new MemoryTable("t", schema);
        table.AppendBatch(new ColumnarBatch(1, new IColumnChunk[] { new FixedWidthColumnChunk(RainDbType.Int32, 1, BitConverter.GetBytes(1), ReadOnlyMemory<byte>.Empty, false) }));
        table.AppendBatch(new ColumnarBatch(1, new IColumnChunk[] { new FixedWidthColumnChunk(RainDbType.Int32, 1, BitConverter.GetBytes(2), ReadOnlyMemory<byte>.Empty, false) }));
        engine.Catalog.Register(table);

        var plan = new VectorizedScanPhysicalPlan(
            table.Id,
            outputColumnIndices: [0],
            aggregate: null,
            options: new VectorizedScanExecutionOptions { MaxDegreeOfParallelism = 8, UseChannelScheduler = false });

        await using var r = await engine.ExecutePhysicalAsync(plan);
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(2, col.Batches.Count);
        Assert.Equal(1, BinaryPrimitives.ReadInt32LittleEndian(col.Batches[0].Columns[0].Values.Span));
        Assert.Equal(2, BinaryPrimitives.ReadInt32LittleEndian(col.Batches[1].Columns[0].Values.Span));
    }

    [Fact]
    public async Task VectorizedScan_channel_scheduler_preserves_batch_order()
    {
        var engine = RainDbEngine.CreateDefault();
        var schema = new TableSchema([new ColumnDef("x", RainDbType.Int32)]);
        var table = new MemoryTable("t", schema);
        for (var b = 0; b < 4; b++)
            table.AppendBatch(new ColumnarBatch(1, new IColumnChunk[] { new FixedWidthColumnChunk(RainDbType.Int32, 1, BitConverter.GetBytes(b), ReadOnlyMemory<byte>.Empty, false) }));
        engine.Catalog.Register(table);

        var plan = new VectorizedScanPhysicalPlan(
            table.Id,
            outputColumnIndices: [0],
            aggregate: null,
            options: new VectorizedScanExecutionOptions { MaxDegreeOfParallelism = 4, UseChannelScheduler = true });

        await using var r = await engine.ExecutePhysicalAsync(plan);
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(4, col.Batches.Count);
        for (var i = 0; i < 4; i++)
            Assert.Equal(i, BinaryPrimitives.ReadInt32LittleEndian(col.Batches[i].Columns[0].Values.Span));
    }

    [Fact]
    public async Task VectorizedScan_aggregate_sum_float64()
    {
        var engine = RainDbEngine.CreateDefault();
        var schema = new TableSchema([new ColumnDef("amt", RainDbType.Float64)]);
        var table = new MemoryTable("t", schema);
        var c1 = new FixedWidthColumnChunk(RainDbType.Float64, 2, BitConverter.GetBytes(1d).Concat(BitConverter.GetBytes(2d)).ToArray(), ReadOnlyMemory<byte>.Empty, false);
        var c2 = new FixedWidthColumnChunk(RainDbType.Float64, 2, BitConverter.GetBytes(3d).Concat(BitConverter.GetBytes(4d)).ToArray(), ReadOnlyMemory<byte>.Empty, false);
        table.AppendBatch(new ColumnarBatch(2, new IColumnChunk[] { c1 }));
        table.AppendBatch(new ColumnarBatch(2, new IColumnChunk[] { c2 }));
        engine.Catalog.Register(table);

        var plan = new VectorizedScanPhysicalPlan(
            table.Id,
            outputColumnIndices: [0],
            aggregate: new AggregateSpec(0, AggregateKind.Sum),
            options: new VectorizedScanExecutionOptions { MaxDegreeOfParallelism = 4, UseAvx2DoubleSum = true });

        await using var r = await engine.ExecutePhysicalAsync(plan);
        var agg = Assert.IsAssignableFrom<IAggregateQueryResult>(r);
        Assert.Equal(10d, agg.Float64Value);
        Assert.Equal(4, agg.ContributingRowCount);
    }

    [Fact]
    public void Mmap_fixed_width_column_roundtrips_zero_copy_span()
    {
        var path = Path.Combine(Path.GetTempPath(), $"rnbcol_{Guid.NewGuid():N}.col");
        try
        {
            var chunk = new FixedWidthColumnChunk(
                RainDbType.Float64,
                2,
                BitConverter.GetBytes(5d).Concat(BitConverter.GetBytes(6d)).ToArray(),
                ReadOnlyMemory<byte>.Empty,
                false);
            ColumnarFixedWidthFileFormat.WriteFile(path, chunk);
            using var map = ColumnarFixedWidthMmapReader.Open(path);
            var mapped = map.Chunk;
            Assert.Equal(chunk.Values.Span.ToArray(), mapped.Values.Span.ToArray());
        }
        finally
        {
            try
            {
                File.Delete(path);
            }
            catch
            {
                // ignore
            }
        }
    }
}
