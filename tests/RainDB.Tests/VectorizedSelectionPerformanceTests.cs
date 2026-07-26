using System.Buffers.Binary;
using System.Diagnostics;
using RainDB;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Core.Tables;
using RainDB.Execution;
using RainDB.Query.Plans;
using RainDB.Schema;

namespace RainDB.Tests;

public class VectorizedSelectionPerformanceTests
{
    private const int LargeBatchRows = 1_048_576;

    [Fact]
    public async Task VectorizedScan_1M_rows_selective_where_correct_row_count()
    {
        var engine = RainDbEngine.CreateDefault();
        var schema = new TableSchema([
            new ColumnDef("bucket", RainDbType.Int32),
            new ColumnDef("payload", RainDbType.Int64),
        ]);
        var table = new MemoryTable("wide", schema);
        var bucketBytes = new byte[LargeBatchRows * sizeof(int)];
        var payloadBytes = new byte[LargeBatchRows * sizeof(long)];
        for (var i = 0; i < LargeBatchRows; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(bucketBytes.AsSpan(i * sizeof(int), sizeof(int)), i % 10);
            BinaryPrimitives.WriteInt64LittleEndian(payloadBytes.AsSpan(i * sizeof(long), sizeof(long)), i);
        }

        table.AppendBatch(new ColumnarBatch(
            LargeBatchRows,
            new IColumnChunk[]
            {
                new FixedWidthColumnChunk(RainDbType.Int32, LargeBatchRows, bucketBytes, ReadOnlyMemory<byte>.Empty, hasNulls: false),
                new FixedWidthColumnChunk(RainDbType.Int64, LargeBatchRows, payloadBytes, ReadOnlyMemory<byte>.Empty, hasNulls: false),
            }));
        engine.Catalog.Register(table);

        // bucket IN {2,3} ~20% selectivity via two conjunctive range filters: bucket >= 2 AND bucket <= 3
        var plan = new VectorizedScanPhysicalPlan(
            table.Id,
            outputColumnIndices: [1],
            filters:
            [
                new ColumnCompareFilter(0, ScalarCompareOp.Ge, 2),
                new ColumnCompareFilter(0, ScalarCompareOp.Le, 3),
            ],
            aggregate: null,
            options: new VectorizedScanExecutionOptions { MaxDegreeOfParallelism = 1 });

        await using var result = await engine.ExecutePhysicalAsync(plan);
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(result);
        Assert.Single(col.Batches);
        var expected = 0;
        for (var i = 0; i < LargeBatchRows; i++)
        {
            var b = i % 10;
            if (b is >= 2 and <= 3)
                expected++;
        }

        Assert.Equal(expected, col.Batches[0].RowCount);
    }

    [Fact]
    public async Task VectorizedScan_1M_rows_selective_where_completes_within_budget()
    {
        var engine = RainDbEngine.CreateDefault();
        var schema = new TableSchema([new ColumnDef("x", RainDbType.Int32)]);
        var table = new MemoryTable("t", schema);
        var bytes = new byte[LargeBatchRows * sizeof(int)];
        for (var i = 0; i < LargeBatchRows; i++)
            BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(i * sizeof(int), sizeof(int)), i % 5);
        table.AppendBatch(new ColumnarBatch(
            LargeBatchRows,
            new IColumnChunk[] { new FixedWidthColumnChunk(RainDbType.Int32, LargeBatchRows, bytes, ReadOnlyMemory<byte>.Empty, false) }));
        engine.Catalog.Register(table);

        var plan = new VectorizedScanPhysicalPlan(
            table.Id,
            outputColumnIndices: [0],
            filters: [new ColumnCompareFilter(0, ScalarCompareOp.Eq, 2)],
            aggregate: null,
            options: new VectorizedScanExecutionOptions { MaxDegreeOfParallelism = 1 });

        var sw = Stopwatch.StartNew();
        await using var result = await engine.ExecutePhysicalAsync(plan);
        sw.Stop();

        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(result);
        Assert.Equal(LargeBatchRows / 5, col.Batches[0].RowCount);
        Assert.True(sw.ElapsedMilliseconds < 5_000, $"Scan took {sw.ElapsedMilliseconds}ms (budget 5000ms).");
    }
}
