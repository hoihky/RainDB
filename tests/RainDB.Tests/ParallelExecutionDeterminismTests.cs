using RainDB;
using RainDB.Execution;
using RainDB.Query.Plans;
using RainDB.Schema;

namespace RainDB.Tests;

public class ParallelExecutionDeterminismTests
{
    [Fact]
    public async Task Parallel_and_serial_scan_produce_same_row_count_and_batch_order()
    {
        var engine = TestDataBuilders.CreateEngine();
        TestDataBuilders.RegisterAnalyticsDemoTables(engine);
        var table = engine.Catalog.TryGetTable("order_lines", out var ts) ? ts : throw new InvalidOperationException();
        var id = table!.Id;

        async Task<long> RunAsync(int dop, bool channel)
        {
            var plan = new VectorizedScanPhysicalPlan(
                id,
                outputColumnIndices: [0, 1, 2],
                filters: null,
                aggregate: null,
                options: new VectorizedScanExecutionOptions
                {
                    MaxDegreeOfParallelism = dop,
                    UseChannelScheduler = channel,
                });
            await using var r = await engine.ExecutePhysicalAsync(plan);
            var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
            Assert.Equal(2, col.Batches.Count);
            return col.RowCount;
        }

        var serial = await RunAsync(1, false);
        var parallel = await RunAsync(-1, false);
        var channel = await RunAsync(-1, true);
        Assert.Equal(7, serial);
        Assert.Equal(serial, parallel);
        Assert.Equal(serial, channel);
    }
}
