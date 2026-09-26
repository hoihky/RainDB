using BenchmarkDotNet.Attributes;
using RainDB.Benchmarks.Infrastructure;
using RainDB.Benchmarks.Plans;
using RainDB.Execution;

namespace RainDB.Benchmarks.Workloads;

[MemoryDiagnoser]
public class ScanBenchmarks : BenchmarkHostLifecycle
{
    [Benchmark(Description = "scan_all_columns")]
    public async Task<long> ScanAllColumns()
    {
        var plan = BenchmarkPhysicalPlans.FullTableScan(Host.SalesFact);
        await using var result = await Host.Engine.ExecutePhysicalAsync(plan).ConfigureAwait(false);
        return result.RowCount;
    }
}
