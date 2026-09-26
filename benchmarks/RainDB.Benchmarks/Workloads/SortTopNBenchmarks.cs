using BenchmarkDotNet.Attributes;
using RainDB.Benchmarks.Infrastructure;
using RainDB.Benchmarks.Plans;

namespace RainDB.Benchmarks.Workloads;

[MemoryDiagnoser]
public class SortTopNBenchmarks : BenchmarkHostLifecycle
{
    [Benchmark(Description = "order_by_limit_top100")]
    public async Task<long> OrderByAmountTop100()
    {
        var plan = BenchmarkPhysicalPlans.OrderByAmountTop100(Host.SalesFact);
        await using var result = await Host.Engine.ExecutePhysicalAsync(plan).ConfigureAwait(false);
        return result.RowCount;
    }
}
