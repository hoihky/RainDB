using BenchmarkDotNet.Attributes;
using RainDB.Benchmarks.Infrastructure;
using RainDB.Benchmarks.Plans;

namespace RainDB.Benchmarks.Workloads;

[MemoryDiagnoser]
public class FilterProjectBenchmarks : BenchmarkHostLifecycle
{
    [Benchmark(Description = "filter_project")]
    public async Task<long> FilterAndProject()
    {
        var plan = BenchmarkPhysicalPlans.FilterAndProject(Host.SalesFact);
        await using var result = await Host.Engine.ExecutePhysicalAsync(plan).ConfigureAwait(false);
        return result.RowCount;
    }
}
