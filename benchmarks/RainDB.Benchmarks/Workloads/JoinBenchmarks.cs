using BenchmarkDotNet.Attributes;
using RainDB.Benchmarks.Infrastructure;
using RainDB.Benchmarks.Plans;

namespace RainDB.Benchmarks.Workloads;

[MemoryDiagnoser]
public class JoinBenchmarks : BenchmarkHostLifecycle
{
    [Benchmark(Description = "hash_inner_join")]
    public async Task<long> HashInnerJoin()
    {
        var plan = BenchmarkPhysicalPlans.InnerHashJoinSalesToRegion(Host.SalesFact, Host.RegionDimension);
        await using var result = await Host.Engine.ExecutePhysicalAsync(plan).ConfigureAwait(false);
        return result.RowCount;
    }
}
