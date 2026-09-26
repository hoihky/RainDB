using BenchmarkDotNet.Attributes;
using RainDB.Benchmarks.Infrastructure;
using RainDB.Benchmarks.Plans;
using RainDB.Execution;

namespace RainDB.Benchmarks.Workloads;

[MemoryDiagnoser]
public class HashAggregateBenchmarks : BenchmarkHostLifecycle
{
    [Benchmark(Description = "hash_group_by_sum")]
    public async Task<long> GroupByCategorySumAmount()
    {
        var plan = BenchmarkPhysicalPlans.GroupByCategorySumAmount(Host.SalesFact);
        await using var result = await Host.Engine.ExecutePhysicalAsync(plan).ConfigureAwait(false);
        if (result is IColumnarQueryResult col)
            return col.RowCount;
        return result.RowCount;
    }
}
