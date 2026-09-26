using RainDB.Benchmarks.Data;
using RainDB.Benchmarks.Plans;
using RainDB.Execution;

namespace RainDB.Benchmarks.Execution;

/// <summary>Runs benchmark-shaped workloads without BenchmarkDotNet (used by smoke tests and optional diagnostics).</summary>
public static class BenchmarkWorkloadRunner
{
    public enum WorkloadKind
    {
        Scan,
        FilterProject,
        HashAggregate,
        HashJoin,
        SortTopN,
    }

    public static async Task<long> ExecuteAsync(BenchmarkEngineHost host, WorkloadKind kind)
    {
        ArgumentNullException.ThrowIfNull(host);
        IPhysicalPlan plan = kind switch
        {
            WorkloadKind.Scan => BenchmarkPhysicalPlans.FullTableScan(host.SalesFact),
            WorkloadKind.FilterProject => BenchmarkPhysicalPlans.FilterAndProject(host.SalesFact),
            WorkloadKind.HashAggregate => BenchmarkPhysicalPlans.GroupByCategorySumAmount(host.SalesFact),
            WorkloadKind.HashJoin => BenchmarkPhysicalPlans.InnerHashJoinSalesToRegion(host.SalesFact, host.RegionDimension),
            WorkloadKind.SortTopN => BenchmarkPhysicalPlans.OrderByAmountTop100(host.SalesFact),
            _ => throw new ArgumentOutOfRangeException(nameof(kind)),
        };

        await using var result = await host.Engine.ExecutePhysicalAsync(plan).ConfigureAwait(false);
        return result.RowCount;
    }

    public static IReadOnlyList<WorkloadKind> AllKinds =>
    [
        WorkloadKind.Scan,
        WorkloadKind.FilterProject,
        WorkloadKind.HashAggregate,
        WorkloadKind.HashJoin,
        WorkloadKind.SortTopN,
    ];
}
