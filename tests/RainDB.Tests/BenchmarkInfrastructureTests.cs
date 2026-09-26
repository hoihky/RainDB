using System.Reflection;
using RainDB.Benchmarks.Data;
using RainDB.Benchmarks.Execution;
using RainDB.Benchmarks.Infrastructure;
using RainDB.Execution;

namespace RainDB.Tests;

public class BenchmarkInfrastructureTests
{
    [Fact]
    public void Synthetic_fact_table_row_count_matches_requested()
    {
        const int n = 12_345;
        var table = SyntheticColumnarDataFactory.CreateSalesFact("f", n, batchRows: 4096);
        Assert.Equal(n, table.RowCount);
        Assert.True(table.Batches.Count >= 2);
    }

    [Fact]
    public void Region_dimension_has_distinct_ids()
    {
        var dim = SyntheticColumnarDataFactory.CreateRegionDimension("d", 10);
        Assert.Equal(10, dim.RowCount);
    }

    [Theory]
    [InlineData(BenchmarkWorkloadRunner.WorkloadKind.Scan)]
    [InlineData(BenchmarkWorkloadRunner.WorkloadKind.FilterProject)]
    [InlineData(BenchmarkWorkloadRunner.WorkloadKind.HashAggregate)]
    [InlineData(BenchmarkWorkloadRunner.WorkloadKind.HashJoin)]
    [InlineData(BenchmarkWorkloadRunner.WorkloadKind.SortTopN)]
    public async Task Workload_runner_completes_on_small_dataset(BenchmarkWorkloadRunner.WorkloadKind kind)
    {
        using var host = BenchmarkEngineHost.Create(BenchmarkWorkloadSize.Small);
        var rows = await BenchmarkWorkloadRunner.ExecuteAsync(host, kind);
        Assert.True(rows > 0);
        switch (kind)
        {
            case BenchmarkWorkloadRunner.WorkloadKind.Scan:
            case BenchmarkWorkloadRunner.WorkloadKind.HashJoin:
                Assert.Equal(host.FactRowCount, rows);
                break;
            case BenchmarkWorkloadRunner.WorkloadKind.SortTopN:
                Assert.Equal(100, rows);
                break;
            case BenchmarkWorkloadRunner.WorkloadKind.HashAggregate:
                Assert.InRange(rows, 1, 256);
                break;
            case BenchmarkWorkloadRunner.WorkloadKind.FilterProject:
                Assert.True(rows < host.FactRowCount);
                break;
        }
    }

    [Fact]
    public void Benchmark_assembly_exposes_five_workload_benchmark_types()
    {
        var asm = typeof(BenchmarkWorkloadRunner).Assembly;
        var types = asm.GetTypes()
            .Where(t => t is { IsClass: true, IsAbstract: false, Namespace: "RainDB.Benchmarks.Workloads" }
                        && t.Name.EndsWith("Benchmarks", StringComparison.Ordinal))
            .Select(t => t.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();
        Assert.Equal(
            ["FilterProjectBenchmarks", "HashAggregateBenchmarks", "JoinBenchmarks", "ScanBenchmarks", "SortTopNBenchmarks"],
            types);
    }

    [Fact]
    public void All_workload_kinds_are_enumerated()
    {
        Assert.Equal(5, BenchmarkWorkloadRunner.AllKinds.Count);
    }
}
