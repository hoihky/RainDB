using BenchmarkDotNet.Running;
using RainDB.Benchmarks.Infrastructure;
using RainDB.Benchmarks.Workloads;

namespace RainDB.Benchmarks;

internal static class Program
{
    public static void Main(string[] args)
    {
        var config = new RainDbBenchmarkConfig();
        var switcher = BenchmarkSwitcher.FromTypes([
            typeof(ScanBenchmarks),
            typeof(FilterProjectBenchmarks),
            typeof(HashAggregateBenchmarks),
            typeof(JoinBenchmarks),
            typeof(SortTopNBenchmarks),
        ]);
        switcher.Run(args, config);
    }
}
