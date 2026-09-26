using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Exporters.Csv;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Order;

namespace RainDB.Benchmarks.Infrastructure;

/// <summary>Default BenchmarkDotNet configuration for RainDB manual baselines.</summary>
public sealed class RainDbBenchmarkConfig : ManualConfig
{
    public RainDbBenchmarkConfig()
    {
        AddJob(Job.Default.WithWarmupCount(2).WithIterationCount(5).WithLaunchCount(1));
        AddLogger(ConsoleLogger.Default);
        AddExporter(MarkdownExporter.Default);
        AddExporter(CsvExporter.Default);
        AddDiagnoser(MemoryDiagnoser.Default);
        WithOrderer(new DefaultOrderer(SummaryOrderPolicy.FastestToSlowest));
        WithOptions(ConfigOptions.DisableOptimizationsValidator);
    }
}
