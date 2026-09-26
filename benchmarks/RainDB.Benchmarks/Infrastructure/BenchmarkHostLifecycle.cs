using BenchmarkDotNet.Attributes;
using RainDB.Benchmarks.Data;

namespace RainDB.Benchmarks.Infrastructure;

/// <summary>Template Method: shared global setup/teardown for engine + synthetic tables.</summary>
public abstract class BenchmarkHostLifecycle
{
    protected BenchmarkEngineHost Host { get; private set; } = null!;

    [GlobalSetup]
    public void SetupBenchmarkHost() => Host = BenchmarkEngineHost.Create(WorkloadSize);

    [GlobalCleanup]
    public void CleanupBenchmarkHost()
    {
        Host.Dispose();
        Host = null!;
    }

    protected virtual BenchmarkWorkloadSize WorkloadSize => BenchmarkWorkloadSize.Standard;
}
