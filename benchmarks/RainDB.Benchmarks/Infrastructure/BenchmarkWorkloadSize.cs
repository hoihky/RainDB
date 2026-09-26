namespace RainDB.Benchmarks.Infrastructure;

/// <summary>Row counts for benchmark datasets (tunable without editing each benchmark class).</summary>
public enum BenchmarkWorkloadSize
{
    /// <summary>Fast smoke / local iteration (~50k rows).</summary>
    Small = 50_000,

    /// <summary>Default manual baseline capture (~250k rows).</summary>
    Standard = 250_000,
}
