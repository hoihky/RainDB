using RainDB;
using RainDB.Benchmarks.Infrastructure;
using RainDB.Core.Tables;

namespace RainDB.Benchmarks.Data;

/// <summary>Owns a configured <see cref="RainDbEngine"/> and benchmark tables for the lifetime of a benchmark class.</summary>
public sealed class BenchmarkEngineHost : IDisposable
{
    private BenchmarkEngineHost(
        RainDbEngine engine,
        MemoryTable salesFact,
        MemoryTable regionDimension,
        int factRowCount)
    {
        Engine = engine;
        SalesFact = salesFact;
        RegionDimension = regionDimension;
        FactRowCount = factRowCount;
    }

    public RainDbEngine Engine { get; }

    public MemoryTable SalesFact { get; }

    public MemoryTable RegionDimension { get; }

    public int FactRowCount { get; }

    public static BenchmarkEngineHost Create(BenchmarkWorkloadSize size)
    {
        var rowCount = (int)size;
        var engine = RainDbEngine.CreateDefault();
        var fact = SyntheticColumnarDataFactory.CreateSalesFact("sales_fact", rowCount);
        var dim = SyntheticColumnarDataFactory.CreateRegionDimension("region_dim", distinctRegions: 4_096);
        engine.Catalog.Register(fact);
        engine.Catalog.Register(dim);
        return new BenchmarkEngineHost(engine, fact, dim, rowCount);
    }

    public void Dispose()
    {
        // Engine holds managed resources only; tables are GC-owned.
    }
}
