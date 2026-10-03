namespace RainDB.Core.Persistence;

public sealed class RainDbFileDatabaseOptions
{
    public bool PreferMmapBatchHydration { get; init; } = true;

    /// <summary>Maximum bytes charged for mmap-backed batch segments; default is unlimited.</summary>
    public long MappedBatchMemoryBudgetBytes { get; init; } = long.MaxValue;

    public MappedBatchBudgetExceededBehavior MappedBatchBudgetExceededBehavior { get; init; } =
        MappedBatchBudgetExceededBehavior.EvictColdBatches;

    public bool EnableInt32DictionaryEncoding { get; init; } = true;
}
