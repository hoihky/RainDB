namespace RainDB.Core.Persistence;

public enum MappedBatchBudgetExceededBehavior
{
    /// <summary>Evict least-recently-used mmap segments until under budget (materialize evicted batches in RAM).</summary>
    EvictColdBatches,

    /// <summary>Throw when registering a new mmap would exceed <see cref="RainDbFileDatabaseOptions.MappedBatchMemoryBudgetBytes"/>.</summary>
    Fail,
}
