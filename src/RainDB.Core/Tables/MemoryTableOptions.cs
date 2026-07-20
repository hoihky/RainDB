using RainDB.Persistence;

namespace RainDB.Core.Tables;

/// <summary>Optional ingest / storage policies for <see cref="MemoryTable"/>.</summary>
public readonly record struct MemoryTableOptions(bool StrictVectorChunkRows = false, IRainDbBatchPersistence? BatchPersistence = null)
{
    /// <summary>Default: do not enforce 64K–1M row bounds (allows small test batches); no disk append hook.</summary>
    public static MemoryTableOptions Default => default;
}
