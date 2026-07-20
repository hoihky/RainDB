using RainDB.Catalog;
using RainDB.Columnar;

namespace RainDB.Persistence;

/// <summary>Optional append hook for memory tables that opt in via <c>MemoryTableOptions.BatchPersistence</c> so each successful append can be mirrored to durable storage.</summary>
public interface IRainDbBatchPersistence
{
    /// <param name="zeroBasedBatchIndex">Index of the batch just appended (0-based).</param>
    void OnBatchAppended(TableId tableId, string tableName, int zeroBasedBatchIndex, IColumnarBatch batch);
}
