using RainDB.Catalog;

namespace RainDB.Persistence;

/// <summary>Notifies the storage layer when a hydrated batch is scanned (LRU touch for mmap budget).</summary>
public interface IMappedBatchScanObserver
{
    void OnBatchScanned(TableId tableId, int batchIndex);
}
