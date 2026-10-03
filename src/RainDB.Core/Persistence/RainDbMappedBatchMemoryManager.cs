using RainDB.Catalog;
using RainDB.Core.Tables;
using RainDB.Persistence;

namespace RainDB.Core.Persistence;

/// <summary>Tracks mmap-backed batch bytes and enforces an optional LRU budget per database.</summary>
public sealed class RainDbMappedBatchMemoryManager : IMappedBatchScanObserver
{
    private readonly object _lock = new();
    private readonly MappedBatchBudgetExceededBehavior _onExceeded;
    private readonly long _budgetBytes;
    private long _residentBytes;
    private readonly LinkedList<MappedBatchEntry> _lru = new();
    private readonly Dictionary<MappedBatchKey, LinkedListNode<MappedBatchEntry>> _index = new();

    public RainDbMappedBatchMemoryManager(long budgetBytes, MappedBatchBudgetExceededBehavior onExceeded)
    {
        if (budgetBytes < 0)
            throw new ArgumentOutOfRangeException(nameof(budgetBytes));
        _budgetBytes = budgetBytes;
        _onExceeded = onExceeded;
    }

    public long ResidentBytes
    {
        get
        {
            lock (_lock)
                return _residentBytes;
        }
    }

    public long BudgetBytes => _budgetBytes;

    public void RegisterMappedBatch(MemoryTable table, int batchIndex, MappedColumnarBatch mapped, string batchFilePath)
    {
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(mapped);
        ArgumentException.ThrowIfNullOrWhiteSpace(batchFilePath);
        var bytes = EstimateMappedBytes(mapped);
        lock (_lock)
        {
            var key = new MappedBatchKey(table.Id, batchIndex);
            if (_index.Remove(key, out var existing))
            {
                _lru.Remove(existing);
                _residentBytes -= existing.Value.ByteSize;
            }

            if (_budgetBytes != long.MaxValue)
            {
                if (_onExceeded == MappedBatchBudgetExceededBehavior.Fail && _residentBytes + bytes > _budgetBytes)
                    throw new InvalidOperationException(
                        $"Mapped batch memory budget ({_budgetBytes} bytes) would be exceeded by {bytes} byte segment.");

                while (_residentBytes + bytes > _budgetBytes && _lru.Last is not null)
                    EvictTail();
            }

            var entry = new MappedBatchEntry(table, batchIndex, mapped, batchFilePath, bytes);
            var node = _lru.AddFirst(entry);
            _index[key] = node;
            _residentBytes += bytes;
            table.AttachMappedBatch(batchIndex, mapped);
        }
    }

    public void OnBatchScanned(TableId tableId, int batchIndex)
    {
        lock (_lock)
        {
            var key = new MappedBatchKey(tableId, batchIndex);
            if (!_index.TryGetValue(key, out var node))
                return;
            _lru.Remove(node);
            _lru.AddFirst(node);
        }
    }

    private void EvictTail()
    {
        if (_lru.Last is not { } tail)
            return;
        var e = tail.Value;
        _lru.Remove(tail);
        _index.Remove(new MappedBatchKey(e.Table.Id, e.BatchIndex));
        _residentBytes -= e.ByteSize;

        var decoded = RainDbBatchBinaryCodec.DecodeBatch(File.ReadAllBytes(e.BatchFilePath));
        e.Table.ReplaceHydratedBatchAt(e.BatchIndex, decoded);
        e.Mapped.Dispose();
        e.Table.DetachMappedBatch(e.BatchIndex);
    }

    private static long EstimateMappedBytes(MappedColumnarBatch mapped)
    {
        long sum = 0;
        foreach (var col in mapped.Batch.Columns)
            sum += col.Values.Length + col.NullBitmap.Length;
        return Math.Max(sum, 4096);
    }

    private readonly record struct MappedBatchKey(TableId TableId, int BatchIndex);

    private sealed class MappedBatchEntry
    {
        public MappedBatchEntry(
            MemoryTable table,
            int batchIndex,
            MappedColumnarBatch mapped,
            string batchFilePath,
            long byteSize)
        {
            Table = table;
            BatchIndex = batchIndex;
            Mapped = mapped;
            BatchFilePath = batchFilePath;
            ByteSize = byteSize;
        }

        public MemoryTable Table { get; }

        public int BatchIndex { get; }

        public MappedColumnarBatch Mapped { get; }

        public string BatchFilePath { get; }

        public long ByteSize { get; }
    }
}
