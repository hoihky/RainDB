using RainDB.Catalog;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Persistence;
using RainDB.Schema;

namespace RainDB.Core.Tables;

/// <summary>In-memory columnar table: schema metadata + append-only <see cref="IColumnarBatch"/> segments.</summary>
public sealed class MemoryTable : ITableSource, IColumnarTableSource
{
    private readonly List<IColumnarBatch> _batches = new();
    private int _schemaVersion = 1;

    public MemoryTable(string name, TableSchema schema, TableId? id = null, MemoryTableOptions options = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        Name = name;
        Schema = schema;
        Id = id ?? TableId.New();
        Options = options;
    }

    public TableId Id { get; }

    public string Name { get; }

    public TableSchema Schema { get; }

    public MemoryTableOptions Options { get; }

    public int SchemaVersion => Volatile.Read(ref _schemaVersion);

    /// <summary>Fired after <see cref="BumpSchemaVersion"/> (schema evolution / cache invalidation hook).</summary>
    public event EventHandler<SchemaVersionChangedEventArgs>? SchemaVersionChanged;

    /// <summary>Columnar segments in insertion order (OLAP scans iterate these).</summary>
    public IReadOnlyList<IColumnarBatch> Batches => _batches;

    /// <summary>Total rows across all batches.</summary>
    public long RowCount
    {
        get
        {
            long sum = 0;
            foreach (var b in _batches)
                sum += b.RowCount;
            return sum;
        }
    }

    /// <summary>Appends a validated batch. Throws if schema/types/row counts do not match.</summary>
    public void AppendBatch(IColumnarBatch batch) => AppendCore(batch, notifyPersistence: true);

    /// <summary>Loads a batch from durable storage without invoking <see cref="IRainDbBatchPersistence"/> (hydration only).</summary>
    internal void AppendHydratedBatch(IColumnarBatch batch) => AppendCore(batch, notifyPersistence: false);

    private void AppendCore(IColumnarBatch batch, bool notifyPersistence)
    {
        ArgumentNullException.ThrowIfNull(batch);
        if (!Schema.MatchesBatch(batch))
            throw new ArgumentException("Batch does not match this table's schema.", nameof(batch));
        VectorChunkLimits.ValidateRowCount(batch.RowCount, Options.StrictVectorChunkRows);
        _batches.Add(batch);
        if (notifyPersistence && Options.BatchPersistence is { } persistence)
        {
            try
            {
                persistence.OnBatchAppended(Id, Name, _batches.Count - 1, batch);
            }
            catch
            {
                _batches.RemoveAt(_batches.Count - 1);
                throw;
            }
        }
    }

    /// <summary>Reserved for ALTER / schema migration; bumps version so planners invalidate caches.</summary>
    /// <returns>The new schema version after increment.</returns>
    public int BumpSchemaVersion()
    {
        var v = Interlocked.Increment(ref _schemaVersion);
        SchemaVersionChanged?.Invoke(this, new SchemaVersionChangedEventArgs(v));
        return v;
    }
}
