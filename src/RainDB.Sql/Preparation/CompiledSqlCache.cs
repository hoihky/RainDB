using System.Collections.Concurrent;
using RainDB.Execution;

namespace RainDB.Sql.Preparation;

/// <summary>Caches physical plans for parameter-free SQL keyed by text and catalog fingerprint.</summary>
public sealed class CompiledSqlCache
{
    private readonly ConcurrentDictionary<CacheKey, IPhysicalPlan> _entries = new();

    public bool TryGet(string sql, long schemaFingerprint, out IPhysicalPlan? plan)
    {
        if (_entries.TryGetValue(new CacheKey(sql, schemaFingerprint), out var cached))
        {
            plan = cached;
            return true;
        }

        plan = null;
        return false;
    }

    public void Store(string sql, long schemaFingerprint, IPhysicalPlan plan) =>
        _entries[new CacheKey(sql, schemaFingerprint)] = plan;

    public void Clear() => _entries.Clear();

    private readonly record struct CacheKey(string Sql, long SchemaFingerprint);
}
