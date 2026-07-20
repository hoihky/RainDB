using System.Collections.Concurrent;
using RainDB.Catalog;

namespace RainDB.Core.Catalog;

public sealed class InMemoryCatalog : ICatalog
{
    private readonly ConcurrentDictionary<string, ITableSource> _byName =
        new(StringComparer.OrdinalIgnoreCase);

    private readonly ConcurrentDictionary<TableId, ITableSource> _byId = new();

    public IReadOnlyCollection<string> TableNames => _byName.Keys.ToArray();

    public IReadOnlyCollection<TableId> TableIds => _byId.Keys.ToArray();

    public bool TryGetTable(string name, out ITableSource? table) => _byName.TryGetValue(name, out table);

    public bool TryGetTable(TableId id, out ITableSource? table) => _byId.TryGetValue(id, out table);

    public void Register(ITableSource table)
    {
        ArgumentNullException.ThrowIfNull(table);
        if (!_byName.TryAdd(table.Name, table))
            throw new InvalidOperationException($"Table name already registered: {table.Name}");
        if (!_byId.TryAdd(table.Id, table))
        {
            _byName.TryRemove(table.Name, out _);
            throw new InvalidOperationException($"Table id already registered: {table.Id}");
        }
    }
}
