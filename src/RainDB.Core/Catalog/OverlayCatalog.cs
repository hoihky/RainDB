using RainDB.Catalog;

namespace RainDB.Core.Catalog;

/// <summary>Catalog view that overlays ephemeral tables on a base catalog (derived-table execution).</summary>
public sealed class OverlayCatalog : ICatalog
{
    private readonly ICatalog _base;
    private readonly Dictionary<string, ITableSource> _byName;
    private readonly Dictionary<TableId, ITableSource> _byId;

    public OverlayCatalog(ICatalog baseCatalog, IEnumerable<ITableSource> overlays)
    {
        _base = baseCatalog ?? throw new ArgumentNullException(nameof(baseCatalog));
        _byName = new Dictionary<string, ITableSource>(StringComparer.OrdinalIgnoreCase);
        _byId = new Dictionary<TableId, ITableSource>();
        foreach (var t in overlays)
        {
            _byName[t.Name] = t;
            _byId[t.Id] = t;
        }
    }

    public bool TryGetTable(string name, out ITableSource? table)
    {
        if (_byName.TryGetValue(name, out table))
            return true;
        return _base.TryGetTable(name, out table);
    }

    public bool TryGetTable(TableId id, out ITableSource? table)
    {
        if (_byId.TryGetValue(id, out table))
            return true;
        return _base.TryGetTable(id, out table);
    }

    public IReadOnlyCollection<string> TableNames =>
        _byName.Keys.Concat(_base.TableNames.Where(n => !_byName.ContainsKey(n))).ToArray();

    public IReadOnlyCollection<TableId> TableIds =>
        _byId.Keys.Concat(_base.TableIds.Where(id => !_byId.ContainsKey(id))).ToArray();

    public void Register(ITableSource table) => _base.Register(table);
}
