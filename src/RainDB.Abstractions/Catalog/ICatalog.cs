namespace RainDB.Catalog;

/// <summary>Metadata registry; execution resolves tables through this port (DIP).</summary>
public interface ICatalog
{
    IReadOnlyCollection<string> TableNames { get; }

    IReadOnlyCollection<TableId> TableIds { get; }

    bool TryGetTable(string name, out ITableSource? table);

    bool TryGetTable(TableId id, out ITableSource? table);

    void Register(ITableSource table);
}
