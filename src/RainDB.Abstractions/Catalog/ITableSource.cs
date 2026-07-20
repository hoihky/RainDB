using RainDB.Schema;

namespace RainDB.Catalog;

/// <summary>Named table exposed to the catalog (ISP: separate from storage layout details).</summary>
public interface ITableSource
{
    /// <summary>Stable identity assigned at table construction.</summary>
    TableId Id { get; }

    string Name { get; }

    TableSchema Schema { get; }

    /// <summary>Bumped when logical schema changes; execution may cache plans per (Id, Version).</summary>
    int SchemaVersion { get; }
}
