using RainDB.Catalog;

namespace RainDB.Sql.Preparation;

/// <summary>Stable catalog generation for prepared-plan cache invalidation.</summary>
public sealed class CatalogSchemaFingerprint
{
    public long Compute(ICatalog catalog)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        var names = catalog.TableNames.OrderBy(static n => n, StringComparer.OrdinalIgnoreCase).ToArray();
        long hash = 17;
        foreach (var name in names)
        {
            if (!catalog.TryGetTable(name, out var table) || table is null)
                continue;
            hash = unchecked(hash * 31 + table.Id.GetHashCode());
            hash = unchecked(hash * 31 + table.SchemaVersion);
            hash = unchecked(hash * 31 + table.Schema.Columns.Count);
        }

        return hash;
    }
}
