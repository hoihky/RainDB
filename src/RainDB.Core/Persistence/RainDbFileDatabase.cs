using System.Text.Json;
using System.Text.Json.Serialization;
using RainDB.Catalog;
using RainDB.Columnar;
using RainDB.Core.Catalog;
using RainDB.Core.Tables;
using RainDB.Persistence;
using RainDB.Schema;

namespace RainDB.Core.Persistence;

/// <summary>
/// File-backed RainDB dataset: <c>catalog.json</c> plus <c>tables/{tableId}/######.batch</c> segments.
/// Opening a directory hydrates <see cref="InMemoryCatalog"/> tables; appends on wired <see cref="MemoryTable"/> instances persist new batches automatically.
/// </summary>
public sealed class RainDbFileDatabase : IRainDbBatchPersistence
{
    public const string CatalogFileName = "catalog.json";
    public const string TablesDirectoryName = "tables";

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = true,
        PropertyNameCaseInsensitive = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    private readonly object _ioLock = new();

    private RainDbFileDatabase(string rootDirectory, InMemoryCatalog catalog)
    {
        RootDirectory = rootDirectory;
        Catalog = catalog;
    }

    /// <summary>Absolute root directory for this database.</summary>
    public string RootDirectory { get; }

    public InMemoryCatalog Catalog { get; }

    /// <summary>Creates or opens a directory-backed database. Existing <see cref="CatalogFileName"/> tables are loaded into memory.</summary>
    public static RainDbFileDatabase Open(string rootDirectory)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(rootDirectory);
        var root = Path.GetFullPath(rootDirectory);
        Directory.CreateDirectory(root);
        var catalog = new InMemoryCatalog();
        var db = new RainDbFileDatabase(root, catalog);
        db.HydrateFromDiskIfPresent();
        return db;
    }

    /// <summary>Registers a new <see cref="MemoryTable"/> wired for automatic batch persistence and writes an updated catalog snapshot.</summary>
    public MemoryTable CreateMemoryTable(string name, TableSchema schema, TableId? id = null, bool strictVectorChunkRows = false)
    {
        ArgumentNullException.ThrowIfNull(schema);
        var opts = new MemoryTableOptions(strictVectorChunkRows, BatchPersistence: this);
        var table = new MemoryTable(name, schema, id, opts);
        Catalog.Register(table);
        FlushCatalog();
        return table;
    }

    /// <summary>Rewrites <see cref="CatalogFileName"/> from the current in-memory catalog. Only <see cref="MemoryTable"/> entries are included (other <see cref="ITableSource"/> implementations are omitted).</summary>
    public void FlushCatalog()
    {
        lock (_ioLock)
        {
            var doc = BuildCatalogDocument(Catalog);
            WriteCatalogAtomic(doc);
        }
    }

    /// <summary>Serializes every <see cref="IColumnarTableSource"/> in <paramref name="catalog"/> into a fresh on-disk layout under <paramref name="rootDirectory"/>.</summary>
    public static void ExportCatalog(ICatalog catalog, string rootDirectory)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentException.ThrowIfNullOrWhiteSpace(rootDirectory);
        var root = Path.GetFullPath(rootDirectory);
        Directory.CreateDirectory(root);
        var tablesRoot = Path.Combine(root, TablesDirectoryName);
        if (Directory.Exists(tablesRoot))
            Directory.Delete(tablesRoot, recursive: true);
        Directory.CreateDirectory(tablesRoot);

        var doc = new RainDbCatalogDocument { FormatVersion = 1 };
        foreach (var name in catalog.TableNames.OrderBy(n => n, StringComparer.OrdinalIgnoreCase))
        {
            if (!catalog.TryGetTable(name, out var ts) || ts is not IColumnarTableSource col)
                throw new InvalidOperationException($"Table '{name}' must implement {nameof(IColumnarTableSource)} for export.");
            var entry = new RainDbTableDocument
            {
                Id = col.Id.ToString(),
                Name = col.Name,
                Columns = col.Schema.Columns.Select(c => new RainDbColumnDocument { Name = c.Name, Type = c.Type.ToString() }).ToList(),
            };
            doc.Tables.Add(entry);
            var tableDir = Path.Combine(tablesRoot, col.Id.ToString());
            Directory.CreateDirectory(tableDir);
            for (var i = 0; i < col.Batches.Count; i++)
            {
                var batchPath = Path.Combine(tableDir, $"{i:D6}.batch");
                WriteBatchFile(batchPath, col.Batches[i]);
            }
        }

        WriteCatalogAtomicToRoot(root, doc);
    }

    /// <summary>Loads tables from disk into a new in-memory catalog (no automatic persistence hook).</summary>
    public static InMemoryCatalog ImportCatalog(string rootDirectory)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(rootDirectory);
        var root = Path.GetFullPath(rootDirectory);
        var catalogPath = Path.Combine(root, CatalogFileName);
        if (!File.Exists(catalogPath))
            throw new FileNotFoundException("catalog.json not found.", catalogPath);
        var json = File.ReadAllText(catalogPath);
        var doc = JsonSerializer.Deserialize<RainDbCatalogDocument>(json, JsonOptions)
            ?? throw new InvalidDataException("catalog.json could not be deserialized.");
        if (doc.FormatVersion != 1)
            throw new NotSupportedException($"catalog formatVersion {doc.FormatVersion} is not supported.");
        var catalog = new InMemoryCatalog();
        foreach (var t in doc.Tables ?? [])
        {
            var schema = ToTableSchema(t);
            var id = TableId.From(Guid.ParseExact(t.Id, "N"));
            var table = new MemoryTable(t.Name, schema, id);
            LoadBatchesIntoTable(root, table);
            catalog.Register(table);
        }

        return catalog;
    }

    void IRainDbBatchPersistence.OnBatchAppended(TableId tableId, string tableName, int zeroBasedBatchIndex, IColumnarBatch batch)
    {
        ArgumentNullException.ThrowIfNull(batch);
        var path = GetBatchPath(tableId, zeroBasedBatchIndex);
        lock (_ioLock)
        {
            Directory.CreateDirectory(Path.GetDirectoryName(path)!);
            WriteBatchFile(path, batch);
        }
    }

    private void HydrateFromDiskIfPresent()
    {
        var catalogPath = Path.Combine(RootDirectory, CatalogFileName);
        if (!File.Exists(catalogPath))
            return;
        lock (_ioLock)
        {
            var json = File.ReadAllText(catalogPath);
            var doc = JsonSerializer.Deserialize<RainDbCatalogDocument>(json, JsonOptions)
                ?? throw new InvalidDataException("catalog.json could not be deserialized.");
            if (doc.FormatVersion != 1)
                throw new NotSupportedException($"catalog formatVersion {doc.FormatVersion} is not supported.");
            foreach (var t in doc.Tables ?? [])
            {
                var schema = ToTableSchema(t);
                var id = TableId.From(Guid.ParseExact(t.Id, "N"));
                var opts = new MemoryTableOptions(BatchPersistence: this);
                var table = new MemoryTable(t.Name, schema, id, opts);
                LoadBatchesIntoTable(RootDirectory, table);
                Catalog.Register(table);
            }
        }
    }

    private static void LoadBatchesIntoTable(string root, MemoryTable table)
    {
        var dir = Path.Combine(root, TablesDirectoryName, table.Id.ToString());
        if (!Directory.Exists(dir))
            return;
        foreach (var file in Directory.GetFiles(dir, "*.batch").OrderBy(f => f, StringComparer.Ordinal))
        {
            var bytes = File.ReadAllBytes(file);
            var batch = RainDbBatchBinaryCodec.DecodeBatch(bytes);
            table.AppendHydratedBatch(batch);
        }
    }

    private static RainDbCatalogDocument BuildCatalogDocument(InMemoryCatalog catalog)
    {
        var doc = new RainDbCatalogDocument { FormatVersion = 1 };
        foreach (var name in catalog.TableNames.OrderBy(n => n, StringComparer.OrdinalIgnoreCase))
        {
            if (!catalog.TryGetTable(name, out var ts) || ts is not MemoryTable mt)
                continue;
            doc.Tables.Add(new RainDbTableDocument
            {
                Id = mt.Id.ToString(),
                Name = mt.Name,
                Columns = mt.Schema.Columns.Select(c => new RainDbColumnDocument { Name = c.Name, Type = c.Type.ToString() }).ToList(),
            });
        }

        return doc;
    }

    private void WriteCatalogAtomic(RainDbCatalogDocument doc) => WriteCatalogAtomicToRoot(RootDirectory, doc);

    private static void WriteCatalogAtomicToRoot(string root, RainDbCatalogDocument doc)
    {
        var catalogPath = Path.Combine(root, CatalogFileName);
        var tmp = catalogPath + ".tmp";
        var json = JsonSerializer.Serialize(doc, JsonOptions);
        File.WriteAllText(tmp, json);
        File.Move(tmp, catalogPath, overwrite: true);
    }

    private static void WriteBatchFile(string path, IColumnarBatch batch)
    {
        var tmp = path + ".tmp";
        using (var fs = File.Create(tmp))
            RainDbBatchBinaryCodec.WriteBatch(fs, batch);
        File.Move(tmp, path, overwrite: true);
    }

    private string GetBatchPath(TableId tableId, int zeroBasedBatchIndex) =>
        Path.Combine(RootDirectory, TablesDirectoryName, tableId.ToString(), $"{zeroBasedBatchIndex:D6}.batch");

    private static TableSchema ToTableSchema(RainDbTableDocument t)
    {
        var list = t.Columns ?? [];
        if (list.Count == 0)
            throw new InvalidDataException($"Table '{t.Name}' has no columns in catalog.");
        var cols = new ColumnDef[list.Count];
        for (var i = 0; i < list.Count; i++)
        {
            var c = list[i];
            if (!Enum.TryParse<RainDbType>(c.Type, ignoreCase: false, out var rt))
                throw new InvalidDataException($"Unknown column type '{c.Type}' for column '{c.Name}'.");
            cols[i] = new ColumnDef(c.Name, rt);
        }

        return new TableSchema(cols);
    }

}

internal sealed class RainDbCatalogDocument
{
    public int FormatVersion { get; set; }

    public List<RainDbTableDocument> Tables { get; set; } = new();
}

internal sealed class RainDbTableDocument
{
    public string Id { get; set; } = "";

    public string Name { get; set; } = "";

    public List<RainDbColumnDocument> Columns { get; set; } = new();
}

internal sealed class RainDbColumnDocument
{
    public string Name { get; set; } = "";

    public string Type { get; set; } = "";
}
