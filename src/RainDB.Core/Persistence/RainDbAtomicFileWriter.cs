namespace RainDB.Core.Persistence;

/// <summary>Atomic replace-on-write helpers for catalog and batch files.</summary>
public sealed class RainDbAtomicFileWriter
{
    public void WriteAllText(string targetPath, string content)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(targetPath);
        var tmp = targetPath + ".tmp";
        File.WriteAllText(tmp, content);
        File.Move(tmp, targetPath, overwrite: true);
    }

    public void WriteStream(string targetPath, Action<Stream> writeBody)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(targetPath);
        ArgumentNullException.ThrowIfNull(writeBody);
        var tmp = targetPath + ".tmp";
        using (var fs = File.Create(tmp))
            writeBody(fs);
        File.Move(tmp, targetPath, overwrite: true);
    }

    public static bool IsCommittedBatchFileName(string fileName) =>
        fileName.EndsWith(".batch", StringComparison.OrdinalIgnoreCase)
        && !fileName.EndsWith(".tmp", StringComparison.OrdinalIgnoreCase);

    /// <summary>Committed catalog only; incomplete <c>catalog.json.tmp</c> is ignored.</summary>
    public static bool TryReadCommittedCatalogJson(string rootDirectory, out string json)
    {
        json = "";
        var catalogPath = Path.Combine(rootDirectory, RainDbFileDatabase.CatalogFileName);
        if (!File.Exists(catalogPath))
            return false;
        json = File.ReadAllText(catalogPath);
        return true;
    }
}
