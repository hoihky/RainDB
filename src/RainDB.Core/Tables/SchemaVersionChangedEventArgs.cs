namespace RainDB.Core.Tables;

/// <summary>Raised after <see cref="MemoryTable.BumpSchemaVersion"/> increments the logical schema generation.</summary>
public sealed class SchemaVersionChangedEventArgs : EventArgs
{
    public SchemaVersionChangedEventArgs(int newVersion) => NewVersion = newVersion;

    public int NewVersion { get; }
}
