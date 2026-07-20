using RainDB.Columnar;

namespace RainDB.Schema;

/// <summary>Immutable logical schema for a table or intermediate operator output (SRP).</summary>
public sealed class TableSchema
{
    public TableSchema(IReadOnlyList<ColumnDef> columns)
    {
        ArgumentNullException.ThrowIfNull(columns);
        if (columns.Count == 0)
            throw new ArgumentException("Schema requires at least one column.", nameof(columns));
        Columns = columns;
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var c in columns)
        {
            if (!names.Add(c.Name))
                throw new ArgumentException($"Duplicate column name: {c.Name}", nameof(columns));
        }
    }

    public IReadOnlyList<ColumnDef> Columns { get; }

    /// <summary>Validates column count, types, and per-chunk row counts for append into a table with this schema.</summary>
    public bool MatchesBatch(IColumnarBatch batch)
    {
        ArgumentNullException.ThrowIfNull(batch);
        if (batch.Columns.Count != Columns.Count || batch.RowCount < 0)
            return false;
        if (batch.RowCount == 0)
            return batch.Columns.All(c => c.RowCount == 0);
        for (var i = 0; i < Columns.Count; i++)
        {
            var col = batch.Columns[i];
            if (col.RowCount != batch.RowCount)
                return false;
            if (col.PhysicalType != Columns[i].Type)
                return false;
        }

        return true;
    }
}
