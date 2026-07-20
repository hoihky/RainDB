using RainDB.Columnar;

namespace RainDB.Core.Columnar;

/// <summary>Immutable columnar slice (one morsel). Typical row counts: 4K–64K for cache fit.</summary>
public sealed class ColumnarBatch : IColumnarBatch
{
    public ColumnarBatch(int rowCount, IReadOnlyList<IColumnChunk> columns)
    {
        if (rowCount < 0)
            throw new ArgumentOutOfRangeException(nameof(rowCount));
        ArgumentNullException.ThrowIfNull(columns);
        if (columns.Count == 0)
            throw new ArgumentException("At least one column required.", nameof(columns));
        for (var i = 0; i < columns.Count; i++)
        {
            var c = columns[i];
            ArgumentNullException.ThrowIfNull(c);
            if (c.RowCount != rowCount)
                throw new ArgumentException($"Column {i} row count {c.RowCount} != batch row count {rowCount}.", nameof(columns));
        }

        RowCount = rowCount;
        Columns = columns;
    }

    public int RowCount { get; }

    public IReadOnlyList<IColumnChunk> Columns { get; }
}
