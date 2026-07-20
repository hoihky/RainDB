namespace RainDB.Columnar;

/// <summary>Horizontal slice: same <see cref="RowCount"/> across columns (columnar batch / morsel).</summary>
public interface IColumnarBatch
{
    int RowCount { get; }

    IReadOnlyList<IColumnChunk> Columns { get; }
}
