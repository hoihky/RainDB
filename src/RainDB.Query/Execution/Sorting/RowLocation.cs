namespace RainDB.Query.Execution.Sorting;

/// <summary>Row pointer into a columnar batch list (batch index + row index).</summary>
internal readonly struct RowLocation : IEquatable<RowLocation>
{
    public RowLocation(int batchIndex, int rowIndex)
    {
        BatchIndex = batchIndex;
        RowIndex = rowIndex;
    }

    public int BatchIndex { get; }

    public int RowIndex { get; }

    public bool Equals(RowLocation other) =>
        BatchIndex == other.BatchIndex && RowIndex == other.RowIndex;

    public override bool Equals(object? obj) => obj is RowLocation other && Equals(other);

    public override int GetHashCode() => HashCode.Combine(BatchIndex, RowIndex);
}
