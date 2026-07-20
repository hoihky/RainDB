namespace RainDB.Core.Columnar;

/// <summary>
/// DuckDB-style vector sizing: each materialized batch should stay in <see cref="MinRows"/>–<see cref="MaxRows"/>
/// for L1/L2 cache locality (policy is optional until ingest pipelines are strict).
/// </summary>
public static class VectorChunkLimits
{
    /// <summary>64K rows — lower bound for “wide vector” batches in OLAP engines.</summary>
    public const int MinRows = 64 * 1024;

    /// <summary>1M rows — upper bound to cap resident set per vector.</summary>
    public const int MaxRows = 1024 * 1024;

    /// <summary>
    /// When <paramref name="enforce"/> is true, requires <paramref name="rowCount"/> == 0 (empty batch) or
    /// <see cref="MinRows"/> ≤ rowCount ≤ <see cref="MaxRows"/>.
    /// </summary>
    public static void ValidateRowCount(int rowCount, bool enforce)
    {
        if (!enforce)
            return;
        if (rowCount == 0)
            return;
        if (rowCount < MinRows || rowCount > MaxRows)
        {
            throw new ArgumentOutOfRangeException(nameof(rowCount), rowCount,
                $"When strict vector sizing is enabled, row count must be 0 or in [{MinRows}, {MaxRows}].");
        }
    }
}
