namespace RainDB.Query.Vectorized;

internal static class RowNullBitmap
{
    internal static bool IsNull(ReadOnlySpan<byte> nullBitmap, int row, bool hasNulls)
    {
        if (!hasNulls)
            return false;
        return (nullBitmap[row >> 3] & (1 << (row & 7))) != 0;
    }
}
