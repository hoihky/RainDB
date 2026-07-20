using RainDB.Schema;

namespace RainDB.Core.Columnar;

public static class ColumnTypeSizes
{
    public static int FixedWidthBytes(RainDbType type) =>
        type switch
        {
            RainDbType.Int32 => sizeof(int),
            RainDbType.Int64 => sizeof(long),
            RainDbType.Float64 => sizeof(double),
            RainDbType.Boolean => sizeof(byte),
            RainDbType.Utf8 => throw new ArgumentException("UTF-8 columns are variable width.", nameof(type)),
            _ => throw new ArgumentOutOfRangeException(nameof(type)),
        };

    public static bool IsFixedWidth(RainDbType type) => type != RainDbType.Utf8;

    public static int NullBitmapBytes(int rowCount) => rowCount == 0 ? 0 : (rowCount + 7) >> 3;
}
