using System.Buffers.Binary;
using RainDB.Schema;

namespace RainDB.Query.Execution;

/// <summary>Consistent ordering for encoded fixed-width key parts (group keys, composite join keys).</summary>
internal static class FixedWidthKeyCompare
{
    internal static int ComparePart(RainDbType type, ulong xa, ulong xb) =>
        type switch
        {
            RainDbType.Int32 => ((int)(uint)xa).CompareTo((int)(uint)xb),
            RainDbType.Int64 => ((long)xa).CompareTo((long)xb),
            RainDbType.Float64 => CompareFloat64Bits(xa, xb),
            RainDbType.Boolean => ((xa != 0) ? 1 : 0).CompareTo(xb != 0 ? 1 : 0),
            _ => xa.CompareTo(xb),
        };

    private static int CompareFloat64Bits(ulong xa, ulong xb)
    {
        var da = BitConverter.Int64BitsToDouble((long)xa);
        var db = BitConverter.Int64BitsToDouble((long)xb);
        return da.CompareTo(db);
    }
}
