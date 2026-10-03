using RainDB.Execution;
using RainDB.Schema;

namespace RainDB.Query.Plans;

/// <summary>Compares one output column of a grouped query (post-aggregation).</summary>
public readonly record struct GroupOutputCompareFilter(
    int OutputColumnIndex,
    RainDbType ColumnType,
    ScalarCompareOp Op,
    long ImmediateBits,
    byte[]? Utf8LiteralBytes = null);
