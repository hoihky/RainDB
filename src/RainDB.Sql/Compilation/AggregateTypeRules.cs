using RainDB.Execution;
using RainDB.Schema;
using RainDB.Sql;

namespace RainDB.Sql.Compilation;

/// <summary>Central rules for which aggregates are legal per column type (SQL bind + physical validation).</summary>
public static class AggregateTypeRules
{
    public static void EnsureSupported(RainDbType columnType, AggregateKind kind)
    {
        if (IsSupported(columnType, kind))
            return;
        throw new SqlCompileException($"Aggregate {kind} is not supported for type {columnType}.");
    }

    public static bool IsSupported(RainDbType columnType, AggregateKind kind) =>
        kind switch
        {
            AggregateKind.Count or AggregateKind.CountDistinct => true,
            AggregateKind.Sum => columnType is RainDbType.Int32 or RainDbType.Int64 or RainDbType.Float64,
            AggregateKind.Min or AggregateKind.Max => columnType is RainDbType.Int32 or RainDbType.Int64 or RainDbType.Float64 or RainDbType.Utf8,
            _ => false,
        };

    public static RainDbType ResultType(AggregateKind kind, RainDbType sourceType) =>
        kind switch
        {
            AggregateKind.Count or AggregateKind.CountDistinct => RainDbType.Int64,
            AggregateKind.Sum when sourceType == RainDbType.Float64 => RainDbType.Float64,
            AggregateKind.Sum => RainDbType.Int64,
            AggregateKind.Min or AggregateKind.Max => sourceType,
            _ => throw new SqlCompileException($"Unknown aggregate {kind}."),
        };
}
