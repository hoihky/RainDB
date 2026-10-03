using System.Globalization;
using System.Text;
using RainDB.Execution;
using RainDB.Logical;
using RainDB.Query.Plans;
using RainDB.Schema;
using RainDB.Sql;

namespace RainDB.Sql.Compilation;

/// <summary>Maps <c>HAVING</c> conjuncts to output-column compare filters on grouped query results.</summary>
public sealed class GroupedHavingBinder
{
    public GroupOutputCompareFilter[]? Bind(
        IReadOnlyList<LogicalHavingConjunct>? conjuncts,
        IReadOnlyList<LogicalSelectListItem> selectList,
        IReadOnlyList<LogicalColumnProjection> groupBy,
        TableSchema schema,
        string tableName,
        AggregateSpec[] aggregates)
    {
        if (conjuncts is null or { Count: 0 })
            return null;
        var outputIndex = BuildOutputIndex(selectList, schema, tableName, aggregates);
        var filters = new GroupOutputCompareFilter[conjuncts.Count];
        for (var i = 0; i < conjuncts.Count; i++)
            filters[i] = BindOne(conjuncts[i], outputIndex, schema, tableName, aggregates);
        return filters;
    }

    private static Dictionary<string, (int OutputIndex, RainDbType Type)> BuildOutputIndex(
        IReadOnlyList<LogicalSelectListItem> selectList,
        TableSchema schema,
        string tableName,
        AggregateSpec[] aggregates)
    {
        var map = new Dictionary<string, (int, RainDbType)>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < selectList.Count; i++)
        {
            switch (selectList[i])
            {
                case LogicalColumnProjection col:
                {
                    var ix = LogicalTableScanBinder.ResolveColumn(schema, col.ColumnName, tableName);
                    map[NormalizeGroupKey(col, tableName)] = (i, schema.Columns[ix].Type);
                    break;
                }
                case LogicalAggregationCall agg:
                    map[NormalizeAggKey(agg)] = (i, ResolveAggregateResultType(agg, schema, aggregates));
                    break;
            }
        }

        return map;
    }

    private GroupOutputCompareFilter BindOne(
        LogicalHavingConjunct conjunct,
        Dictionary<string, (int OutputIndex, RainDbType Type)> outputIndex,
        TableSchema schema,
        string tableName,
        AggregateSpec[] aggregates)
    {
        if (conjunct.GroupKeyColumn is { } gk)
        {
            var key = NormalizeGroupKey(gk, tableName);
            if (!outputIndex.TryGetValue(key, out var slot))
                throw new SqlCompileException("HAVING references a group key column that is not in the SELECT list.");
            var bits = CoerceLiteral(slot.Type, conjunct.Literal);
            return new GroupOutputCompareFilter(slot.OutputIndex, slot.Type, conjunct.Operator, bits);
        }

        if (conjunct.Aggregate is not { } agg)
            throw new SqlCompileException("HAVING conjunct must reference a group key or aggregate.");

        var aggKey = NormalizeAggKey(agg);
        if (!outputIndex.TryGetValue(aggKey, out var aggSlot))
            throw new SqlCompileException("HAVING aggregate must appear in the SELECT list.");

        var resultType = ResolveAggregateResultType(agg, schema, aggregates);
        var imm = CoerceLiteral(resultType, conjunct.Literal);
        if (resultType == RainDbType.Utf8 && conjunct.Operator is not (ScalarCompareOp.Eq or ScalarCompareOp.Ne))
            throw new SqlCompileException("HAVING on UTF-8 supports only '=' and '!='.");
        if (resultType == RainDbType.Utf8)
        {
            var bytes = Encoding.UTF8.GetBytes(conjunct.Literal.Text);
            return new GroupOutputCompareFilter(aggSlot.OutputIndex, resultType, conjunct.Operator, 0, bytes);
        }

        return new GroupOutputCompareFilter(aggSlot.OutputIndex, resultType, conjunct.Operator, imm);
    }

    private static RainDbType ResolveAggregateResultType(LogicalAggregationCall agg, TableSchema schema, AggregateSpec[] specs)
    {
        foreach (var s in specs)
        {
            if (s.Kind != agg.Kind)
                continue;
            if (agg.Kind == AggregateKind.Count)
                return RainDbType.Int64;
            var src = schema.Columns[s.SourceColumnIndex].Type;
            return AggregateTypeRules.ResultType(agg.Kind, src);
        }

        throw new SqlCompileException("HAVING aggregate does not match SELECT aggregates.");
    }

    private static string NormalizeGroupKey(LogicalColumnProjection p, string scanTable) =>
        $"{p.QualifierTableName ?? scanTable}\u001f{p.ColumnName}";

    private static string NormalizeAggKey(LogicalAggregationCall a) =>
        $"{a.Kind}\u001f{a.ArgumentQualifierTableName}\u001f{a.ArgumentColumnName}";

    private static long CoerceLiteral(RainDbType type, SqlLiteral literal) =>
        type switch
        {
            RainDbType.Int32 => CoerceInt32(literal),
            RainDbType.Int64 => CoerceInt64(literal),
            RainDbType.Float64 => CoerceFloat64(literal),
            RainDbType.Boolean => CoerceBool(literal),
            RainDbType.Utf8 => 0,
            _ => throw new SqlCompileException($"HAVING literal coercion for {type} is not supported."),
        };

    private static long CoerceBool(SqlLiteral literal)
    {
        if (literal.Kind != SqlLiteralKind.Boolean)
            throw new SqlCompileException("Boolean HAVING literal required.");
        return literal.Text.Equals("TRUE", StringComparison.OrdinalIgnoreCase) ? 1L : 0L;
    }

    private static long CoerceInt32(SqlLiteral literal)
    {
        if (!int.TryParse(literal.Text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v))
            throw new SqlCompileException($"Invalid integer literal '{literal.Text}'.");
        return v;
    }

    private static long CoerceInt64(SqlLiteral literal)
    {
        if (!long.TryParse(literal.Text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v))
            throw new SqlCompileException($"Invalid integer literal '{literal.Text}'.");
        return v;
    }

    private static long CoerceFloat64(SqlLiteral literal)
    {
        if (literal.Kind == SqlLiteralKind.Integer
            && int.TryParse(literal.Text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var iv))
            return BitConverter.DoubleToInt64Bits(iv);
        if (literal.Kind == SqlLiteralKind.Float
            && double.TryParse(literal.Text, NumberStyles.Float, CultureInfo.InvariantCulture, out var dv))
            return BitConverter.DoubleToInt64Bits(dv);
        throw new SqlCompileException("Float HAVING literal required.");
    }
}
