using System.Diagnostics;
using System.Globalization;
using System.Text;
using RainDB.Catalog;
using RainDB.Core.Columnar;
using RainDB.Execution;
using RainDB.Logical;
using RainDB.Query.Plans;
using RainDB.Schema;
using RainDB.Sql;

namespace RainDB.Sql.Compilation;

/// <summary>Binds <see cref="LogicalTableScan"/> to <see cref="IPhysicalPlan"/> (vectorized scan or hash aggregate).</summary>
public static class LogicalTableScanBinder
{
    public static IPhysicalPlan BindAndLower(
        LogicalTableScan scan,
        ICatalog catalog,
        VectorizedScanExecutionOptions scanOptions = default)
    {
        ArgumentNullException.ThrowIfNull(scan);
        ArgumentNullException.ThrowIfNull(catalog);
        if (!catalog.TryGetTable(scan.TableName, out var ts) || ts is null)
            throw new SqlCompileException($"Table '{scan.TableName}' does not exist in the catalog or is not a columnar table.");
        if (ts is not IColumnarTableSource colTable)
            throw new SqlCompileException($"Table '{scan.TableName}' is registered but is not a columnar table source.");
        var schema = ts.Schema;

        if (scan.GroupByColumns is { Count: > 0 })
            return BindHashAggregate(scan, colTable, schema, scanOptions);

        return BindVectorizedScan(scan, colTable, schema, scanOptions);
    }

    private static HashAggregatePhysicalPlan BindHashAggregate(
        LogicalTableScan scan,
        IColumnarTableSource colTable,
        TableSchema schema,
        VectorizedScanExecutionOptions scanOptions)
    {
        if (scan.SelectList is not { Count: > 0 })
            throw new SqlCompileException("GROUP BY query requires a SELECT list.");

        var groupIndices = new int[scan.GroupByColumns!.Count];
        for (var i = 0; i < scan.GroupByColumns.Count; i++)
        {
            var p = scan.GroupByColumns[i];
            ValidateProjectionTableQualifier(p, scan.TableName);
            var ix = ResolveColumn(schema, p.ColumnName, scan.TableName);
            groupIndices[i] = ix;
        }

        var keyOrdinal = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < scan.GroupByColumns.Count; i++)
            keyOrdinal[NormalizeGroupKey(scan.GroupByColumns[i], scan.TableName)] = i;

        var aggs = new List<AggregateSpec>();
        var slots = new List<HashAggregateOutputSlot>();
        foreach (var item in scan.SelectList)
        {
            switch (item)
            {
                case LogicalColumnProjection col:
                    if (!keyOrdinal.TryGetValue(NormalizeGroupKey(col, scan.TableName), out var ko))
                        throw new SqlCompileException(
                            $"Column '{ExplainProj(col)}' is not listed in GROUP BY for table '{scan.TableName}'.");
                    slots.Add(new HashAggregateOutputSlot(HashAggregateOutputColumnKind.GroupKey, ko));
                    break;
                case LogicalAggregationCall agg:
                    aggs.Add(ToAggregateSpec(schema, agg, scan.TableName));
                    slots.Add(new HashAggregateOutputSlot(HashAggregateOutputColumnKind.Aggregate, aggs.Count - 1));
                    break;
                default:
                    throw new SqlCompileException("Unsupported SELECT list item.");
            }
        }

        ValidateWhereTableQualifiers(scan.WhereConjuncts, scan.TableName);
        var filters = BuildColumnCompareFilters(scan.WhereConjuncts, schema, scan.TableName);
        return new HashAggregatePhysicalPlan(colTable.Id, groupIndices, aggs.ToArray(), slots.ToArray(), filters, scanOptions);
    }

    private static string NormalizeGroupKey(LogicalColumnProjection p, string scanTable) =>
        $"{p.QualifierTableName ?? scanTable}\u001f{p.ColumnName}";

    private static string ExplainProj(LogicalColumnProjection p) =>
        p.QualifierTableName is { } q ? $"{q}.{p.ColumnName}" : p.ColumnName;

    private static AggregateSpec ToAggregateSpec(TableSchema schema, LogicalAggregationCall agg, string tableName)
    {
        if (agg.ArgumentQualifierTableName is { } aq && !aq.Equals(tableName, StringComparison.OrdinalIgnoreCase))
        {
            throw new SqlCompileException(
                $"Aggregate argument references table '{aq}' but the FROM clause scans '{tableName}' only.");
        }

        switch (agg.Kind)
        {
            case AggregateKind.Count when agg.ArgumentColumnName is null:
                return new AggregateSpec(-1, AggregateKind.Count);
            case AggregateKind.Count:
            {
                var ci = ResolveColumn(schema, agg.ArgumentColumnName!, tableName);
                ValidateAggregate(schema.Columns[ci].Type, AggregateKind.Count);
                return new AggregateSpec(ci, AggregateKind.Count);
            }
            case AggregateKind.Sum:
            case AggregateKind.Min:
            case AggregateKind.Max:
            {
                var si = ResolveColumn(schema, agg.ArgumentColumnName!, tableName);
                ValidateAggregate(schema.Columns[si].Type, agg.Kind);
                return new AggregateSpec(si, agg.Kind);
            }
            default:
                throw new SqlCompileException($"Aggregate {agg.Kind} is not supported.");
        }
    }

    private static IPhysicalPlan BindVectorizedScan(
        LogicalTableScan scan,
        IColumnarTableSource colTable,
        TableSchema schema,
        VectorizedScanExecutionOptions scanOptions)
    {
        ValidateWhereTableQualifiers(scan.WhereConjuncts, scan.TableName);
        var colCount = schema.Columns.Count;
        var filters = BuildColumnCompareFilters(scan.WhereConjuncts, schema, scan.TableName);

        AggregateSpec? aggregate = null;
        int[] outputIndices;
        if (scan.Aggregate is { } a)
        {
            AggregateSpec spec;
            if (a.Kind == AggregateKind.Count && a.ColumnName is null)
                spec = new AggregateSpec(-1, AggregateKind.Count);
            else if (a.Kind == AggregateKind.Count)
                spec = new AggregateSpec(ResolveColumn(schema, a.ColumnName!, scan.TableName), AggregateKind.Count);
            else
                spec = new AggregateSpec(ResolveColumn(schema, a.ColumnName!, scan.TableName), a.Kind);

            if (spec.SourceColumnIndex >= 0)
                ValidateAggregate(schema.Columns[spec.SourceColumnIndex].Type, spec.Kind);
            else
                ValidateAggregate(RainDbType.Int32, spec.Kind); // COUNT(*) — type ignored

            aggregate = spec;
            outputIndices = spec.SourceColumnIndex >= 0 ? [spec.SourceColumnIndex] : [];
        }
        else if (scan.Projection is null)
        {
            outputIndices = new int[colCount];
            for (var i = 0; i < colCount; i++)
                outputIndices[i] = i;
        }
        else
        {
            outputIndices = new int[scan.Projection.Count];
            for (var i = 0; i < scan.Projection.Count; i++)
            {
                var p = scan.Projection[i];
                ValidateProjectionTableQualifier(p, scan.TableName);
                outputIndices[i] = ResolveColumn(schema, p.ColumnName, scan.TableName);
            }
        }

        var scanPlan = new VectorizedScanPhysicalPlan(colTable.Id, outputIndices, filters, aggregate, scanOptions);
        if (scan.Aggregate is not null)
            return scanPlan;
        if (scan.OrderBy is not { Count: > 0 } && scan.Limit is null)
            return scanPlan;

        var sortSpecs = scan.OrderBy is { Count: > 0 } ob
            ? BuildTableSortKeySpecs(schema, scan.TableName, ob)
            : Array.Empty<SortKeyPhysicalSpec>();
        return new SortTopNPhysicalPlan(colTable.Id, outputIndices, filters, sortSpecs, scan.Limit, scanOptions);
    }

    private static SortKeyPhysicalSpec[] BuildTableSortKeySpecs(
        TableSchema schema,
        string tableName,
        IReadOnlyList<LogicalSortKey> keys)
    {
        var arr = new SortKeyPhysicalSpec[keys.Count];
        for (var i = 0; i < keys.Count; i++)
        {
            var k = keys[i];
            ValidateProjectionTableQualifier(k.Column, tableName);
            var ix = ResolveColumn(schema, k.Column.ColumnName, tableName);
            var t = schema.Columns[ix].Type;
            if (t != RainDbType.Utf8 && !ColumnTypeSizes.IsFixedWidth(t))
            {
                throw new SqlCompileException(
                    $"ORDER BY does not support type {t} for column '{schema.Columns[ix].Name}'.");
            }

            arr[i] = new SortKeyPhysicalSpec(ix, k.Descending);
        }

        return arr;
    }

    internal static ColumnCompareFilter[]? BuildColumnCompareFilters(IReadOnlyList<SimpleWhereClause>? conjuncts, TableSchema schema, string tableName)
    {
        if (conjuncts is null or { Count: 0 })
            return null;
        var arr = new ColumnCompareFilter[conjuncts.Count];
        for (var i = 0; i < conjuncts.Count; i++)
            arr[i] = BuildColumnCompareFilter(conjuncts[i], schema, tableName);
        return arr;
    }

    /// <summary>Binds one conjunct to a column filter (shared with join lowering).</summary>
    internal static ColumnCompareFilter BuildColumnCompareFilter(SimpleWhereClause where, TableSchema schema, string tableName)
    {
        var wi = ResolveColumn(schema, where.ColumnName, tableName);
        var wt = schema.Columns[wi].Type;
        if (wt == RainDbType.Utf8)
        {
            if (where.Operator is not (ScalarCompareOp.Eq or ScalarCompareOp.Ne))
                throw new SqlCompileException(
                    $"WHERE on UTF-8 column '{where.ColumnName}' (table '{tableName}') supports only '=' and '!=' or '<>' with a string literal.");
            if (where.Literal.Kind != SqlLiteralKind.String)
                throw new SqlCompileException(
                    $"UTF-8 column '{where.ColumnName}' (table '{tableName}') requires a single-quoted string literal.");
            var bytes = Encoding.UTF8.GetBytes(where.Literal.Text);
            return new ColumnCompareFilter(wi, where.Operator, 0, bytes);
        }

        if (!ColumnTypeSizes.IsFixedWidth(wt))
            throw new SqlCompileException(
                $"WHERE comparisons on type {wt} (column '{where.ColumnName}', table '{tableName}') are not supported.");
        var bits = CoerceLiteralToImmediateBits(wt, where.Literal);
        return new ColumnCompareFilter(wi, where.Operator, bits);
    }

    internal static void ValidateWhereTableQualifiers(IReadOnlyList<SimpleWhereClause>? conjuncts, string scannedTableName)
    {
        if (conjuncts is null)
            return;
        foreach (var w in conjuncts)
            ValidateWhereTableQualifier(w, scannedTableName);
    }

    private static void ValidateProjectionTableQualifier(LogicalColumnProjection p, string scannedTableName)
    {
        if (p.QualifierTableName is { } q && !q.Equals(scannedTableName, StringComparison.OrdinalIgnoreCase))
            throw new SqlCompileException(
                $"SELECT references table '{q}' but the FROM clause scans '{scannedTableName}' only.");
    }

    internal static void ValidateWhereTableQualifier(SimpleWhereClause? where, string scannedTableName)
    {
        if (where?.QualifierTableName is { } q && !q.Equals(scannedTableName, StringComparison.OrdinalIgnoreCase))
            throw new SqlCompileException(
                $"WHERE references table '{q}' but the FROM clause scans '{scannedTableName}' only.");
    }

    private static void ValidateAggregate(RainDbType columnType, AggregateKind kind)
    {
        switch (kind)
        {
            case AggregateKind.Count:
                return;
            case AggregateKind.Sum when columnType is RainDbType.Int32 or RainDbType.Int64 or RainDbType.Float64:
                return;
            case AggregateKind.Min or AggregateKind.Max when columnType == RainDbType.Float64:
                return;
            default:
                throw new SqlCompileException($"Aggregate {kind} is not supported for type {columnType}.");
        }
    }

    private static int ResolveColumn(TableSchema schema, string name, string tableName)
    {
        for (var i = 0; i < schema.Columns.Count; i++)
        {
            if (schema.Columns[i].Name.Equals(name, StringComparison.OrdinalIgnoreCase))
                return i;
        }

        throw new SqlCompileException($"Unknown column '{name}' in table '{tableName}'.");
    }

    private static long CoerceLiteralToImmediateBits(RainDbType columnType, SqlLiteral literal)
    {
        return columnType switch
        {
            RainDbType.Boolean => CoerceBool(literal),
            RainDbType.Int32 => CoerceInt32(literal),
            RainDbType.Int64 => CoerceInt64(literal),
            RainDbType.Float64 => CoerceFloat64(literal),
            _ => throw new SqlCompileException($"Literal binding for {columnType} is not supported."),
        };
    }

    private static long CoerceBool(SqlLiteral literal)
    {
        if (literal.Kind != SqlLiteralKind.Boolean)
            throw new SqlCompileException("Boolean column requires literal TRUE or FALSE.");
        return literal.Text.Equals("TRUE", StringComparison.OrdinalIgnoreCase) ? 1L : 0L;
    }

    private static long CoerceInt32(SqlLiteral literal)
    {
        switch (literal.Kind)
        {
            case SqlLiteralKind.Integer:
                if (!int.TryParse(literal.Text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v))
                    throw new SqlCompileException($"Invalid integer literal '{literal.Text}'.");
                return v;
            case SqlLiteralKind.Float:
                throw new SqlCompileException("Cannot use a floating literal for an Int32 column (cast not supported).");
            case SqlLiteralKind.Boolean:
                throw new SqlCompileException("Cannot use a boolean literal for an Int32 column.");
            case SqlLiteralKind.String:
                throw new SqlCompileException("Cannot use a string literal for an Int32 column.");
            default:
                throw new UnreachableException();
        }
    }

    private static long CoerceInt64(SqlLiteral literal)
    {
        switch (literal.Kind)
        {
            case SqlLiteralKind.Integer:
                if (!long.TryParse(literal.Text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v))
                    throw new SqlCompileException($"Invalid integer literal '{literal.Text}'.");
                return v;
            case SqlLiteralKind.Float:
                throw new SqlCompileException("Cannot use a floating literal for an Int64 column (cast not supported).");
            case SqlLiteralKind.Boolean:
                throw new SqlCompileException("Cannot use a boolean literal for an Int64 column.");
            case SqlLiteralKind.String:
                throw new SqlCompileException("Cannot use a string literal for an Int64 column.");
            default:
                throw new UnreachableException();
        }
    }

    private static long CoerceFloat64(SqlLiteral literal)
    {
        switch (literal.Kind)
        {
            case SqlLiteralKind.Float:
                if (!double.TryParse(literal.Text, NumberStyles.Float, CultureInfo.InvariantCulture, out var d))
                    throw new SqlCompileException($"Invalid floating literal '{literal.Text}'.");
                return BitConverter.DoubleToInt64Bits(d);
            case SqlLiteralKind.Integer:
                if (!long.TryParse(literal.Text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var iv))
                    throw new SqlCompileException($"Invalid integer literal '{literal.Text}'.");
                return BitConverter.DoubleToInt64Bits(iv);
            case SqlLiteralKind.Boolean:
                throw new SqlCompileException("Cannot use a boolean literal for a Float64 column.");
            case SqlLiteralKind.String:
                throw new SqlCompileException("Cannot use a string literal for a Float64 column.");
            default:
                throw new UnreachableException();
        }
    }
}
