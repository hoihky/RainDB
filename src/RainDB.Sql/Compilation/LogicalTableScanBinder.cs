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
public sealed class LogicalTableScanBinder
{
    private readonly ScalarExpressionBindingPipeline _expressions = new();
    private readonly GroupedHavingBinder _havingBinder = new();
    private readonly UncorrelatedSubqueryBinder? _subqueryBinder;

    public LogicalTableScanBinder(UncorrelatedSubqueryBinder? subqueryBinder = null) =>
        _subqueryBinder = subqueryBinder;

    internal UncorrelatedSubqueryBinder? SubqueryBinder => _subqueryBinder;

    public IPhysicalPlan BindAndLower(
        LogicalTableScan scan,
        ICatalog catalog,
        VectorizedScanExecutionOptions scanOptions = default,
        PhysicalJoinAlgorithm joinAlgorithm = PhysicalJoinAlgorithm.Hash)
    {
        ArgumentNullException.ThrowIfNull(scan);
        ArgumentNullException.ThrowIfNull(catalog);
        if (!catalog.TryGetTable(scan.TableName, out var ts) || ts is null)
            throw new SqlCompileException($"Table '{scan.TableName}' does not exist in the catalog or is not a columnar table.");
        if (ts is not IColumnarTableSource colTable)
            throw new SqlCompileException($"Table '{scan.TableName}' is registered but is not a columnar table source.");
        var schema = ts.Schema;

        if (scan.GroupByColumns is { Count: > 0 })
            return BindHashAggregate(scan, colTable, schema, catalog, scanOptions);

        return BindVectorizedScan(scan, colTable.Id, schema, scan.TableName, catalog, scanOptions, joinAlgorithm);
    }

    internal IPhysicalPlan BindDerivedScan(
        LogicalDerivedTableScan scan,
        TableId ephemeralTableId,
        TableSchema derivedSchema,
        ICatalog catalog,
        VectorizedScanExecutionOptions scanOptions,
        PhysicalJoinAlgorithm joinAlgorithm)
    {
        var pseudo = new LogicalTableScan
        {
            TableName = scan.Alias,
            Projection = scan.Projection,
            SelectList = scan.SelectList,
            WhereConjuncts = scan.WhereConjuncts,
            SubqueryPredicates = scan.SubqueryPredicates,
            OrderBy = scan.OrderBy,
            Limit = scan.Limit,
        };
        return BindVectorizedScan(pseudo, ephemeralTableId, derivedSchema, scan.Alias, catalog, scanOptions, joinAlgorithm);
    }

    private IPhysicalPlan BindHashAggregate(
        LogicalTableScan scan,
        IColumnarTableSource colTable,
        TableSchema schema,
        ICatalog catalog,
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

        if (scan.HavingConjuncts is { Count: > 0 } && aggs.Count == 0)
            throw new SqlCompileException("HAVING requires at least one aggregate in the SELECT list.");

        ValidateWhereTableQualifiers(scan.WhereConjuncts, scan.TableName);
        var filters = BuildColumnCompareFilters(scan.WhereConjuncts, schema, scan.TableName);
        var having = _havingBinder.Bind(scan.HavingConjuncts, scan.SelectList!, scan.GroupByColumns!, schema, scan.TableName, aggs.ToArray());
        var aggPlan = new HashAggregatePhysicalPlan(colTable.Id, groupIndices, aggs.ToArray(), slots.ToArray(), filters, having, scanOptions);
        if (scan.OrderBy is not { Count: > 0 } && scan.Limit is null)
            return MaybeWrapDistinct(scan, aggPlan, catalog);

        var sortSpecs = BuildGroupedSortSpecs(scan.OrderBy ?? [], scan.SelectList!, scan.GroupByColumns!, schema, scan.TableName, slots, aggs);
        var outSchema = InferGroupedOutputSchema(schema, scan.GroupByColumns!, slots, aggs);
        return MaybeWrapDistinct(
            scan,
            new GroupedSortTopNPhysicalPlan(aggPlan, outSchema, sortSpecs, scan.Limit, scanOptions),
            catalog);
    }

    private static TableSchema InferGroupedOutputSchema(
        TableSchema input,
        IReadOnlyList<LogicalColumnProjection> groupBy,
        List<HashAggregateOutputSlot> slots,
        List<AggregateSpec> aggs)
    {
        var cols = new List<ColumnDef>(slots.Count);
        foreach (var slot in slots)
        {
            switch (slot.Kind)
            {
                case HashAggregateOutputColumnKind.GroupKey:
                    var gi = groupBy[slot.Ordinal];
                    var ix = -1;
                    for (var ci = 0; ci < input.Columns.Count; ci++)
                    {
                        if (input.Columns[ci].Name.Equals(gi.ColumnName, StringComparison.OrdinalIgnoreCase))
                        {
                            ix = ci;
                            break;
                        }
                    }

                    cols.Add(ix >= 0 ? input.Columns[ix] : new ColumnDef(gi.ColumnName, RainDbType.Int32));
                    break;
                case HashAggregateOutputColumnKind.Aggregate:
                    var spec = aggs[slot.Ordinal];
                    var src = spec.SourceColumnIndex >= 0 ? input.Columns[spec.SourceColumnIndex].Type : RainDbType.Int64;
                    cols.Add(new ColumnDef($"agg{slot.Ordinal}", AggregateTypeRules.ResultType(spec.Kind, src)));
                    break;
            }
        }

        return new TableSchema(cols);
    }

    private SortKeyPhysicalSpec[] BuildGroupedSortSpecs(
        IReadOnlyList<LogicalSortKey> orderBy,
        IReadOnlyList<LogicalSelectListItem> selectList,
        IReadOnlyList<LogicalColumnProjection> groupBy,
        TableSchema inputSchema,
        string tableName,
        List<HashAggregateOutputSlot> slots,
        List<AggregateSpec> aggs)
    {
        var arr = new SortKeyPhysicalSpec[orderBy.Count];
        for (var i = 0; i < orderBy.Count; i++)
        {
            var k = orderBy[i];
            if (k.SortExpression is not null)
                throw new SqlCompileException("ORDER BY expression is not supported with GROUP BY yet.");
            if (k.Column is null)
                throw new SqlCompileException("ORDER BY requires a column reference.");
            var outIx = ResolveGroupedOutputColumnIndex(k.Column, selectList, groupBy, tableName, slots, aggs);
            arr[i] = new SortKeyPhysicalSpec(outIx, k.Descending);
        }

        return arr;
    }

    private static int ResolveGroupedOutputColumnIndex(
        LogicalColumnProjection key,
        IReadOnlyList<LogicalSelectListItem> selectList,
        IReadOnlyList<LogicalColumnProjection> groupBy,
        string tableName,
        List<HashAggregateOutputSlot> slots,
        List<AggregateSpec> aggs)
    {
        for (var i = 0; i < selectList.Count; i++)
        {
            if (selectList[i] is LogicalColumnProjection col
                && string.Equals(col.ColumnName, key.ColumnName, StringComparison.OrdinalIgnoreCase)
                && string.Equals(col.QualifierTableName ?? tableName, key.QualifierTableName ?? tableName, StringComparison.OrdinalIgnoreCase))
                return i;
            if (selectList[i] is LogicalAggregationCall agg
                && key.ColumnName.Equals(agg.ArgumentColumnName ?? "*", StringComparison.OrdinalIgnoreCase))
                return i;
        }

        throw new SqlCompileException($"ORDER BY column '{key.ColumnName}' must appear in the SELECT list.");
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

        if (agg.IsDistinct)
        {
            if (agg.Kind != AggregateKind.Count || agg.ArgumentColumnName is null)
                throw new SqlCompileException("DISTINCT is only supported with COUNT(column).");
            var di = ResolveColumn(schema, agg.ArgumentColumnName, tableName);
            AggregateTypeRules.EnsureSupported(schema.Columns[di].Type, AggregateKind.CountDistinct);
            return new AggregateSpec(di, AggregateKind.CountDistinct);
        }

        switch (agg.Kind)
        {
            case AggregateKind.Count when agg.ArgumentColumnName is null:
                return new AggregateSpec(-1, AggregateKind.Count);
            case AggregateKind.Count:
            {
                var ci = ResolveColumn(schema, agg.ArgumentColumnName!, tableName);
                AggregateTypeRules.EnsureSupported(schema.Columns[ci].Type, AggregateKind.Count);
                return new AggregateSpec(ci, AggregateKind.Count);
            }
            case AggregateKind.Sum:
            case AggregateKind.Min:
            case AggregateKind.Max:
            {
                var si = ResolveColumn(schema, agg.ArgumentColumnName!, tableName);
                AggregateTypeRules.EnsureSupported(schema.Columns[si].Type, agg.Kind);
                return new AggregateSpec(si, agg.Kind);
            }
            default:
                throw new SqlCompileException($"Aggregate {agg.Kind} is not supported.");
        }
    }

    private IPhysicalPlan BindVectorizedScan(
        LogicalTableScan scan,
        TableId tableId,
        TableSchema schema,
        string tableName,
        ICatalog catalog,
        VectorizedScanExecutionOptions scanOptions,
        PhysicalJoinAlgorithm joinAlgorithm)
    {
        ValidateWhereTableQualifiers(scan.WhereConjuncts, tableName);
        var colCount = schema.Columns.Count;
        var filters = BuildColumnCompareFilters(scan.WhereConjuncts, schema, tableName);
        var (inSub, existsSub) = _subqueryBinder?.Bind(
            scan.SubqueryPredicates,
            catalog,
            tableName,
            schema,
            scanOptions,
            joinAlgorithm) ?? (null, null);

        AggregateSpec? aggregate = null;
        ScanOutputColumn[] outputColumns;
        if (scan.Aggregate is { } a)
        {
            AggregateSpec spec;
            if (a.Kind == AggregateKind.Count && a.ColumnName is null)
                spec = new AggregateSpec(-1, AggregateKind.Count);
            else if (a.Kind == AggregateKind.Count)
                spec = new AggregateSpec(ResolveColumn(schema, a.ColumnName!, tableName), AggregateKind.Count);
            else
                spec = new AggregateSpec(ResolveColumn(schema, a.ColumnName!, tableName), a.Kind);

            if (spec.SourceColumnIndex >= 0)
                AggregateTypeRules.EnsureSupported(schema.Columns[spec.SourceColumnIndex].Type, spec.Kind);
            else
                AggregateTypeRules.EnsureSupported(RainDbType.Int32, spec.Kind); // COUNT(*) — type ignored

            aggregate = spec;
            outputColumns = spec.SourceColumnIndex >= 0
                ? [new ScanOutputColumn(spec.SourceColumnIndex)]
                : [];
        }
        else if (scan.SelectList is { Count: > 0 } selectList)
        {
            outputColumns = BindScanOutputColumns(selectList, schema, tableName);
        }
        else if (scan.Projection is null)
        {
            outputColumns = new ScanOutputColumn[colCount];
            for (var i = 0; i < colCount; i++)
                outputColumns[i] = new ScanOutputColumn(i);
        }
        else
        {
            outputColumns = new ScanOutputColumn[scan.Projection.Count];
            for (var i = 0; i < scan.Projection.Count; i++)
            {
                var p = scan.Projection[i];
                ValidateProjectionTableQualifier(p, tableName);
                outputColumns[i] = new ScanOutputColumn(ResolveColumn(schema, p.ColumnName, tableName));
            }
        }

        var scanPlan = new VectorizedScanPhysicalPlan(tableId, outputColumns, filters, aggregate, scanOptions, inSub, existsSub);
        if (scan.Aggregate is not null)
            return scanPlan;

        IPhysicalPlan result = scanPlan;
        if (scan.OrderBy is { Count: > 0 } || scan.Limit is not null)
        {
            var sortSpecs = scan.OrderBy is { Count: > 0 } ob
                ? BuildTableSortKeySpecs(schema, tableName, ob)
                : Array.Empty<SortKeyPhysicalSpec>();
            var sortOutputIndices = Array.ConvertAll(outputColumns, static c =>
                c.Int32Expression is null && c.Float64Expression is null ? c.ColumnIndex : -1);
            if (sortOutputIndices.Any(static i => i < 0))
                throw new SqlCompileException("ORDER BY / LIMIT with computed SELECT expressions is not supported yet.");
            result = new SortTopNPhysicalPlan(tableId, sortOutputIndices, filters, sortSpecs, scan.Limit, scanOptions, inSub, existsSub);
        }

        return MaybeWrapDistinct(scan, result, catalog);
    }

    private static IPhysicalPlan MaybeWrapDistinct(LogicalTableScan scan, IPhysicalPlan plan, ICatalog catalog)
    {
        if (!scan.SelectDistinct)
            return plan;
        var schema = PhysicalPlanOutputSchema.Resolve(plan, catalog);
        return new DistinctPhysicalPlan(plan, schema);
    }

    private ScanOutputColumn[] BindScanOutputColumns(
        IReadOnlyList<LogicalSelectListItem> items,
        TableSchema schema,
        string tableName)
    {
        var cols = new ScanOutputColumn[items.Count];
        for (var i = 0; i < items.Count; i++)
        {
            switch (items[i])
            {
                case LogicalColumnProjection p:
                    ValidateProjectionTableQualifier(p, tableName);
                    cols[i] = new ScanOutputColumn(ResolveColumn(schema, p.ColumnName, tableName));
                    break;
                case LogicalScalarProjection sp:
                    cols[i] = BindScalarProjection(sp.Expression, schema, tableName);
                    break;
                default:
                    throw new SqlCompileException("Unsupported SELECT list item for a non-grouped scan.");
            }
        }

        return cols;
    }

    private ScanOutputColumn BindScalarProjection(LogicalScalarExpression expr, TableSchema schema, string tableName)
    {
        var t = _expressions.InferType(expr, schema, tableName);
        return t switch
        {
            RainDbType.Int32 => new ScanOutputColumn(-1, _expressions.BindInt32(expr, schema, tableName)),
            RainDbType.Float64 => new ScanOutputColumn(-1, Float64Expression: _expressions.BindFloat64(expr, schema, tableName)),
            _ => throw new SqlCompileException($"SELECT expression result type {t} is not supported."),
        };
    }

    private SortKeyPhysicalSpec[] BuildTableSortKeySpecs(
        TableSchema schema,
        string tableName,
        IReadOnlyList<LogicalSortKey> keys)
    {
        var arr = new SortKeyPhysicalSpec[keys.Count];
        for (var i = 0; i < keys.Count; i++)
        {
            var k = keys[i];
            if (k.SortExpression is { } sortExpr)
            {
                var t = _expressions.InferType(sortExpr, schema, tableName);
                arr[i] = t switch
                {
                    RainDbType.Int32 => new SortKeyPhysicalSpec(Descending: k.Descending, Int32SortExpression: _expressions.BindInt32(sortExpr, schema, tableName)),
                    RainDbType.Float64 => new SortKeyPhysicalSpec(Descending: k.Descending, Float64SortExpression: _expressions.BindFloat64(sortExpr, schema, tableName)),
                    _ => throw new SqlCompileException($"ORDER BY expression type {t} is not supported."),
                };
                continue;
            }

            if (k.Column is null)
                throw new SqlCompileException("ORDER BY requires a column or expression.");
            ValidateProjectionTableQualifier(k.Column, tableName);
            var ix = ResolveColumn(schema, k.Column.ColumnName, tableName);
            var colType = schema.Columns[ix].Type;
            if (colType != RainDbType.Utf8 && !ColumnTypeSizes.IsFixedWidth(colType))
            {
                throw new SqlCompileException(
                    $"ORDER BY does not support type {colType} for column '{schema.Columns[ix].Name}'.");
            }

            arr[i] = new SortKeyPhysicalSpec(ix, k.Descending);
        }

        return arr;
    }

    internal ColumnCompareFilter[]? BuildColumnCompareFilters(IReadOnlyList<SimpleWhereClause>? conjuncts, TableSchema schema, string tableName)
    {
        if (conjuncts is null or { Count: 0 })
            return null;
        var arr = new ColumnCompareFilter[conjuncts.Count];
        for (var i = 0; i < conjuncts.Count; i++)
            arr[i] = BuildColumnCompareFilter(conjuncts[i], schema, tableName);
        return arr;
    }

    /// <summary>Binds one conjunct to a column filter (shared with join lowering).</summary>
    internal ColumnCompareFilter BuildColumnCompareFilter(SimpleWhereClause where, TableSchema schema, string tableName)
    {
        if (where.UsesParameter)
            throw new SqlCompileException($"Parameter '@{where.ParameterName}' must be bound before physical compilation.");
        if (where.Literal is not { } literal)
            throw new SqlCompileException($"WHERE predicate is missing a literal value.");
        if (where.LeftExpression is { } lex)
        {
            ScalarExpressionBindingPipeline.ValidateTableRefs(lex, tableName);
            var exprType = _expressions.InferType(lex, schema, tableName);
            if (exprType == RainDbType.Int32)
            {
                var bound = _expressions.BindInt32(lex, schema, tableName);
                var imm = CoerceLiteralToImmediateBits(RainDbType.Int32, literal);
                return new ColumnCompareFilter(-1, where.Operator, imm, Int32Expression: bound);
            }

            if (exprType == RainDbType.Float64)
            {
                var bound = _expressions.BindFloat64(lex, schema, tableName);
                var imm = CoerceLiteralToImmediateBits(RainDbType.Float64, literal);
                return new ColumnCompareFilter(-1, where.Operator, imm, Float64Expression: bound);
            }

            throw new SqlCompileException($"WHERE expression type {exprType} is not supported.");
        }

        var wi = ResolveColumn(schema, where.ColumnName, tableName);
        var wt = schema.Columns[wi].Type;
        if (wt == RainDbType.Utf8)
        {
            if (where.Operator is not (ScalarCompareOp.Eq or ScalarCompareOp.Ne))
                throw new SqlCompileException(
                    $"WHERE on UTF-8 column '{where.ColumnName}' (table '{tableName}') supports only '=' and '!=' or '<>' with a string literal.");
            if (literal.Kind != SqlLiteralKind.String)
                throw new SqlCompileException(
                    $"UTF-8 column '{where.ColumnName}' (table '{tableName}') requires a single-quoted string literal.");
            var bytes = Encoding.UTF8.GetBytes(literal.Text);
            return new ColumnCompareFilter(wi, where.Operator, 0, bytes);
        }

        if (!ColumnTypeSizes.IsFixedWidth(wt))
            throw new SqlCompileException(
                $"WHERE comparisons on type {wt} (column '{where.ColumnName}', table '{tableName}') are not supported.");
        var bits = CoerceLiteralToImmediateBits(wt, literal);
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
        if (where?.LeftExpression is { } lex)
            ScalarExpressionBindingPipeline.ValidateTableRefs(lex, scannedTableName);
        if (where?.QualifierTableName is { } q && !q.Equals(scannedTableName, StringComparison.OrdinalIgnoreCase))
            throw new SqlCompileException(
                $"WHERE references table '{q}' but the FROM clause scans '{scannedTableName}' only.");
    }

    internal static int ResolveColumn(TableSchema schema, string name, string tableName)
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
