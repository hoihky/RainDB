using RainDB.Catalog;
using RainDB.Core.Columnar;
using RainDB.Execution;
using RainDB.Logical;
using RainDB.Query.Plans;
using RainDB.Schema;
using RainDB.Sql;

namespace RainDB.Sql.Compilation;

/// <summary>Binds <see cref="LogicalInnerJoin"/> to <see cref="JoinPhysicalPlan"/> or <see cref="GroupedJoinPhysicalPlan"/>.</summary>
public static class LogicalJoinBinder
{
    public static IPhysicalPlan BindAndLower(
        LogicalInnerJoin join,
        ICatalog catalog,
        PhysicalJoinAlgorithm algorithm,
        VectorizedScanExecutionOptions scanOptions = default)
    {
        ArgumentNullException.ThrowIfNull(join);
        ArgumentNullException.ThrowIfNull(catalog);
        if (join.LeftKeyColumns.Count != join.RightKeyColumns.Count || join.LeftKeyColumns.Count == 0)
            throw new SqlCompileException("JOIN requires at least one AND-separated equi-predicate.");

        if (!catalog.TryGetTable(join.LeftTableName, out var leftTs) || leftTs is not IColumnarTableSource leftCol)
            throw new SqlCompileException($"Table '{join.LeftTableName}' does not exist in the catalog or is not a columnar table.");
        if (!catalog.TryGetTable(join.RightTableName, out var rightTs) || rightTs is not IColumnarTableSource rightCol)
            throw new SqlCompileException($"Table '{join.RightTableName}' does not exist in the catalog or is not a columnar table.");

        var probeIx = new int[join.LeftKeyColumns.Count];
        var buildIx = new int[join.RightKeyColumns.Count];
        for (var i = 0; i < join.LeftKeyColumns.Count; i++)
        {
            var lk = join.LeftKeyColumns[i];
            var rk = join.RightKeyColumns[i];
            if (!TableEq(lk.TableName, join.LeftTableName))
                throw new SqlCompileException($"JOIN key must reference table '{join.LeftTableName}' on the left side.");
            if (!TableEq(rk.TableName, join.RightTableName))
                throw new SqlCompileException($"JOIN key must reference table '{join.RightTableName}' on the right side.");

            probeIx[i] = ResolveColumn(leftTs.Schema, lk.ColumnName, leftTs.Name);
            buildIx[i] = ResolveColumn(rightTs.Schema, rk.ColumnName, rightTs.Name);

            var lt = leftTs.Schema.Columns[probeIx[i]].Type;
            var rt = rightTs.Schema.Columns[buildIx[i]].Type;
            if (lt != rt)
            {
                throw new SqlCompileException(
                    $"JOIN key type mismatch for pair {i}: left column '{leftTs.Schema.Columns[probeIx[i]].Name}' is {lt}, " +
                    $"right column '{rightTs.Schema.Columns[buildIx[i]].Name}' is {rt}. Both sides must use the same type.");
            }

            if (lt != RainDbType.Utf8 && !ColumnTypeSizes.IsFixedWidth(lt))
            {
                throw new SqlCompileException(
                    $"JOIN on column '{leftTs.Schema.Columns[probeIx[i]].Name}' uses unsupported type {lt}. " +
                    "Use fixed-width types or Utf8 for join keys.");
            }
        }

        if (join.GroupByColumns is { Count: > 0 })
            return BindGroupedJoin(join, leftCol, rightCol, leftTs, rightTs, probeIx, buildIx, algorithm, scanOptions);

        var (probeFilters, buildFilters) = ResolveJoinWhere(join.WhereConjuncts, leftTs, rightTs);
        var (outputOrder, outputSchema) = BindJoinOutputs(join.SelectProjection, leftTs, rightTs);

        var joinPlan = new JoinPhysicalPlan(
            algorithm,
            leftCol.Id,
            rightCol.Id,
            probeIx,
            buildIx,
            outputSchema,
            outputColumnOrder: outputOrder,
            probeSideFilters: probeFilters,
            buildSideFilters: buildFilters);
        if (join.OrderBy is not { Count: > 0 } && join.Limit is null)
            return joinPlan;

        var sortSpecs = join.OrderBy is { Count: > 0 } ob
            ? BuildJoinSortKeySpecs(ob, join.SelectProjection, leftTs, rightTs, outputSchema)
            : Array.Empty<SortKeyPhysicalSpec>();
        return new JoinSortTopNPhysicalPlan(joinPlan, sortSpecs, join.Limit, scanOptions);
    }

    private static GroupedJoinPhysicalPlan BindGroupedJoin(
        LogicalInnerJoin join,
        IColumnarTableSource leftCol,
        IColumnarTableSource rightCol,
        ITableSource leftTs,
        ITableSource rightTs,
        int[] probeIx,
        int[] buildIx,
        PhysicalJoinAlgorithm algorithm,
        VectorizedScanExecutionOptions scanOptions)
    {
        if (join.SelectList is not { Count: > 0 })
            throw new SqlCompileException("GROUP BY join requires a SELECT list.");
        if (join.SelectProjection is not null)
            throw new SqlCompileException("Internal error: grouped join logical plan must omit SelectProjection.");
        if (join.OrderBy is { Count: > 0 } || join.Limit is not null)
            throw new SqlCompileException("ORDER BY and LIMIT are not supported with GROUP BY on a join.");

        var (probeFilters, buildFilters) = ResolveJoinWhere(join.WhereConjuncts, leftTs, rightTs);
        var (_, outputSchema) = BindJoinOutputs(null, leftTs, rightTs);
        var joinPlan = new JoinPhysicalPlan(
            algorithm,
            leftCol.Id,
            rightCol.Id,
            probeIx,
            buildIx,
            outputSchema,
            outputColumnOrder: null,
            probeSideFilters: probeFilters,
            buildSideFilters: buildFilters);

        var leftWidth = leftTs.Schema.Columns.Count;
        var groupIndices = new int[join.GroupByColumns!.Count];
        for (var i = 0; i < join.GroupByColumns.Count; i++)
            groupIndices[i] = ResolveJoinStarOutputColumnIndex(join.GroupByColumns[i], leftTs, rightTs, leftWidth);

        var keyOrdinal = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < join.GroupByColumns.Count; i++)
            keyOrdinal[JoinGroupKey(join.GroupByColumns[i])] = i;

        var aggs = new List<AggregateSpec>();
        var slots = new List<HashAggregateOutputSlot>();
        foreach (var item in join.SelectList)
        {
            switch (item)
            {
                case LogicalColumnProjection col:
                    if (!keyOrdinal.TryGetValue(JoinGroupKey(col), out var ko))
                        throw new SqlCompileException($"Column '{ExplainCol(col)}' is not listed in GROUP BY.");
                    slots.Add(new HashAggregateOutputSlot(HashAggregateOutputColumnKind.GroupKey, ko));
                    break;
                case LogicalAggregationCall agg:
                    aggs.Add(ToAggregateSpecJoin(outputSchema, agg, leftTs, rightTs, leftWidth));
                    slots.Add(new HashAggregateOutputSlot(HashAggregateOutputColumnKind.Aggregate, aggs.Count - 1));
                    break;
                default:
                    throw new SqlCompileException("Unsupported SELECT list item.");
            }
        }

        var aggTableId = new TableId(Guid.NewGuid());
        var aggPlan = new HashAggregatePhysicalPlan(
            aggTableId,
            groupIndices,
            aggs.ToArray(),
            slots.ToArray(),
            filters: null,
            scanOptions);
        return new GroupedJoinPhysicalPlan(joinPlan, aggPlan);
    }

    private static string JoinGroupKey(LogicalColumnProjection p) =>
        $"{p.QualifierTableName}\u001f{p.ColumnName}";

    private static string ExplainCol(LogicalColumnProjection p) =>
        p.QualifierTableName is { } q ? $"{q}.{p.ColumnName}" : p.ColumnName;

    private static SortKeyPhysicalSpec[] BuildJoinSortKeySpecs(
        IReadOnlyList<LogicalSortKey> keys,
        IReadOnlyList<LogicalColumnProjection>? selectProjection,
        ITableSource leftTs,
        ITableSource rightTs,
        TableSchema joinOutputSchema)
    {
        var arr = new SortKeyPhysicalSpec[keys.Count];
        for (var i = 0; i < keys.Count; i++)
        {
            var ix = ResolveJoinOrderKeyColumnIndex(keys[i].Column, selectProjection, leftTs, rightTs);
            var t = joinOutputSchema.Columns[ix].Type;
            if (t != RainDbType.Utf8 && !ColumnTypeSizes.IsFixedWidth(t))
                throw new SqlCompileException($"ORDER BY does not support type {t} for join column '{joinOutputSchema.Columns[ix].Name}'.");
            arr[i] = new SortKeyPhysicalSpec(ix, keys[i].Descending);
        }

        return arr;
    }

    private static int ResolveJoinOrderKeyColumnIndex(
        LogicalColumnProjection key,
        IReadOnlyList<LogicalColumnProjection>? selectProjection,
        ITableSource leftTs,
        ITableSource rightTs)
    {
        if (key.QualifierTableName is null)
            throw new SqlCompileException("ORDER BY on a join requires qualified table.column.");

        if (selectProjection is { Count: > 0 } sp)
        {
            for (var i = 0; i < sp.Count; i++)
            {
                if (SameQualifiedColumn(sp[i], key))
                    return i;
            }

            throw new SqlCompileException(
                $"ORDER BY column '{ExplainCol(key)}' must appear in the SELECT list for this join shape.");
        }

        var lw = leftTs.Schema.Columns.Count;
        if (TableEq(key.QualifierTableName, leftTs.Name))
            return ResolveColumn(leftTs.Schema, key.ColumnName, leftTs.Name);
        if (TableEq(key.QualifierTableName, rightTs.Name))
            return lw + ResolveColumn(rightTs.Schema, key.ColumnName, rightTs.Name);
        throw new SqlCompileException($"ORDER BY references unknown table '{key.QualifierTableName}'.");
    }

    private static bool SameQualifiedColumn(LogicalColumnProjection a, LogicalColumnProjection b) =>
        string.Equals(a.ColumnName, b.ColumnName, StringComparison.OrdinalIgnoreCase)
        && string.Equals(a.QualifierTableName ?? "", b.QualifierTableName ?? "", StringComparison.OrdinalIgnoreCase);

    private static int ResolveJoinStarOutputColumnIndex(
        LogicalColumnProjection p,
        ITableSource leftTs,
        ITableSource rightTs,
        int leftColCount)
    {
        if (p.QualifierTableName is not { } qt)
            throw new SqlCompileException("JOIN GROUP BY requires qualified table.column.");

        if (TableEq(qt, leftTs.Name))
            return ResolveColumn(leftTs.Schema, p.ColumnName, leftTs.Name);

        if (TableEq(qt, rightTs.Name))
            return leftColCount + ResolveColumn(rightTs.Schema, p.ColumnName, rightTs.Name);

        throw new SqlCompileException($"GROUP BY references unknown table '{qt}'.");
    }

    private static int ResolveJoinAggregateColumnIndex(
        LogicalAggregationCall agg,
        ITableSource leftTs,
        ITableSource rightTs,
        int leftColCount)
    {
        var name = agg.ArgumentColumnName!;
        if (agg.ArgumentQualifierTableName is { } qt)
        {
            if (TableEq(qt, leftTs.Name))
                return ResolveColumn(leftTs.Schema, name, leftTs.Name);
            if (TableEq(qt, rightTs.Name))
                return leftColCount + ResolveColumn(rightTs.Schema, name, rightTs.Name);
            throw new SqlCompileException($"Unknown table '{qt}' in aggregate argument.");
        }

        var li = TryResolveColumn(leftTs.Schema, name);
        var ri = TryResolveColumn(rightTs.Schema, name);
        if (li >= 0 && ri >= 0)
        {
            throw new SqlCompileException(
                $"Column '{name}' in aggregate is ambiguous between '{leftTs.Name}' and '{rightTs.Name}'; use table.column.");
        }

        if (li >= 0)
            return li;
        if (ri >= 0)
            return leftColCount + ri;
        throw new SqlCompileException($"Unknown column '{name}' in aggregate.");
    }

    private static AggregateSpec ToAggregateSpecJoin(
        TableSchema joinSchema,
        LogicalAggregationCall agg,
        ITableSource leftTs,
        ITableSource rightTs,
        int leftColCount)
    {
        switch (agg.Kind)
        {
            case AggregateKind.Count when agg.ArgumentColumnName is null:
                return new AggregateSpec(-1, AggregateKind.Count);
            case AggregateKind.Count:
            {
                var ci = ResolveJoinAggregateColumnIndex(agg, leftTs, rightTs, leftColCount);
                ValidateJoinAggregate(joinSchema.Columns[ci].Type, AggregateKind.Count);
                return new AggregateSpec(ci, AggregateKind.Count);
            }
            case AggregateKind.Sum:
            case AggregateKind.Min:
            case AggregateKind.Max:
            {
                var si = ResolveJoinAggregateColumnIndex(agg, leftTs, rightTs, leftColCount);
                ValidateJoinAggregate(joinSchema.Columns[si].Type, agg.Kind);
                return new AggregateSpec(si, agg.Kind);
            }
            default:
                throw new SqlCompileException($"Aggregate {agg.Kind} is not supported.");
        }
    }

    private static void ValidateJoinAggregate(RainDbType columnType, AggregateKind kind)
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

    private static (JoinOutputColumnRef[]? order, TableSchema schema) BindJoinOutputs(
        IReadOnlyList<LogicalColumnProjection>? selectProjection,
        ITableSource left,
        ITableSource right)
    {
        if (selectProjection is null or { Count: 0 })
        {
            var starCols = new List<ColumnDef>();
            foreach (var c in left.Schema.Columns)
                starCols.Add(new ColumnDef($"{left.Name}_{c.Name}", c.Type));
            foreach (var c in right.Schema.Columns)
                starCols.Add(new ColumnDef($"{right.Name}_{c.Name}", c.Type));
            return (null, new TableSchema(starCols));
        }

        var refs = new List<JoinOutputColumnRef>();
        var cols = new List<ColumnDef>();
        foreach (var p in selectProjection)
        {
            if (p.QualifierTableName is { } qt)
            {
                if (TableEq(qt, left.Name))
                {
                    var ix = ResolveColumn(left.Schema, p.ColumnName, left.Name);
                    refs.Add(new JoinOutputColumnRef(IsProbe: true, ix));
                    cols.Add(new ColumnDef($"{left.Name}_{left.Schema.Columns[ix].Name}", left.Schema.Columns[ix].Type));
                    continue;
                }

                if (TableEq(qt, right.Name))
                {
                    var ix = ResolveColumn(right.Schema, p.ColumnName, right.Name);
                    refs.Add(new JoinOutputColumnRef(IsProbe: false, ix));
                    cols.Add(new ColumnDef($"{right.Name}_{right.Schema.Columns[ix].Name}", right.Schema.Columns[ix].Type));
                    continue;
                }

                throw new SqlCompileException(
                    $"SELECT references unknown table '{qt}' (FROM joins '{left.Name}' and '{right.Name}').");
            }

            var li = TryResolveColumn(left.Schema, p.ColumnName);
            var ri = TryResolveColumn(right.Schema, p.ColumnName);
            if (li >= 0 && ri >= 0)
            {
                throw new SqlCompileException(
                    $"SELECT column '{p.ColumnName}' is ambiguous between '{left.Name}' and '{right.Name}'; use qualified table.column.");
            }

            if (li >= 0)
            {
                refs.Add(new JoinOutputColumnRef(true, li));
                cols.Add(new ColumnDef($"{left.Name}_{left.Schema.Columns[li].Name}", left.Schema.Columns[li].Type));
                continue;
            }

            if (ri >= 0)
            {
                refs.Add(new JoinOutputColumnRef(false, ri));
                cols.Add(new ColumnDef($"{right.Name}_{right.Schema.Columns[ri].Name}", right.Schema.Columns[ri].Type));
                continue;
            }

            throw new SqlCompileException(
                $"Unknown SELECT column '{p.ColumnName}' (not found on '{left.Name}' or '{right.Name}').");
        }

        return (refs.ToArray(), new TableSchema(cols));
    }

    private static (ColumnCompareFilter[]? probe, ColumnCompareFilter[]? build) ResolveJoinWhere(
        IReadOnlyList<SimpleWhereClause>? conjuncts,
        ITableSource left,
        ITableSource right)
    {
        if (conjuncts is null or { Count: 0 })
            return (null, null);

        var probeList = new List<ColumnCompareFilter>();
        var buildList = new List<ColumnCompareFilter>();
        foreach (var w in conjuncts)
        {
            if (w.QualifierTableName is { } qt)
            {
                if (TableEq(qt, left.Name))
                    probeList.Add(LogicalTableScanBinder.BuildColumnCompareFilter(w, left.Schema, left.Name));
                else if (TableEq(qt, right.Name))
                    buildList.Add(LogicalTableScanBinder.BuildColumnCompareFilter(w, right.Schema, right.Name));
                else
                    throw new SqlCompileException(
                        $"WHERE references unknown table '{qt}' (expected '{left.Name}' or '{right.Name}').");
                continue;
            }

            var li = TryResolveColumn(left.Schema, w.ColumnName);
            var ri = TryResolveColumn(right.Schema, w.ColumnName);
            if (li >= 0 && ri >= 0)
            {
                throw new SqlCompileException(
                    $"WHERE column '{w.ColumnName}' is ambiguous between '{left.Name}' and '{right.Name}'; use qualified table.column.");
            }

            if (li >= 0)
                probeList.Add(LogicalTableScanBinder.BuildColumnCompareFilter(w, left.Schema, left.Name));
            else if (ri >= 0)
                buildList.Add(LogicalTableScanBinder.BuildColumnCompareFilter(w, right.Schema, right.Name));
            else
                throw new SqlCompileException(
                    $"Unknown column '{w.ColumnName}' in WHERE (not found on '{left.Name}' or '{right.Name}').");
        }

        return (
            probeList.Count > 0 ? probeList.ToArray() : null,
            buildList.Count > 0 ? buildList.ToArray() : null);
    }

    private static int TryResolveColumn(TableSchema schema, string name)
    {
        for (var i = 0; i < schema.Columns.Count; i++)
        {
            if (schema.Columns[i].Name.Equals(name, StringComparison.OrdinalIgnoreCase))
                return i;
        }

        return -1;
    }

    private static bool TableEq(string a, string b) => a.Equals(b, StringComparison.OrdinalIgnoreCase);

    private static int ResolveColumn(TableSchema schema, string name, string tableName)
    {
        for (var i = 0; i < schema.Columns.Count; i++)
        {
            if (schema.Columns[i].Name.Equals(name, StringComparison.OrdinalIgnoreCase))
                return i;
        }

        throw new SqlCompileException($"Unknown column '{name}' in table '{tableName}'.");
    }
}
