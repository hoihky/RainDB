using System.Buffers;
using System.IO;
using RainDB.Catalog;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Execution;
using RainDB.Query.Execution.Operators;
using RainDB.Query.Execution.Sorting;
using RainDB.Query.Plans;
using RainDB.Query.Results;
using RainDB.Query.Runtime;
using RainDB.Query.Vectorized;
using RainDB.Schema;

namespace RainDB.Query.Execution;

/// <summary>In-memory sort and/or LIMIT over columnar batches (single-table or pre-materialized join output).</summary>
public sealed class SortTopNOperator : Operators.ISortTopNOperator
{
    private readonly Operators.IJoinOperator _join;
    private readonly QueryOperatorDependencies _deps;

    public SortTopNOperator(Operators.IJoinOperator join)
        : this(join, new QueryOperatorDependencies())
    {
    }

    internal SortTopNOperator(Operators.IJoinOperator join, QueryOperatorDependencies dependencies)
    {
        _join = join ?? throw new ArgumentNullException(nameof(join));
        _deps = dependencies ?? throw new ArgumentNullException(nameof(dependencies));
    }

    public async ValueTask<IQueryResult> ExecuteTableAsync(
        SortTopNPhysicalPlan plan,
        IColumnarTableSource table,
        IExecutionContext context)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(context);
        if (plan.TableId != table.Id)
            throw new ArgumentException("SortTopN plan table id does not match source.", nameof(table));
        ValidateSortKeys(table.Schema, plan.SortKeys);
        ValidateOutputAndFilters(table.Schema, plan.OutputColumnIndices, plan.Filters);

        var subFilters = await ResolveSubqueryFiltersAsync(plan.InSubqueries, plan.ExistsSubqueries, context)
            .ConfigureAwait(false);
        if (subFilters.IsDenyAll)
            return new ColumnarMaterializedQueryResult(Array.Empty<IColumnarBatch>());

        var batches = table.Batches;
        var ct = context.CancellationToken;
        RowLocation[] rows;
        if (subFilters.CorrelatedExists is { Length: > 0 } || subFilters.CorrelatedIn is { Length: > 0 })
        {
            if (context is not RainDbExecutionContext { NestedExecutor: { } nested })
                throw new InvalidOperationException("Correlated subqueries require NestedExecutor.");
            rows = await CollectFilteredRowsCorrelatedAsync(
                batches,
                plan.Filters,
                subFilters,
                nested,
                context,
                ct).ConfigureAwait(false);
        }
        else
        {
            rows = CollectFilteredRows(batches, plan.Filters, subFilters, ct);
        }
        var ordered = _deps.SortTopNSelection.SelectInSortOrder(
            rows,
            plan.SortKeys,
            plan.Limit,
            table.Schema,
            batches);
        var batch = MaterializeRows(batches, table.Schema, ordered, plan.OutputColumnIndices);
        return new ColumnarMaterializedQueryResult([batch]);
    }

    private static async ValueTask<ResolvedSubqueryFilters> ResolveSubqueryFiltersAsync(
        SubqueryInPhysicalSpec[]? inSpecs,
        SubqueryExistsPhysicalSpec[]? existsSpecs,
        IExecutionContext context)
    {
        if (inSpecs is null && existsSpecs is null)
            return ResolvedSubqueryFilters.Empty;
        if (context is not RainDbExecutionContext { NestedExecutor: { } executor })
            throw new InvalidOperationException("Subquery predicates require NestedExecutor on the execution context.");
        return await SubqueryFilterResolver.ResolveAsync(inSpecs, existsSpecs, executor, context).ConfigureAwait(false);
    }

    public async ValueTask<IQueryResult> ExecuteJoinAsync(
        JoinSortTopNPhysicalPlan plan,
        IColumnarTableSource probeTable,
        IColumnarTableSource buildTable,
        IExecutionContext context)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(probeTable);
        ArgumentNullException.ThrowIfNull(buildTable);
        ArgumentNullException.ThrowIfNull(context);
        var joinRes = await _join.ExecuteAsync(plan.Join, probeTable, buildTable, context).ConfigureAwait(false);
        if (joinRes is not IColumnarQueryResult col)
            throw new InvalidOperationException("Join must return columnar result.");
        var batches = col.Batches;
        var schema = plan.Join.OutputSchema;
        ValidateSortKeys(schema, plan.SortKeys);
        var rows = CollectAllRows(batches, context.CancellationToken);
        var ordered = _deps.SortTopNSelection.SelectInSortOrder(
            rows,
            plan.SortKeys,
            plan.Limit,
            schema,
            batches);
        var outIx = new int[schema.Columns.Count];
        for (var i = 0; i < outIx.Length; i++)
            outIx[i] = i;
        var batch = MaterializeRows(batches, schema, ordered, outIx);
        return new ColumnarMaterializedQueryResult([batch]);
    }

    private static void ValidateOutputAndFilters(TableSchema schema, int[] outputIx, ColumnCompareFilter[]? filters)
    {
        foreach (var ix in outputIx)
        {
            if ((uint)ix >= (uint)schema.Columns.Count)
                throw new ArgumentException($"Output column index {ix} is out of range.", nameof(outputIx));
        }

        if (filters is { } fa)
        {
            foreach (var f in fa)
            {
                if ((uint)f.ColumnIndex >= (uint)schema.Columns.Count)
                    throw new ArgumentException("Filter column index is out of range.", nameof(filters));
            }
        }
    }

    private static void ValidateSortKeys(TableSchema schema, SortKeyPhysicalSpec[] keys)
    {
        foreach (var k in keys)
        {
            if (k.Int32SortExpression is not null || k.Float64SortExpression is not null)
                continue;
            if ((uint)k.ColumnIndex >= (uint)schema.Columns.Count)
                throw new ArgumentException($"Sort key column index {k.ColumnIndex} is out of range.", nameof(keys));
            var t = schema.Columns[k.ColumnIndex].Type;
            if (t != RainDbType.Utf8 && !ColumnTypeSizes.IsFixedWidth(t))
                throw new NotSupportedException($"ORDER BY on type {t} is not supported.");
        }
    }

    private async ValueTask<RowLocation[]> CollectFilteredRowsCorrelatedAsync(
        IReadOnlyList<IColumnarBatch> batches,
        ColumnCompareFilter[]? filters,
        ResolvedSubqueryFilters subqueryFilters,
        IQueryExecutor executor,
        IExecutionContext context,
        CancellationToken ct)
    {
        var exists = subqueryFilters.CorrelatedExists;
        var inSpecs = subqueryFilters.CorrelatedIn;
        var list = new List<RowLocation>();
        var compare = filters ?? Array.Empty<ColumnCompareFilter>();
        var inFilters = subqueryFilters.InFilters ?? Array.Empty<ColumnInSetFilter>();
        var rent = ArrayPool<int>.Shared;
        var tmp = rent.Rent(4096);
        try
        {
            for (var bi = 0; bi < batches.Count; bi++)
            {
                ct.ThrowIfCancellationRequested();
                var batch = batches[bi];
                var cap = Math.Max(batch.RowCount, 1);
                if (tmp.Length < cap)
                {
                    rent.Return(tmp);
                    tmp = rent.Rent(cap);
                }

                int k;
                if (compare.Length > 0 || inFilters.Length > 0)
                {
                    k = _deps.Selection.FillSelectedRowsConjunctive(
                        batch,
                        compare.AsSpan(),
                        inFilters.AsSpan(),
                        tmp.AsSpan(0, batch.RowCount));
                }
                else
                {
                    k = batch.RowCount;
                    for (var r = 0; r < k; r++)
                        tmp[r] = r;
                }

                for (var i = 0; i < k; i++)
                {
                    var r = tmp[i];
                    var ok = true;
                    if (exists is { Length: > 0 })
                    {
                        foreach (var spec in exists)
                        {
                            if (!await CorrelatedSubqueryExecutor.ExistsForOuterRowAsync(spec, batch, r, executor, context)
                                    .ConfigureAwait(false))
                            {
                                ok = false;
                                break;
                            }
                        }
                    }

                    if (ok && inSpecs is { Length: > 0 })
                    {
                        foreach (var spec in inSpecs)
                        {
                            if (!await CorrelatedSubqueryExecutor.InForOuterRowAsync(
                                    spec,
                                    batch,
                                    r,
                                    executor,
                                    context,
                                    _deps.Selection).ConfigureAwait(false))
                            {
                                ok = false;
                                break;
                            }
                        }
                    }

                    if (ok)
                        list.Add(new RowLocation(bi, r));
                }
            }

            return list.ToArray();
        }
        finally
        {
            rent.Return(tmp);
        }
    }

    private RowLocation[] CollectFilteredRows(
        IReadOnlyList<IColumnarBatch> batches,
        ColumnCompareFilter[]? filters,
        ResolvedSubqueryFilters subqueryFilters,
        CancellationToken ct)
    {
        var rent = ArrayPool<int>.Shared;
        var total = 0;
        foreach (var b in batches)
            total += b.RowCount;
        var tmp = rent.Rent(Math.Max(total, 16));
        try
        {
            var list = new List<RowLocation>(total);
            var compare = filters is { Length: > 0 } ? filters.AsSpan() : ReadOnlySpan<ColumnCompareFilter>.Empty;
            var inSpan = subqueryFilters.InFilters is { } inf
                ? inf.AsSpan()
                : ReadOnlySpan<ColumnInSetFilter>.Empty;
            var hasFilters = compare.Length > 0 || inSpan.Length > 0;
            for (var bi = 0; bi < batches.Count; bi++)
            {
                ct.ThrowIfCancellationRequested();
                var batch = batches[bi];
                int k;
                if (hasFilters)
                {
                    k = _deps.Selection.FillSelectedRowsConjunctive(batch, compare, inSpan, tmp.AsSpan(0, batch.RowCount));
                    for (var i = 0; i < k; i++)
                        list.Add(new RowLocation(bi, tmp[i]));
                }
                else
                {
                    for (var r = 0; r < batch.RowCount; r++)
                        list.Add(new RowLocation(bi, r));
                }
            }

            return list.ToArray();
        }
        finally
        {
            rent.Return(tmp);
        }
    }

    private static RowLocation[] CollectAllRows(IReadOnlyList<IColumnarBatch> batches, CancellationToken ct)
    {
        var list = new List<RowLocation>();
        for (var bi = 0; bi < batches.Count; bi++)
        {
            ct.ThrowIfCancellationRequested();
            var batch = batches[bi];
            for (var r = 0; r < batch.RowCount; r++)
                list.Add(new RowLocation(bi, r));
        }

        return list.ToArray();
    }

    private ColumnarBatch MaterializeRows(
        IReadOnlyList<IColumnarBatch> batches,
        TableSchema schema,
        ReadOnlySpan<RowLocation> rows,
        ReadOnlySpan<int> outputColumnIndices)
    {
        var n = rows.Length;
        var cols = new IColumnChunk[outputColumnIndices.Length];
        for (var c = 0; c < outputColumnIndices.Length; c++)
        {
            var colIx = outputColumnIndices[c];
            var t = schema.Columns[colIx].Type;
            cols[c] = t == RainDbType.Utf8
                ? GatherUtf8Column(batches, colIx, rows)
                : GatherFixedWidthColumn(batches, colIx, t, rows);
        }

        return new ColumnarBatch(n, cols);
    }

    private IColumnChunk GatherFixedWidthColumn(
        IReadOnlyList<IColumnarBatch> batches,
        int colIx,
        RainDbType type,
        ReadOnlySpan<RowLocation> rows)
    {
        var w = ColumnTypeSizes.FixedWidthBytes(type);
        var values = new byte[checked(rows.Length * w)];
        var nbBytes = ColumnTypeSizes.NullBitmapBytes(rows.Length);
        var nb = nbBytes > 0 ? new byte[nbBytes] : Array.Empty<byte>();
        var anyNull = false;
        for (var o = 0; o < rows.Length; o++)
        {
            var loc = rows[o];
            var col = batches[loc.BatchIndex].Columns[colIx];
            var r = loc.RowIndex;
            var srcNb = col.HasNulls ? col.NullBitmap.Span : ReadOnlySpan<byte>.Empty;
            if (_deps.Selection.IsNull(srcNb, r, col.HasNulls))
            {
                anyNull = true;
                SetNull(nb, o);
                continue;
            }

            col.Values.Span.Slice(r * w, w).CopyTo(values.AsSpan(o * w, w));
        }

        return new FixedWidthColumnChunk(type, rows.Length, values, nb, anyNull);
    }

    private IColumnChunk GatherUtf8Column(IReadOnlyList<IColumnarBatch> batches, int colIx, ReadOnlySpan<RowLocation> rows)
    {
        var offsets = new int[rows.Length + 1];
        using var blob = new MemoryStream();
        var nbBytes = ColumnTypeSizes.NullBitmapBytes(rows.Length);
        var nb = nbBytes > 0 ? new byte[nbBytes] : Array.Empty<byte>();
        var anyNull = false;
        for (var o = 0; o < rows.Length; o++)
        {
            offsets[o] = (int)blob.Length;
            var loc = rows[o];
            var col = batches[loc.BatchIndex].Columns[colIx];
            var r = loc.RowIndex;
            var srcNb = col.HasNulls ? col.NullBitmap.Span : ReadOnlySpan<byte>.Empty;
            if (_deps.Selection.IsNull(srcNb, r, col.HasNulls))
            {
                anyNull = true;
                SetNull(nb, o);
                continue;
            }

            ReadOnlySpan<byte> payload = col switch
            {
                Utf8ColumnChunk u => u.Values.Span[u.Offsets.Span[r]..u.Offsets.Span[r + 1]],
                Utf8LengthPrefixedColumnChunk lp => lp.GetPayloadSpan(r),
                _ => throw new NotSupportedException($"UTF-8 chunk {col.GetType().Name}."),
            };
            blob.Write(payload);
        }

        offsets[rows.Length] = (int)blob.Length;
        return new Utf8ColumnChunk(rows.Length, offsets, blob.ToArray(), nb, anyNull);
    }

    private static void SetNull(byte[] nb, int row)
    {
        var b = row >> 3;
        nb[b] |= (byte)(1 << (row & 7));
    }
}
