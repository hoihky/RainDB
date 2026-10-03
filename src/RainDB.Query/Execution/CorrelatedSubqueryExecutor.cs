using System.Buffers.Binary;
using RainDB.Catalog;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Execution;
using RainDB.Query.Plans;
using RainDB.Query.Vectorized;
using RainDB.Schema;

namespace RainDB.Query.Execution;

internal readonly record struct CorrelatedOuterRow(
    IColumnarBatch? SingleTable,
    int SingleRow,
    IColumnarBatch? Probe,
    int ProbeRow,
    IColumnarBatch? Build,
    int BuildRow);

internal static class CorrelatedSubqueryExecutor
{
    internal static async ValueTask<bool> InForOuterRowAsync(
        SubqueryInPhysicalSpec spec,
        IColumnarBatch outerBatch,
        int outerRow,
        IQueryExecutor executor,
        IExecutionContext context,
        SelectionEvaluator selection) =>
        await InForOuterRowAsync(spec, new CorrelatedOuterRow(outerBatch, outerRow, null, 0, null, 0), outerBatch, outerRow, executor, context, selection)
            .ConfigureAwait(false);

    internal static async ValueTask<bool> InForJoinMatchAsync(
        SubqueryInPhysicalSpec spec,
        bool inColumnOnProbe,
        IColumnarBatch probeBatch,
        int probeRow,
        IColumnarBatch buildBatch,
        int buildRow,
        IQueryExecutor executor,
        IExecutionContext context,
        SelectionEvaluator selection)
    {
        var outer = new CorrelatedOuterRow(null, 0, probeBatch, probeRow, buildBatch, buildRow);
        var inBatch = inColumnOnProbe ? probeBatch : buildBatch;
        var inRow = inColumnOnProbe ? probeRow : buildRow;
        return await InForOuterRowAsync(spec, outer, inBatch, inRow, executor, context, selection).ConfigureAwait(false);
    }

    private static async ValueTask<bool> InForOuterRowAsync(
        SubqueryInPhysicalSpec spec,
        CorrelatedOuterRow outer,
        IColumnarBatch inValueBatch,
        int inValueRow,
        IQueryExecutor executor,
        IExecutionContext context,
        SelectionEvaluator selection)
    {
        if (spec.Correlations is not { Length: > 0 })
            throw new InvalidOperationException("Correlated IN requires correlation bindings.");

        var innerPlan = spec.Subquery;
        if (innerPlan is not VectorizedScanPhysicalPlan scan)
            throw new NotSupportedException("Correlated IN inner plan must be a single-table scan.");

        var filters = BuildRuntimeFilters(outer, spec.Correlations);
        var correlatedScan = new VectorizedScanPhysicalPlan(
            scan.TableId,
            scan.OutputColumns,
            MergeFilters(scan.Filters, filters),
            scan.Aggregate,
            scan.Options,
            scan.InSubqueries,
            scan.ExistsSubqueries);

        var result = await executor.ExecuteAsync(correlatedScan, context).ConfigureAwait(false);
        if (result is not IColumnarQueryResult col || col.RowCount == 0)
            return spec.Negated;

        var set = ScalarValueSet.FromSingleColumn(col, spec.SubqueryResultColumnIndex, spec.ColumnType);
        var outerCol = inValueBatch.Columns[spec.ColumnIndex];
        var member = set.Contains(outerCol, inValueRow, selection);
        return spec.Negated ? !member : member;
    }

    internal static async ValueTask<bool> ExistsForOuterRowAsync(
        SubqueryExistsPhysicalSpec spec,
        IColumnarBatch outerBatch,
        int outerRow,
        IQueryExecutor executor,
        IExecutionContext context) =>
        await ExistsForMatchAsync(
            spec,
            new CorrelatedOuterRow(outerBatch, outerRow, null, 0, null, 0),
            executor,
            context).ConfigureAwait(false);

    internal static async ValueTask<bool> ExistsForJoinMatchAsync(
        SubqueryExistsPhysicalSpec spec,
        IColumnarBatch probeBatch,
        int probeRow,
        IColumnarBatch buildBatch,
        int buildRow,
        IQueryExecutor executor,
        IExecutionContext context) =>
        await ExistsForMatchAsync(
            spec,
            new CorrelatedOuterRow(null, 0, probeBatch, probeRow, buildBatch, buildRow),
            executor,
            context).ConfigureAwait(false);

    private static async ValueTask<bool> ExistsForMatchAsync(
        SubqueryExistsPhysicalSpec spec,
        CorrelatedOuterRow outer,
        IQueryExecutor executor,
        IExecutionContext context)
    {
        if (spec.Correlations is not { Length: > 0 } bindings)
            throw new InvalidOperationException("Correlated EXISTS requires correlation bindings.");

        if (spec.Subquery is not VectorizedScanPhysicalPlan scan)
            throw new NotSupportedException("Correlated EXISTS inner plan must be a single-table scan.");

        var filters = BuildRuntimeFilters(outer, bindings);
        var correlatedScan = new VectorizedScanPhysicalPlan(
            scan.TableId,
            scan.OutputColumns,
            MergeFilters(scan.Filters, filters),
            scan.Aggregate,
            scan.Options,
            scan.InSubqueries,
            scan.ExistsSubqueries);

        var result = await executor.ExecuteAsync(correlatedScan, context).ConfigureAwait(false);
        var any = result is IColumnarQueryResult c && c.RowCount > 0;
        return spec.Negated ? !any : any;
    }

    private static ColumnCompareFilter[] MergeFilters(ColumnCompareFilter[]? baseFilters, ColumnCompareFilter[] runtime)
    {
        if (baseFilters is null or { Length: 0 })
            return runtime;
        var merged = new ColumnCompareFilter[baseFilters.Length + runtime.Length];
        baseFilters.CopyTo(merged, 0);
        runtime.CopyTo(merged, baseFilters.Length);
        return merged;
    }

    private static ColumnCompareFilter[] BuildRuntimeFilters(
        CorrelatedOuterRow outer,
        CorrelatedEqualityBinding[] bindings)
    {
        var arr = new ColumnCompareFilter[bindings.Length];
        for (var i = 0; i < bindings.Length; i++)
        {
            var b = bindings[i];
            var bits = ReadBindingValue(outer, b);
            arr[i] = new ColumnCompareFilter(b.InnerColumnIndex, ScalarCompareOp.Eq, bits);
        }

        return arr;
    }

    private static long ReadBindingValue(CorrelatedOuterRow outer, CorrelatedEqualityBinding b)
    {
        var (batch, row) = b.OuterSource switch
        {
            CorrelatedOuterColumnSource.SingleTable => (outer.SingleTable!, outer.SingleRow),
            CorrelatedOuterColumnSource.JoinProbe => (outer.Probe!, outer.ProbeRow),
            CorrelatedOuterColumnSource.JoinBuild => (outer.Build!, outer.BuildRow),
            _ => throw new InvalidOperationException($"Unknown outer source {b.OuterSource}."),
        };
        return ReadImmediateBits(batch, row, b.OuterColumnIndex, b.CompareType);
    }

    private static long ReadImmediateBits(IColumnarBatch batch, int row, int colIx, RainDbType type)
    {
        var span = batch.Columns[colIx].Values.Span;
        return type switch
        {
            RainDbType.Int32 => BinaryPrimitives.ReadInt32LittleEndian(span.Slice(row * sizeof(int), sizeof(int))),
            RainDbType.Int64 => BinaryPrimitives.ReadInt64LittleEndian(span.Slice(row * sizeof(long), sizeof(long))),
            RainDbType.Float64 => BinaryPrimitives.ReadInt64LittleEndian(span.Slice(row * sizeof(double), sizeof(double))),
            _ => throw new NotSupportedException($"Correlated binding type {type} is not supported."),
        };
    }
}
