using System.Buffers.Binary;
using RainDB.Catalog;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Execution;
using RainDB.Query.Plans;
using RainDB.Query.Results;
using RainDB.Logical;
using RainDB.Query.Vectorized;
using RainDB.Schema;

namespace RainDB.Query.Execution;

internal static class CorrelatedSubqueryExecutor
{
    internal static async ValueTask<bool> InForOuterRowAsync(
        SubqueryInPhysicalSpec spec,
        IColumnarBatch outerBatch,
        int outerRow,
        IQueryExecutor executor,
        IExecutionContext context,
        SelectionEvaluator selection)
    {
        if (spec.Correlations is not { Length: > 0 } bindings)
            throw new InvalidOperationException("Correlated IN requires correlation bindings.");

        var innerPlan = spec.Subquery;
        if (innerPlan is not VectorizedScanPhysicalPlan scan)
            throw new NotSupportedException("Correlated IN inner plan must be a single-table scan.");

        var filters = BuildRuntimeFilters(outerBatch, outerRow, bindings);
        var correlatedScan = new VectorizedScanPhysicalPlan(
            scan.TableId,
            scan.OutputColumns,
            MergeFilters(scan.Filters, filters),
            scan.Aggregate,
            scan.Options,
            scan.InSubqueries,
            scan.ExistsSubqueries);

        var table = RequireTable(context, scan.TableId);
        var result = await executor.ExecuteAsync(correlatedScan, context).ConfigureAwait(false);
        if (result is not IColumnarQueryResult col || col.RowCount == 0)
            return spec.Negated;

        var set = ScalarValueSet.FromSingleColumn(col, spec.SubqueryResultColumnIndex, spec.ColumnType);
        var outerCol = outerBatch.Columns[spec.ColumnIndex];
        var member = set.Contains(outerCol, outerRow, selection);
        return spec.Negated ? !member : member;
    }

    internal static async ValueTask<bool> ExistsForOuterRowAsync(
        SubqueryExistsPhysicalSpec spec,
        IColumnarBatch outerBatch,
        int outerRow,
        IQueryExecutor executor,
        IExecutionContext context)
    {
        if (spec.Correlations is not { Length: > 0 } bindings)
            throw new InvalidOperationException("Correlated EXISTS requires correlation bindings.");

        var innerPlan = spec.Subquery;
        if (innerPlan is not VectorizedScanPhysicalPlan scan)
            throw new NotSupportedException("Correlated EXISTS inner plan must be a single-table scan.");

        var filters = BuildRuntimeFilters(outerBatch, outerRow, bindings);
        var correlatedScan = new VectorizedScanPhysicalPlan(
            scan.TableId,
            scan.OutputColumns,
            MergeFilters(scan.Filters, filters),
            scan.Aggregate,
            scan.Options,
            scan.InSubqueries,
            scan.ExistsSubqueries);

        var table = RequireTable(context, scan.TableId);
        var result = await executor.ExecuteAsync(correlatedScan, context).ConfigureAwait(false);
        var any = result is IColumnarQueryResult c && c.RowCount > 0;
        return spec.Negated ? !any : any;
    }

    private static IColumnarTableSource RequireTable(IExecutionContext context, TableId id)
    {
        if (!context.Catalog.TryGetTable(id, out var ts) || ts is not IColumnarTableSource col)
            throw new InvalidOperationException($"Table {id} not found.");
        return col;
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
        IColumnarBatch outerBatch,
        int outerRow,
        CorrelatedEqualityBinding[] bindings)
    {
        var arr = new ColumnCompareFilter[bindings.Length];
        for (var i = 0; i < bindings.Length; i++)
        {
            var b = bindings[i];
            var bits = ReadImmediateBits(outerBatch, outerRow, b.OuterColumnIndex, b.CompareType);
            arr[i] = new ColumnCompareFilter(b.InnerColumnIndex, ScalarCompareOp.Eq, bits);
        }

        return arr;
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
