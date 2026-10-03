using RainDB.Catalog;
using RainDB.Columnar;
using RainDB.Core.Catalog;
using RainDB.Execution;
using RainDB.Query.Runtime;
using RainDB.Query.Execution.Operators;
using RainDB.Query.Plans;
using RainDB.Query.Results;

namespace RainDB.Query.Execution;

/// <summary>Default executor: dispatches physical plans to injected OLAP operators.</summary>
public sealed class DefaultQueryExecutor : IQueryExecutor
{
    private readonly IQueryOperatorSuite _operators;

    public DefaultQueryExecutor(IQueryOperatorSuite? operators = null) =>
        _operators = operators ?? new DefaultQueryOperatorSuite();

    public IQueryOperatorSuite Operators => _operators;

    public async ValueTask<IQueryResult> ExecuteAsync(IPhysicalPlan plan, IExecutionContext context)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(context.AlignedBufferPool);
        _ = plan.Explain();

        if (plan is VectorizedScanPhysicalPlan vs)
        {
            var cols = RequireColumnarTable(context, vs.TableId);
            return await _operators.Scan.ExecuteAsync(vs, cols, context).ConfigureAwait(false);
        }

        if (plan is HashAggregatePhysicalPlan ha)
        {
            var cols = RequireColumnarTable(context, ha.TableId);
            return await _operators.HashAggregate.ExecuteAsync(ha, cols, context).ConfigureAwait(false);
        }

        if (plan is JoinPhysicalPlan join)
        {
            var probeCols = RequireColumnarTable(context, join.ProbeTableId);
            var buildCols = RequireColumnarTable(context, join.BuildTableId);
            return await _operators.Join.ExecuteAsync(join, probeCols, buildCols, context).ConfigureAwait(false);
        }

        if (plan is SortTopNPhysicalPlan st)
        {
            var cols = RequireColumnarTable(context, st.TableId);
            return await _operators.SortTopN.ExecuteTableAsync(st, cols, context).ConfigureAwait(false);
        }

        if (plan is JoinSortTopNPhysicalPlan jst)
        {
            var probeCols = RequireColumnarTable(context, jst.Join.ProbeTableId);
            var buildCols = RequireColumnarTable(context, jst.Join.BuildTableId);
            return await _operators.SortTopN.ExecuteJoinAsync(jst, probeCols, buildCols, context).ConfigureAwait(false);
        }

        if (plan is GroupedJoinPhysicalPlan grouped)
        {
            var probeCols = RequireColumnarTable(context, grouped.Join.ProbeTableId);
            var buildCols = RequireColumnarTable(context, grouped.Join.BuildTableId);
            return await _operators.GroupedJoin.ExecuteAsync(grouped, probeCols, buildCols, context).ConfigureAwait(false);
        }

        if (plan is DerivedTableScanPhysicalPlan derived)
            return await ExecuteDerivedTableAsync(derived, context).ConfigureAwait(false);

        if (plan is DistinctPhysicalPlan distinct)
            return await _operators.Distinct.ExecuteAsync(distinct, this, context).ConfigureAwait(false);

        if (plan is GroupedSortTopNPhysicalPlan groupedSort)
            return await ExecuteGroupedSortTopNAsync(groupedSort, context).ConfigureAwait(false);

        if (plan is UnionAllPhysicalPlan union)
        {
            var batches = new List<IColumnarBatch>();
            foreach (var input in union.Inputs)
            {
                var child = await ExecuteAsync(input, context).ConfigureAwait(false);
                if (child is IColumnarQueryResult col)
                {
                    foreach (var b in col.Batches)
                        batches.Add(b);
                }
                else
                    throw new NotSupportedException("UNION ALL branches must return columnar row sets.");
            }

            return new ColumnarMaterializedQueryResult(batches);
        }

        if (plan is ExplainOnlyPhysicalPlan explain)
            throw new NotSupportedException($"Physical plan '{explain.Label}' cannot be executed. Use EXPLAIN-style APIs or implement the operator.");

        if (plan is ExplainBundlePhysicalPlan bundle)
            return new ExplainTextQueryResult(bundle.Explain());

        throw new NotSupportedException($"Unsupported physical plan type: {plan.GetType().Name}.");
    }

    private async ValueTask<IQueryResult> ExecuteDerivedTableAsync(
        DerivedTableScanPhysicalPlan plan,
        IExecutionContext context)
    {
        var inner = await ExecuteAsync(plan.Subquery, context).ConfigureAwait(false);
        if (inner is not IColumnarQueryResult col)
            throw new NotSupportedException("Derived table subquery must return columnar rows.");
        var batches = new List<IColumnarBatch>();
        foreach (var b in col.Batches)
            batches.Add(b);
        var ephemeral = new EphemeralColumnarTableSource(
            plan.EphemeralTableId,
            plan.Alias,
            plan.DerivedSchema,
            batches);
        var overlay = new OverlayCatalog(context.Catalog, [ephemeral]);
        var scoped = context is RainDbExecutionContext rc
            ? new RainDbExecutionContext(overlay, rc.BufferPool, rc.AlignedBufferPool, rc.SpillWriter, rc.CancellationToken, rc.MappedBatchScanObserver)
            {
                NestedExecutor = rc.NestedExecutor ?? this,
            }
            : throw new InvalidOperationException("Derived table execution requires RainDbExecutionContext.");
        return await ExecuteAsync(plan.OuterPlan, scoped).ConfigureAwait(false);
    }

    private async ValueTask<IQueryResult> ExecuteGroupedSortTopNAsync(
        GroupedSortTopNPhysicalPlan plan,
        IExecutionContext context)
    {
        var aggRes = await ExecuteAsync(plan.Aggregate, context).ConfigureAwait(false);
        if (aggRes is not IColumnarQueryResult col)
            throw new InvalidOperationException("Grouped sort input must be columnar.");
        var batchList = new List<IColumnarBatch>();
        foreach (var b in col.Batches)
            batchList.Add(b);
        var ephemeralId = new TableId(Guid.NewGuid());
        var ephemeral = new EphemeralColumnarTableSource(ephemeralId, "grouped", plan.OutputSchema, batchList);
        if (context is not RainDbExecutionContext rc)
            throw new InvalidOperationException("Grouped sort requires RainDbExecutionContext.");
        var overlay = new OverlayCatalog(context.Catalog, [ephemeral]);
        var scoped = new RainDbExecutionContext(overlay, rc.BufferPool, rc.AlignedBufferPool, rc.SpillWriter, rc.CancellationToken, rc.MappedBatchScanObserver)
        {
            NestedExecutor = rc.NestedExecutor ?? this,
        };
        var outIx = new int[plan.OutputSchema.Columns.Count];
        for (var i = 0; i < outIx.Length; i++)
            outIx[i] = i;
        var sortPlan = new SortTopNPhysicalPlan(ephemeralId, outIx, null, plan.SortKeys, plan.Limit, plan.Options);
        return await _operators.SortTopN.ExecuteTableAsync(sortPlan, ephemeral, scoped).ConfigureAwait(false);
    }

    private static IColumnarTableSource RequireColumnarTable(IExecutionContext context, TableId tableId)
    {
        if (!context.Catalog.TryGetTable(tableId, out var ts) || ts is not IColumnarTableSource cols)
            throw new InvalidOperationException($"Columnar table {tableId} was not found in the catalog.");
        return cols;
    }
}
