using RainDB.Catalog;
using RainDB.Columnar;
using RainDB.Execution;
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

    private static IColumnarTableSource RequireColumnarTable(IExecutionContext context, TableId tableId)
    {
        if (!context.Catalog.TryGetTable(tableId, out var ts) || ts is not IColumnarTableSource cols)
            throw new InvalidOperationException($"Columnar table {tableId} was not found in the catalog.");
        return cols;
    }
}
