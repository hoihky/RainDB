using RainDB.Catalog;
using RainDB.Execution;
using RainDB.Query.Plans;
using RainDB.Query.Results;

namespace RainDB.Query.Execution;

/// <summary>Default executor: vectorized scan, sort/top-N, hash aggregate, inner joins (with optional post-join sort), and grouped joins.</summary>
public sealed class DefaultQueryExecutor : IQueryExecutor
{
    public async ValueTask<IQueryResult> ExecuteAsync(IPhysicalPlan plan, IExecutionContext context)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(context.AlignedBufferPool);
        _ = plan.Explain();

        if (plan is VectorizedScanPhysicalPlan vs)
        {
            var cols = RequireColumnarTable(context, vs.TableId);
            return await VectorizedScanEngine.ExecuteAsync(vs, cols, context).ConfigureAwait(false);
        }

        if (plan is HashAggregatePhysicalPlan ha)
        {
            var cols = RequireColumnarTable(context, ha.TableId);
            return await HashAggregateEngine.ExecuteAsync(ha, cols, context).ConfigureAwait(false);
        }

        if (plan is JoinPhysicalPlan join)
        {
            var probeCols = RequireColumnarTable(context, join.ProbeTableId);
            var buildCols = RequireColumnarTable(context, join.BuildTableId);
            return await JoinExecutionEngine.ExecuteAsync(join, probeCols, buildCols, context).ConfigureAwait(false);
        }

        if (plan is SortTopNPhysicalPlan st)
        {
            var cols = RequireColumnarTable(context, st.TableId);
            return await SortTopNEngine.ExecuteTableAsync(st, cols, context).ConfigureAwait(false);
        }

        if (plan is JoinSortTopNPhysicalPlan jst)
        {
            var probeCols = RequireColumnarTable(context, jst.Join.ProbeTableId);
            var buildCols = RequireColumnarTable(context, jst.Join.BuildTableId);
            return await SortTopNEngine.ExecuteJoinAsync(jst, probeCols, buildCols, context).ConfigureAwait(false);
        }

        if (plan is GroupedJoinPhysicalPlan grouped)
        {
            var probeCols = RequireColumnarTable(context, grouped.Join.ProbeTableId);
            var buildCols = RequireColumnarTable(context, grouped.Join.BuildTableId);
            var joinResult = await JoinExecutionEngine.ExecuteAsync(grouped.Join, probeCols, buildCols, context).ConfigureAwait(false);
            try
            {
                if (joinResult is not IColumnarQueryResult colResult)
                    throw new InvalidOperationException("Join execution must return a columnar result for grouped join.");
                var ephemeral = new EphemeralColumnarTableSource(
                    grouped.Aggregate.TableId,
                    "_grouped_join_",
                    grouped.Join.OutputSchema,
                    colResult.Batches);
                return await HashAggregateEngine.ExecuteAsync(grouped.Aggregate, ephemeral, context).ConfigureAwait(false);
            }
            finally
            {
                await joinResult.DisposeAsync().ConfigureAwait(false);
            }
        }

        if (plan is ExplainOnlyPhysicalPlan explain)
            throw new NotSupportedException($"Physical plan '{explain.Label}' cannot be executed. Use EXPLAIN-style APIs or implement the operator.");

        throw new NotSupportedException($"Unsupported physical plan type: {plan.GetType().Name}.");
    }

    private static IColumnarTableSource RequireColumnarTable(IExecutionContext context, TableId tableId)
    {
        if (!context.Catalog.TryGetTable(tableId, out var ts) || ts is not IColumnarTableSource cols)
            throw new InvalidOperationException($"Columnar table {tableId} was not found in the catalog.");
        return cols;
    }
}
