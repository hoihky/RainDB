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
            if (!context.Catalog.TryGetTable(vs.TableId, out var ts) || ts is not IColumnarTableSource cols)
                throw new InvalidOperationException($"Columnar table {vs.TableId} was not found in the catalog.");
            return await VectorizedScanEngine.ExecuteAsync(vs, cols, context).ConfigureAwait(false);
        }

        if (plan is HashAggregatePhysicalPlan ha)
        {
            if (!context.Catalog.TryGetTable(ha.TableId, out var ts2) || ts2 is not IColumnarTableSource cols2)
                throw new InvalidOperationException($"Columnar table {ha.TableId} was not found in the catalog.");
            return await HashAggregateEngine.ExecuteAsync(ha, cols2, context).ConfigureAwait(false);
        }

        if (plan is JoinPhysicalPlan join)
        {
            if (!context.Catalog.TryGetTable(join.ProbeTableId, out var probeTs)
                || probeTs is not IColumnarTableSource probeCols)
                throw new InvalidOperationException($"Columnar probe table {join.ProbeTableId} was not found in the catalog.");
            if (!context.Catalog.TryGetTable(join.BuildTableId, out var buildTs)
                || buildTs is not IColumnarTableSource buildCols)
                throw new InvalidOperationException($"Columnar build table {join.BuildTableId} was not found in the catalog.");
            return await JoinExecutionEngine.ExecuteAsync(join, probeCols, buildCols, context).ConfigureAwait(false);
        }

        if (plan is SortTopNPhysicalPlan st)
        {
            if (!context.Catalog.TryGetTable(st.TableId, out var ts) || ts is not IColumnarTableSource cols)
                throw new InvalidOperationException($"Columnar table {st.TableId} was not found in the catalog.");
            return await SortTopNEngine.ExecuteTableAsync(st, cols, context).ConfigureAwait(false);
        }

        if (plan is JoinSortTopNPhysicalPlan jst)
        {
            if (!context.Catalog.TryGetTable(jst.Join.ProbeTableId, out var probeTs3)
                || probeTs3 is not IColumnarTableSource probeCols3)
                throw new InvalidOperationException($"Columnar probe table {jst.Join.ProbeTableId} was not found in the catalog.");
            if (!context.Catalog.TryGetTable(jst.Join.BuildTableId, out var buildTs3)
                || buildTs3 is not IColumnarTableSource buildCols3)
                throw new InvalidOperationException($"Columnar build table {jst.Join.BuildTableId} was not found in the catalog.");
            return await SortTopNEngine.ExecuteJoinAsync(jst, probeCols3, buildCols3, context).ConfigureAwait(false);
        }

        if (plan is GroupedJoinPhysicalPlan grouped)
        {
            if (!context.Catalog.TryGetTable(grouped.Join.ProbeTableId, out var probeTs2)
                || probeTs2 is not IColumnarTableSource probeCols2)
                throw new InvalidOperationException($"Columnar probe table {grouped.Join.ProbeTableId} was not found in the catalog.");
            if (!context.Catalog.TryGetTable(grouped.Join.BuildTableId, out var buildTs2)
                || buildTs2 is not IColumnarTableSource buildCols2)
                throw new InvalidOperationException($"Columnar build table {grouped.Join.BuildTableId} was not found in the catalog.");
            var joinResult = await JoinExecutionEngine.ExecuteAsync(grouped.Join, probeCols2, buildCols2, context).ConfigureAwait(false);
            if (joinResult is not IColumnarQueryResult colResult)
                throw new InvalidOperationException("Join execution must return a columnar result for grouped join.");
            var ephemeral = new EphemeralColumnarTableSource(
                grouped.Aggregate.TableId,
                "_grouped_join_",
                grouped.Join.OutputSchema,
                colResult.Batches);
            return await HashAggregateEngine.ExecuteAsync(grouped.Aggregate, ephemeral, context).ConfigureAwait(false);
        }

        IQueryResult r = new EmptyQueryResult(0);
        return r;
    }
}
