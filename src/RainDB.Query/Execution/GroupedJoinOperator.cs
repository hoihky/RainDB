using RainDB.Catalog;
using RainDB.Core.Columnar;
using RainDB.Execution;
using RainDB.Query.Execution.Operators;
using RainDB.Query.Plans;

namespace RainDB.Query.Execution;

/// <summary>Pipelines inner join probe batches directly into hash aggregation (no full join rowset).</summary>
public sealed class GroupedJoinOperator : IGroupedJoinOperator
{
    private readonly IJoinOperator _join;
    private readonly IHashAggregateGroupingSupport _hashAggregate;

    internal GroupedJoinOperator(IJoinOperator join, IHashAggregateGroupingSupport hashAggregate)
    {
        _join = join ?? throw new ArgumentNullException(nameof(join));
        _hashAggregate = hashAggregate ?? throw new ArgumentNullException(nameof(hashAggregate));
    }

    public async ValueTask<IQueryResult> ExecuteAsync(
        GroupedJoinPhysicalPlan plan,
        IColumnarTableSource probeTable,
        IColumnarTableSource buildTable,
        IExecutionContext context)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(probeTable);
        ArgumentNullException.ThrowIfNull(buildTable);
        ArgumentNullException.ThrowIfNull(context);

        var joinSchema = plan.Join.OutputSchema;
        _hashAggregate.ValidatePlanForInputSchema(plan.Aggregate, joinSchema);

        var subFilters = await JoinOperator.ResolveJoinSubqueryFiltersAsync(plan.Join, context).ConfigureAwait(false);
        if (subFilters.IsDenyAll)
        {
            return _hashAggregate.UsesCompositeGroupKeys(plan.Aggregate, joinSchema)
                ? await _hashAggregate.MaterializeFromGlobalCompositeAsync(
                    plan.Aggregate, joinSchema, new Dictionary<CompositeJoinKey, AggregateAccumulator[]>()).ConfigureAwait(false)
                : await _hashAggregate.MaterializeFromGlobalAsync(
                    plan.Aggregate, joinSchema, new Dictionary<GroupKey, AggregateAccumulator[]>()).ConfigureAwait(false);
        }

        return _hashAggregate.UsesCompositeGroupKeys(plan.Aggregate, joinSchema)
            ? await ExecuteCompositeAsync(plan, probeTable, buildTable, context, subFilters).ConfigureAwait(false)
            : await ExecuteFixedWidthAsync(plan, probeTable, buildTable, context, subFilters).ConfigureAwait(false);
    }

    private ValueTask<IQueryResult> ExecuteFixedWidthAsync(
        GroupedJoinPhysicalPlan plan,
        IColumnarTableSource probeTable,
        IColumnarTableSource buildTable,
        IExecutionContext context,
        ResolvedJoinSubqueryFilters subFilters)
    {
        var global = new Dictionary<GroupKey, AggregateAccumulator[]>();
        var agg = plan.Aggregate;
        var schema = plan.Join.OutputSchema;
        var ct = context.CancellationToken;

        StreamJoin(plan.Join, probeTable, buildTable, context, subFilters, joinBatch =>
        {
            if (joinBatch.RowCount == 0)
                return;
            var partial = _hashAggregate.AccumulateBatchForGrouped(joinBatch, agg, ct);
            _hashAggregate.MergePartialIntoGlobal(global, partial, agg.Aggregates);
        });

        return _hashAggregate.MaterializeFromGlobalAsync(agg, schema, global);
    }

    private ValueTask<IQueryResult> ExecuteCompositeAsync(
        GroupedJoinPhysicalPlan plan,
        IColumnarTableSource probeTable,
        IColumnarTableSource buildTable,
        IExecutionContext context,
        ResolvedJoinSubqueryFilters subFilters)
    {
        var global = new Dictionary<CompositeJoinKey, AggregateAccumulator[]>();
        var agg = plan.Aggregate;
        var schema = plan.Join.OutputSchema;
        var ct = context.CancellationToken;

        StreamJoin(plan.Join, probeTable, buildTable, context, subFilters, joinBatch =>
        {
            if (joinBatch.RowCount == 0)
                return;
            var partial = _hashAggregate.AccumulateBatchCompositeForGrouped(joinBatch, agg, schema, ct);
            _hashAggregate.MergePartialIntoGlobalComposite(global, partial, agg.Aggregates);
        });

        return _hashAggregate.MaterializeFromGlobalCompositeAsync(agg, schema, global);
    }

    private void StreamJoin(
        JoinPhysicalPlan joinPlan,
        IColumnarTableSource probeTable,
        IColumnarTableSource buildTable,
        IExecutionContext context,
        ResolvedJoinSubqueryFilters subFilters,
        Action<ColumnarBatch> emitBatch)
    {
        if (_join is JoinOperator joinOp)
        {
            joinOp.ExecuteStreaming(
                joinPlan,
                probeTable,
                buildTable,
                context,
                emitBatch,
                Joining.JoinMatchChunkEmitter.DefaultChunkRowCount,
                subFilters);
            return;
        }

        _join.ExecuteStreaming(joinPlan, probeTable, buildTable, context, emitBatch);
    }
}
