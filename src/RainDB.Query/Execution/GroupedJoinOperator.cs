using RainDB.Catalog;
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

    public ValueTask<IQueryResult> ExecuteAsync(
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

        return _hashAggregate.UsesCompositeGroupKeys(plan.Aggregate, joinSchema)
            ? ExecuteCompositeAsync(plan, probeTable, buildTable, context)
            : ExecuteFixedWidthAsync(plan, probeTable, buildTable, context);
    }

    private ValueTask<IQueryResult> ExecuteFixedWidthAsync(
        GroupedJoinPhysicalPlan plan,
        IColumnarTableSource probeTable,
        IColumnarTableSource buildTable,
        IExecutionContext context)
    {
        var global = new Dictionary<GroupKey, AggregateAccumulator[]>();
        var agg = plan.Aggregate;
        var schema = plan.Join.OutputSchema;
        var ct = context.CancellationToken;

        _join.ExecuteStreaming(
            plan.Join,
            probeTable,
            buildTable,
            context,
            joinBatch =>
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
        IExecutionContext context)
    {
        var global = new Dictionary<CompositeJoinKey, AggregateAccumulator[]>();
        var agg = plan.Aggregate;
        var schema = plan.Join.OutputSchema;
        var ct = context.CancellationToken;

        _join.ExecuteStreaming(
            plan.Join,
            probeTable,
            buildTable,
            context,
            joinBatch =>
            {
                if (joinBatch.RowCount == 0)
                    return;
                var partial = _hashAggregate.AccumulateBatchCompositeForGrouped(joinBatch, agg, schema, ct);
                _hashAggregate.MergePartialIntoGlobalComposite(global, partial, agg.Aggregates);
            });

        return _hashAggregate.MaterializeFromGlobalCompositeAsync(agg, schema, global);
    }
}
