using RainDB.Catalog;
using RainDB.Columnar;
using RainDB.Execution;
using RainDB.Query.Plans;
using RainDB.Query.Results;
using RainDB.Schema;

namespace RainDB.Query.Execution;

/// <summary>Pipelines inner join probe batches directly into hash aggregation (no full join rowset).</summary>
public static class GroupedJoinExecutionEngine
{
    public static ValueTask<IQueryResult> ExecuteAsync(
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
        HashAggregateEngine.ValidatePlanForInputSchema(plan.Aggregate, joinSchema);

        return HashAggregateEngine.UsesCompositeGroupKeys(plan.Aggregate, joinSchema)
            ? ExecuteCompositeAsync(plan, probeTable, buildTable, context)
            : ExecuteFixedWidthAsync(plan, probeTable, buildTable, context);
    }

    private static ValueTask<IQueryResult> ExecuteFixedWidthAsync(
        GroupedJoinPhysicalPlan plan,
        IColumnarTableSource probeTable,
        IColumnarTableSource buildTable,
        IExecutionContext context)
    {
        var global = new Dictionary<GroupKey, AggregateAccumulator[]>();
        var agg = plan.Aggregate;
        var schema = plan.Join.OutputSchema;
        var ct = context.CancellationToken;

        JoinExecutionEngine.ExecuteStreaming(
            plan.Join,
            probeTable,
            buildTable,
            context,
            joinBatch =>
            {
                if (joinBatch.RowCount == 0)
                    return;
                var partial = HashAggregateEngine.AccumulateBatchForGrouped(joinBatch, agg, ct);
                HashAggregateEngine.MergePartialIntoGlobal(global, partial, agg.Aggregates);
            });

        return HashAggregateEngine.MaterializeFromGlobalAsync(agg, schema, global);
    }

    private static ValueTask<IQueryResult> ExecuteCompositeAsync(
        GroupedJoinPhysicalPlan plan,
        IColumnarTableSource probeTable,
        IColumnarTableSource buildTable,
        IExecutionContext context)
    {
        var global = new Dictionary<CompositeJoinKey, AggregateAccumulator[]>();
        var agg = plan.Aggregate;
        var schema = plan.Join.OutputSchema;
        var ct = context.CancellationToken;

        JoinExecutionEngine.ExecuteStreaming(
            plan.Join,
            probeTable,
            buildTable,
            context,
            joinBatch =>
            {
                if (joinBatch.RowCount == 0)
                    return;
                var partial = HashAggregateEngine.AccumulateBatchCompositeForGrouped(joinBatch, agg, schema, ct);
                HashAggregateEngine.MergePartialIntoGlobalComposite(global, partial, agg.Aggregates);
            });

        return HashAggregateEngine.MaterializeFromGlobalCompositeAsync(agg, schema, global);
    }
}
