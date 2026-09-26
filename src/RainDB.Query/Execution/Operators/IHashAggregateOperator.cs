using RainDB.Catalog;
using RainDB.Columnar;
using RainDB.Execution;
using RainDB.Query.Plans;
using RainDB.Schema;

namespace RainDB.Query.Execution.Operators;

public interface IHashAggregateOperator
{
    ValueTask<IQueryResult> ExecuteAsync(
        HashAggregatePhysicalPlan plan,
        IColumnarTableSource table,
        IExecutionContext context);
}

/// <summary>Incremental hash-agg steps used when grouping over streamed join batches (same assembly only).</summary>
internal interface IHashAggregateGroupingSupport
{
    void ValidatePlanForInputSchema(HashAggregatePhysicalPlan plan, TableSchema schema);

    bool UsesCompositeGroupKeys(HashAggregatePhysicalPlan plan, TableSchema schema);

    Dictionary<GroupKey, AggregateAccumulator[]> AccumulateBatchForGrouped(
        IColumnarBatch batch,
        HashAggregatePhysicalPlan plan,
        CancellationToken cancellationToken);

    Dictionary<CompositeJoinKey, AggregateAccumulator[]> AccumulateBatchCompositeForGrouped(
        IColumnarBatch batch,
        HashAggregatePhysicalPlan plan,
        TableSchema schema,
        CancellationToken cancellationToken);

    void MergePartialIntoGlobal(
        Dictionary<GroupKey, AggregateAccumulator[]> global,
        Dictionary<GroupKey, AggregateAccumulator[]> partial,
        AggregateSpec[] specs);

    void MergePartialIntoGlobalComposite(
        Dictionary<CompositeJoinKey, AggregateAccumulator[]> global,
        Dictionary<CompositeJoinKey, AggregateAccumulator[]> partial,
        AggregateSpec[] specs);

    ValueTask<IQueryResult> MaterializeFromGlobalAsync(
        HashAggregatePhysicalPlan plan,
        TableSchema inputSchema,
        Dictionary<GroupKey, AggregateAccumulator[]> global);

    ValueTask<IQueryResult> MaterializeFromGlobalCompositeAsync(
        HashAggregatePhysicalPlan plan,
        TableSchema inputSchema,
        Dictionary<CompositeJoinKey, AggregateAccumulator[]> global);
}
