using RainDB.Catalog;
using RainDB.Core.Columnar;
using RainDB.Execution;
using RainDB.Query.Execution.Joining;
using RainDB.Query.Plans;

namespace RainDB.Query.Execution.Operators;

public interface IJoinOperator
{
    ValueTask<IQueryResult> ExecuteAsync(
        JoinPhysicalPlan plan,
        IColumnarTableSource probeTable,
        IColumnarTableSource buildTable,
        IExecutionContext context);

    void ExecuteStreaming(
        JoinPhysicalPlan plan,
        IColumnarTableSource probeTable,
        IColumnarTableSource buildTable,
        IExecutionContext context,
        Action<ColumnarBatch> emitBatch,
        int matchChunkRowCount = JoinMatchChunkEmitter.DefaultChunkRowCount);
}
