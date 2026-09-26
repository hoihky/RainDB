using RainDB.Catalog;
using RainDB.Execution;
using RainDB.Query.Plans;

namespace RainDB.Query.Execution.Operators;

public interface IVectorizedScanOperator
{
    ValueTask<IQueryResult> ExecuteAsync(
        VectorizedScanPhysicalPlan plan,
        IColumnarTableSource table,
        IExecutionContext context);
}
