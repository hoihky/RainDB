using RainDB.Catalog;
using RainDB.Execution;
using RainDB.Query.Plans;

namespace RainDB.Query.Execution.Operators;

public interface IGroupedJoinOperator
{
    ValueTask<IQueryResult> ExecuteAsync(
        GroupedJoinPhysicalPlan plan,
        IColumnarTableSource probeTable,
        IColumnarTableSource buildTable,
        IExecutionContext context);
}
