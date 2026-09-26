using RainDB.Catalog;
using RainDB.Execution;
using RainDB.Query.Plans;

namespace RainDB.Query.Execution.Operators;

public interface ISortTopNOperator
{
    ValueTask<IQueryResult> ExecuteTableAsync(
        SortTopNPhysicalPlan plan,
        IColumnarTableSource table,
        IExecutionContext context);

    ValueTask<IQueryResult> ExecuteJoinAsync(
        JoinSortTopNPhysicalPlan plan,
        IColumnarTableSource probeTable,
        IColumnarTableSource buildTable,
        IExecutionContext context);
}
