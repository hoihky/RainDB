using RainDB.Execution;
using RainDB.Query.Plans;

namespace RainDB.Query.Execution.Operators;

public interface IDistinctOperator
{
    ValueTask<IQueryResult> ExecuteAsync(DistinctPhysicalPlan plan, IQueryExecutor nestedExecutor, IExecutionContext context);
}
