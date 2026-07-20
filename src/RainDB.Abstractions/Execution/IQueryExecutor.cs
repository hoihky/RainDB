namespace RainDB.Execution;

/// <summary>Runs a compiled physical plan (SRP: parsing/planning live elsewhere).</summary>
public interface IQueryExecutor
{
    ValueTask<IQueryResult> ExecuteAsync(IPhysicalPlan plan, IExecutionContext context);
}
