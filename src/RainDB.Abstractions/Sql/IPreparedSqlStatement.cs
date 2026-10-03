using RainDB.Catalog;
using RainDB.Execution;

namespace RainDB.Sql;

/// <summary>Compile-once SQL with optional bound parameters (re-bind and compile per execution).</summary>
public interface IPreparedSqlStatement
{
    string SqlText { get; }

    IReadOnlyList<string> ParameterNames { get; }

    ValueTask<IPhysicalPlan> CompileAsync(
        ICatalog catalog,
        IReadOnlyDictionary<string, SqlParameterValue> parameters,
        CancellationToken cancellationToken = default);

    ValueTask<IQueryResult> ExecuteAsync(
        ICatalog catalog,
        IQueryExecutor executor,
        IExecutionContext context,
        IReadOnlyDictionary<string, SqlParameterValue> parameters,
        CancellationToken cancellationToken = default);
}
