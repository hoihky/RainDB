using RainDB.Catalog;
using RainDB.Execution;
using RainDB.Logical;
using RainDB.Sql.Compilation;

namespace RainDB.Sql.Preparation;

/// <summary>Optimized logical template compiled to physical plans per parameter set.</summary>
public sealed class PreparedSqlStatement : IPreparedSqlStatement
{
    private readonly LogicalPlan _optimizedTemplate;
    private readonly LogicalParameterBinder _parameterBinder;
    private readonly SqlCompilationService _compilation;

    internal PreparedSqlStatement(
        string sqlText,
        LogicalPlan optimizedTemplate,
        IReadOnlyList<string> parameterNames,
        LogicalParameterBinder parameterBinder,
        SqlCompilationService compilation)
    {
        SqlText = sqlText;
        _optimizedTemplate = optimizedTemplate;
        ParameterNames = parameterNames;
        _parameterBinder = parameterBinder;
        _compilation = compilation;
    }

    public string SqlText { get; }

    public IReadOnlyList<string> ParameterNames { get; }

    public ValueTask<IPhysicalPlan> CompileAsync(
        ICatalog catalog,
        IReadOnlyDictionary<string, SqlParameterValue> parameters,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentNullException.ThrowIfNull(parameters);
        cancellationToken.ThrowIfCancellationRequested();
        var bound = _parameterBinder.BindParameters(_optimizedTemplate, parameters);
        var plan = _compilation.CompileBoundLogical(bound, catalog);
        return ValueTask.FromResult(plan);
    }

    public async ValueTask<IQueryResult> ExecuteAsync(
        ICatalog catalog,
        IQueryExecutor executor,
        IExecutionContext context,
        IReadOnlyDictionary<string, SqlParameterValue> parameters,
        CancellationToken cancellationToken = default)
    {
        var plan = await CompileAsync(catalog, parameters, cancellationToken).ConfigureAwait(false);
        return await executor.ExecuteAsync(plan, context).ConfigureAwait(false);
    }
}
