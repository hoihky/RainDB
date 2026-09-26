using RainDB.Catalog;
using RainDB.Execution;
using RainDB.Logical;
using RainDB.Query.Plans;
using RainDB.Sql.Parsing;

namespace RainDB.Sql.Compilation;

public sealed class DefaultSqlCompiler : ISqlCompiler
{
    private readonly VectorizedScanExecutionOptions _defaultScanOptions;
    private readonly LogicalPlanCompiler _logicalCompiler;

    public DefaultSqlCompiler(
        VectorizedScanExecutionOptions defaultScanOptions = default,
        LogicalPlanCompiler? logicalCompiler = null)
    {
        _defaultScanOptions = defaultScanOptions;
        _logicalCompiler = logicalCompiler ?? new LogicalPlanCompiler();
    }

    public ValueTask<IPhysicalPlan> CompileAsync(string sql, ICatalog catalog, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sql);
        ArgumentNullException.ThrowIfNull(catalog);
        cancellationToken.ThrowIfCancellationRequested();
        var logical = _logicalCompiler.Parse(sql);
        IPhysicalPlan plan = _logicalCompiler.CompilePhysical(logical.Root, catalog, _defaultScanOptions);
        return ValueTask.FromResult(plan);
    }
}
