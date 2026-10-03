using RainDB.Catalog;
using RainDB.Execution;
using RainDB.Query.Plans;
using RainDB.Sql.Preparation;

namespace RainDB.Sql.Compilation;

public sealed class DefaultSqlCompiler : ISqlCompiler
{
    private readonly SqlCompilationService _compilation;
    private readonly CatalogSchemaFingerprint _schemaFingerprint;
    private readonly CompiledSqlCache _cache;

    public DefaultSqlCompiler(
        VectorizedScanExecutionOptions defaultScanOptions = default,
        LogicalPlanCompiler? logicalCompiler = null,
        SqlCompilationService? compilation = null,
        CatalogSchemaFingerprint? schemaFingerprint = null,
        CompiledSqlCache? cache = null)
    {
        _compilation = compilation ?? new SqlCompilationService(
            physicalCompiler: logicalCompiler ?? new LogicalPlanCompiler(),
            scanOptions: defaultScanOptions);
        _schemaFingerprint = schemaFingerprint ?? new CatalogSchemaFingerprint();
        _cache = cache ?? new CompiledSqlCache();
    }

    public SqlCompilationService Compilation => _compilation;

    public CompiledSqlCache PlanCache => _cache;

    public ValueTask<IPhysicalPlan> CompileAsync(string sql, ICatalog catalog, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sql);
        ArgumentNullException.ThrowIfNull(catalog);
        cancellationToken.ThrowIfCancellationRequested();

        var parsed = _compilation.PhysicalCompiler.Parse(sql);
        var fingerprint = _schemaFingerprint.Compute(catalog);
        if (!_compilation.ContainsParameters(parsed)
            && _cache.TryGet(sql, fingerprint, out var cached)
            && cached is not null)
            return ValueTask.FromResult(cached);

        var physical = _compilation.CompilePhysical(parsed, catalog);
        if (!_compilation.ContainsParameters(parsed) && physical is not ExplainBundlePhysicalPlan)
            _cache.Store(sql, fingerprint, physical);
        return ValueTask.FromResult(physical);
    }

    public ValueTask<IPreparedSqlStatement> PrepareAsync(string sql, ICatalog catalog, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sql);
        ArgumentNullException.ThrowIfNull(catalog);
        cancellationToken.ThrowIfCancellationRequested();

        var parsed = _compilation.PhysicalCompiler.Parse(sql);
        var optimized = _compilation.Optimize(parsed);
        var names = _compilation.ParameterBinder.CollectParameterNames(optimized.Root);
        IPreparedSqlStatement prepared = new PreparedSqlStatement(
            sql,
            optimized,
            names,
            _compilation.ParameterBinder,
            _compilation);
        return ValueTask.FromResult(prepared);
    }
}
