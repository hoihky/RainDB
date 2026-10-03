using RainDB.Catalog;
using RainDB.Execution;
using RainDB.Logical;
using RainDB.Query.Plans;
using RainDB.Sql.Optimization;
using RainDB.Sql.Planning;
using RainDB.Sql.Preparation;

namespace RainDB.Sql.Compilation;

/// <summary>Parse → optimize → physical bind pipeline (facade over collaborators).</summary>
public sealed class SqlCompilationService
{
    private readonly LogicalPlanCompiler _physicalCompiler;
    private readonly LogicalRewritePipeline _optimizer;
    private readonly IJoinAlgorithmSelector _joinSelector;
    private readonly LogicalParameterBinder _parameterBinder;
    private readonly SqlExplainFormatter _explainFormatter;
    private readonly PhysicalPlanningOptions _planningOptions;
    private readonly VectorizedScanExecutionOptions _scanOptions;

    public SqlCompilationService(
        LogicalPlanCompiler? physicalCompiler = null,
        LogicalRewritePipeline? optimizer = null,
        IJoinAlgorithmSelector? joinSelector = null,
        LogicalParameterBinder? parameterBinder = null,
        SqlExplainFormatter? explainFormatter = null,
        PhysicalPlanningOptions? planningOptions = null,
        VectorizedScanExecutionOptions scanOptions = default)
    {
        _physicalCompiler = physicalCompiler ?? new LogicalPlanCompiler();
        _optimizer = optimizer ?? new LogicalRewritePipeline();
        _joinSelector = joinSelector ?? new HeuristicJoinAlgorithmSelector();
        _parameterBinder = parameterBinder ?? new LogicalParameterBinder();
        _explainFormatter = explainFormatter ?? new SqlExplainFormatter();
        _planningOptions = planningOptions ?? new PhysicalPlanningOptions();
        _scanOptions = scanOptions;
    }

    public LogicalPlanCompiler PhysicalCompiler => _physicalCompiler;

    public LogicalParameterBinder ParameterBinder => _parameterBinder;

    public LogicalPlan Optimize(LogicalPlan parsed) =>
        _optimizer.Optimize(new LogicalPlan(parsed.Root, explainLevel: null));

    public IPhysicalPlan CompilePhysical(LogicalPlan logical, ICatalog catalog)
    {
        ArgumentNullException.ThrowIfNull(logical);
        ArgumentNullException.ThrowIfNull(catalog);

        var optimized = Optimize(logical);
        var executable = BindPhysical(optimized, catalog);

        if (logical.ExplainLevel is not { } explainLevel)
            return executable;

        var logicalText = _explainFormatter.FormatLogical(optimized);
        var physicalText = _explainFormatter.FormatPhysical(executable);
        return new ExplainBundlePhysicalPlan(logicalText, physicalText, explainLevel);
    }

    public IPhysicalPlan CompileBoundLogical(LogicalPlan boundLogical, ICatalog catalog)
    {
        ArgumentNullException.ThrowIfNull(boundLogical);
        ArgumentNullException.ThrowIfNull(catalog);
        return BindPhysical(boundLogical, catalog);
    }

    public bool ContainsParameters(LogicalPlan plan) =>
        _parameterBinder.CollectParameterNames(plan.Root).Count > 0;

    private IPhysicalPlan BindPhysical(LogicalPlan optimized, ICatalog catalog)
    {
        var joinAlgorithm = optimized.Root is LogicalInnerJoin join
            ? _joinSelector.Select(join, catalog, _planningOptions)
            : PhysicalJoinAlgorithm.Hash;
        return _physicalCompiler.CompilePhysical(optimized.Root, catalog, _scanOptions, joinAlgorithm);
    }
}
