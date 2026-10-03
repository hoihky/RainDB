using RainDB.Catalog;
using RainDB.Sql.Parsing;
using RainDB.Execution;
using RainDB.Logical;
using RainDB.Query.Plans;

namespace RainDB.Sql.Compilation;

/// <summary>Binds logical plan roots to physical plans (composition over table-scan and join binders).</summary>
public sealed class LogicalPlanCompiler
{
    private readonly SqlParser _parser;
    private readonly LogicalTableScanBinder _tableScanBinder;
    private readonly LogicalJoinBinder _joinBinder;
    private readonly LogicalUnionAllBinder _unionBinder;
    private readonly UncorrelatedSubqueryBinder _subqueryBinder;
    private readonly LogicalDerivedTableScanBinder _derivedBinder;

    public LogicalPlanCompiler(
        SqlParser? parser = null,
        LogicalTableScanBinder? tableScanBinder = null,
        LogicalJoinBinder? joinBinder = null,
        LogicalUnionAllBinder? unionBinder = null,
        UncorrelatedSubqueryBinder? subqueryBinder = null,
        LogicalDerivedTableScanBinder? derivedBinder = null)
    {
        _parser = parser ?? new SqlParser();
        _subqueryBinder = subqueryBinder ?? new UncorrelatedSubqueryBinder(this);
        _tableScanBinder = tableScanBinder ?? new LogicalTableScanBinder(_subqueryBinder);
        _joinBinder = joinBinder ?? new LogicalJoinBinder(_tableScanBinder);
        _unionBinder = unionBinder ?? new LogicalUnionAllBinder(this);
        _derivedBinder = derivedBinder ?? new LogicalDerivedTableScanBinder(this, _tableScanBinder);
    }

    public SqlParser Parser => _parser;

    public LogicalPlan Parse(string sql) => _parser.Parse(sql);

    public LogicalTableScanBinder TableScanBinder => _tableScanBinder;

    public LogicalJoinBinder JoinBinder => _joinBinder;

    public IPhysicalPlan CompilePhysical(
        ILogicalRoot root,
        ICatalog catalog,
        VectorizedScanExecutionOptions scanOptions = default,
        PhysicalJoinAlgorithm joinAlgorithm = PhysicalJoinAlgorithm.Hash) =>
        root switch
        {
            LogicalTableScan s => _tableScanBinder.BindAndLower(s, catalog, scanOptions, joinAlgorithm),
            LogicalDerivedTableScan d => _derivedBinder.BindAndLower(d, catalog, scanOptions, joinAlgorithm),
            LogicalInnerJoin j => _joinBinder.BindAndLower(j, catalog, joinAlgorithm, scanOptions),
            LogicalUnionAll u => _unionBinder.BindAndLower(u, catalog, scanOptions, joinAlgorithm),
            _ => throw new InvalidOperationException($"Unsupported logical root {root.GetType().Name}."),
        };
}
