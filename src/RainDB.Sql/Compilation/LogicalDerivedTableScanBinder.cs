using RainDB.Catalog;
using RainDB.Execution;
using RainDB.Logical;
using RainDB.Query.Plans;
using RainDB.Schema;
using RainDB.Sql;

namespace RainDB.Sql.Compilation;

public sealed class LogicalDerivedTableScanBinder
{
    private readonly LogicalPlanCompiler _compiler;
    private readonly LogicalTableScanBinder _scanBinder;

    public LogicalDerivedTableScanBinder(LogicalPlanCompiler compiler, LogicalTableScanBinder scanBinder)
    {
        _compiler = compiler;
        _scanBinder = scanBinder;
    }

    public DerivedTableScanPhysicalPlan BindAndLower(
        LogicalDerivedTableScan scan,
        ICatalog catalog,
        VectorizedScanExecutionOptions scanOptions = default,
        PhysicalJoinAlgorithm joinAlgorithm = PhysicalJoinAlgorithm.Hash)
    {
        ArgumentNullException.ThrowIfNull(scan);
        var inner = _compiler.CompilePhysical(scan.Subquery.Root, catalog, scanOptions, joinAlgorithm);
        var schema = PhysicalPlanOutputSchema.Resolve(inner, catalog);
        var ephemeralId = new TableId(Guid.NewGuid());
        var outer = _scanBinder.BindDerivedScan(scan, ephemeralId, schema, catalog, scanOptions, joinAlgorithm);
        return new DerivedTableScanPhysicalPlan(inner, ephemeralId, scan.Alias, schema, outer);
    }
}
