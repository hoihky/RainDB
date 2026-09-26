using RainDB.Catalog;
using RainDB.Execution;
using RainDB.Logical;
using RainDB.Query.Plans;
using RainDB.Sql.Compilation;

namespace RainDB.Sql;

/// <summary>Entry points for the strict SQL subset (parse → logical → physical without <see cref="ISqlCompiler"/>).</summary>
public sealed class StrictSqlSubset
{
    public static StrictSqlSubset Shared { get; } = new();

    private readonly LogicalPlanCompiler _compiler = new();

    public LogicalPlanCompiler Compiler => _compiler;

    public LogicalPlan Parse(string sql) => _compiler.Parse(sql);

    public IPhysicalPlan CompilePhysical(
        string sql,
        ICatalog catalog,
        VectorizedScanExecutionOptions scanOptions = default) =>
        _compiler.CompilePhysical(Parse(sql).Root, catalog, scanOptions);

    /// <summary>Parse SQL into a <see cref="LogicalPlan"/> (table scan or inner join root).</summary>
    public static LogicalPlan ParseLogicalPlan(string sql) => Shared.Parse(sql);

    /// <summary>Parse and bind to a physical plan using <paramref name="catalog"/>.</summary>
    public static IPhysicalPlan CompilePhysicalPlan(
        string sql,
        ICatalog catalog,
        VectorizedScanExecutionOptions scanOptions = default) =>
        Shared.CompilePhysical(sql, catalog, scanOptions);
}
