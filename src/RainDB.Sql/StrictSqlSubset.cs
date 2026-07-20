using RainDB.Catalog;
using RainDB.Execution;
using RainDB.Logical;
using RainDB.Query.Plans;
using RainDB.Sql.Compilation;
using RainDB.Sql.Parsing;

namespace RainDB.Sql;

/// <summary>Entry points for the strict SQL subset (parse → logical → physical without <see cref="ISqlCompiler"/>).</summary>
public static class StrictSqlSubset
{
    /// <summary>Parse SQL into a <see cref="LogicalPlan"/> (table scan or inner join root).</summary>
    public static LogicalPlan ParseLogicalPlan(string sql) => SqlParser.Parse(sql);

    /// <summary>Parse and bind to a physical plan using <paramref name="catalog"/>.</summary>
    public static IPhysicalPlan CompilePhysicalPlan(
        string sql,
        ICatalog catalog,
        VectorizedScanExecutionOptions scanOptions = default) =>
        CompileRoot(ParseLogicalPlan(sql).Root, catalog, scanOptions);

    private static IPhysicalPlan CompileRoot(
        ILogicalRoot root,
        ICatalog catalog,
        VectorizedScanExecutionOptions scanOptions) =>
        root switch
        {
            LogicalTableScan s => LogicalTableScanBinder.BindAndLower(s, catalog, scanOptions),
            LogicalInnerJoin j => LogicalJoinBinder.BindAndLower(j, catalog, PhysicalJoinAlgorithm.Hash, scanOptions),
            _ => throw new InvalidOperationException($"Unsupported logical root {root.GetType().Name}."),
        };
}
