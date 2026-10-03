using RainDB.Catalog;
using RainDB.Execution;
using RainDB.Logical;
using RainDB.Query.Plans;
using RainDB.Schema;
using RainDB.Sql;

namespace RainDB.Sql.Compilation;

/// <summary>Binds <see cref="LogicalUnionAll"/> to physical union / distinct plans (left-associative).</summary>
public sealed class LogicalUnionAllBinder
{
    private readonly LogicalPlanCompiler _compiler;

    public LogicalUnionAllBinder(LogicalPlanCompiler compiler) =>
        _compiler = compiler ?? throw new ArgumentNullException(nameof(compiler));

    public IPhysicalPlan BindAndLower(
        LogicalUnionAll union,
        ICatalog catalog,
        VectorizedScanExecutionOptions scanOptions = default,
        PhysicalJoinAlgorithm joinAlgorithm = PhysicalJoinAlgorithm.Hash)
    {
        ArgumentNullException.ThrowIfNull(union);
        ArgumentNullException.ThrowIfNull(catalog);
        if (union.Branches.Count < 2)
            throw new SqlCompileException("UNION requires at least two SELECT statements.");

        TableSchema? schema = null;
        IPhysicalPlan? acc = null;
        for (var i = 0; i < union.Branches.Count; i++)
        {
            var physical = _compiler.CompilePhysical(union.Branches[i], catalog, scanOptions, joinAlgorithm);
            var branchSchema = PhysicalPlanOutputSchema.Resolve(physical, catalog);
            if (schema is null)
                schema = branchSchema;
            else
                PhysicalPlanOutputSchema.AssertCompatible(schema, branchSchema);

            if (acc is null)
            {
                acc = physical;
                continue;
            }

            var distinct = ResolveDistinctBetween(union, i - 1);
            var concat = new UnionAllPhysicalPlan([acc, physical], schema!);
            acc = distinct ? new DistinctPhysicalPlan(concat, schema!) : concat;
        }

        return acc!;
    }

    private static bool ResolveDistinctBetween(LogicalUnionAll union, int betweenIndex)
    {
        if (union.DistinctBetweenBranches is { Count: > 0 } flags)
        {
            if (betweenIndex < 0 || betweenIndex >= flags.Count)
                throw new SqlCompileException("UNION branch operator metadata is inconsistent.");
            return flags[betweenIndex];
        }

        return !union.UnionAll;
    }
}
