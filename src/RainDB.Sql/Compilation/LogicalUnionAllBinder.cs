using RainDB.Catalog;
using RainDB.Execution;
using RainDB.Logical;
using RainDB.Query.Plans;
using RainDB.Schema;
using RainDB.Sql;

namespace RainDB.Sql.Compilation;

/// <summary>Binds <see cref="LogicalUnionAll"/> to <see cref="UnionAllPhysicalPlan"/>.</summary>
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
            throw new SqlCompileException("UNION ALL requires at least two SELECT statements.");

        var inputs = new IPhysicalPlan[union.Branches.Count];
        TableSchema? schema = null;
        for (var i = 0; i < union.Branches.Count; i++)
        {
            var physical = _compiler.CompilePhysical(union.Branches[i], catalog, scanOptions, joinAlgorithm);
            var branchSchema = PhysicalPlanOutputSchema.Resolve(physical, catalog);
            if (schema is null)
                schema = branchSchema;
            else
                PhysicalPlanOutputSchema.AssertCompatible(schema, branchSchema);
            inputs[i] = physical;
        }

        var concat = new UnionAllPhysicalPlan(inputs, schema!);
        if (union.UnionAll)
            return concat;

        return new DistinctPhysicalPlan(concat, schema!);
    }
}
