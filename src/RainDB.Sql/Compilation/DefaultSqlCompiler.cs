using RainDB.Catalog;
using RainDB.Execution;
using RainDB.Logical;
using RainDB.Query.Plans;
using RainDB.Sql.Parsing;

namespace RainDB.Sql.Compilation;

public sealed class DefaultSqlCompiler : ISqlCompiler
{
    private readonly VectorizedScanExecutionOptions _defaultScanOptions;

    public DefaultSqlCompiler(VectorizedScanExecutionOptions defaultScanOptions = default) =>
        _defaultScanOptions = defaultScanOptions;

    public ValueTask<IPhysicalPlan> CompileAsync(string sql, ICatalog catalog, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sql);
        ArgumentNullException.ThrowIfNull(catalog);
        cancellationToken.ThrowIfCancellationRequested();
        var logical = SqlParser.Parse(sql);
        IPhysicalPlan plan = logical.Root switch
        {
            LogicalTableScan s => LogicalTableScanBinder.BindAndLower(s, catalog, _defaultScanOptions),
            LogicalInnerJoin j => LogicalJoinBinder.BindAndLower(j, catalog, PhysicalJoinAlgorithm.Hash, _defaultScanOptions),
            _ => throw new InvalidOperationException($"Unsupported logical root {logical.Root.GetType().Name}."),
        };
        return ValueTask.FromResult(plan);
    }
}
