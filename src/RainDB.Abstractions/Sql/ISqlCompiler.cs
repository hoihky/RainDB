using RainDB.Catalog;
using RainDB.Execution;

namespace RainDB.Sql;

/// <summary>Maps SQL text to a physical plan using the catalog for name resolution (strict subset).</summary>
public interface ISqlCompiler
{
    /// <exception cref="SqlCompileException">When text is not supported or invalid.</exception>
    ValueTask<IPhysicalPlan> CompileAsync(string sql, ICatalog catalog, CancellationToken cancellationToken = default);

    /// <summary>Parse and optimize once; compile to physical per execution with bound parameters.</summary>
    ValueTask<IPreparedSqlStatement> PrepareAsync(string sql, ICatalog catalog, CancellationToken cancellationToken = default);
}
