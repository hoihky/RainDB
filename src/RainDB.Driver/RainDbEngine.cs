using RainDB.Catalog;
using RainDB.Core.Catalog;
using RainDB.Core.Memory;
using RainDB.Core.Persistence;
using RainDB.Execution;
using RainDB.Linq;
using RainDB.Linq.Compilation;
using RainDB.Memory;
using RainDB.Query.Execution;
using RainDB.Query.Runtime;
using RainDB.Sql;
using RainDB.Sql.Compilation;

namespace RainDB;

/// <summary>Composition root for the embedded engine (SRP: hosts collaborators; extend via constructor injection).</summary>
public sealed class RainDbEngine
{
    public RainDbEngine(
        ICatalog catalog,
        IBufferPool bufferPool,
        IAlignedBufferPool alignedBufferPool,
        IQueryExecutor executor,
        ISqlCompiler sqlCompiler,
        ILinqCompiler linqCompiler,
        ISpillWriter? spillWriter = null,
        RainDbFileDatabase? fileDatabase = null)
    {
        Catalog = catalog;
        BufferPool = bufferPool;
        AlignedBufferPool = alignedBufferPool;
        Executor = executor;
        SqlCompiler = sqlCompiler;
        LinqCompiler = linqCompiler;
        SpillWriter = spillWriter ?? NoOpSpillWriter.Instance;
        FileDatabase = fileDatabase;
    }

    /// <summary>When the engine was opened via <see cref="OpenPersistent"/>, holds the backing store so it is not collected while the engine runs.</summary>
    public RainDbFileDatabase? FileDatabase { get; }

    public ICatalog Catalog { get; }

    public IBufferPool BufferPool { get; }

    public IAlignedBufferPool AlignedBufferPool { get; }

    public IQueryExecutor Executor { get; }

    public ISqlCompiler SqlCompiler { get; }

    public ILinqCompiler LinqCompiler { get; }

    public ISpillWriter SpillWriter { get; }

    /// <summary>Factory with in-memory catalog and hybrid pooled + aligned buffers.</summary>
    public static RainDbEngine CreateDefault() => CreateDefault(new InMemoryCatalog());

    /// <summary>Same collaborators as <see cref="CreateDefault"/> but uses the supplied catalog.</summary>
    public static RainDbEngine CreateDefault(ICatalog catalog)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        var buffers = new HybridBufferPool();
        var executor = new DefaultQueryExecutor();
        var sql = new DefaultSqlCompiler();
        var linq = new DefaultLinqCompiler();
        return new RainDbEngine(catalog, buffers, buffers, executor, sql, linq, NoOpSpillWriter.Instance);
    }

    /// <summary>
    /// Opens a directory-backed database: loads <see cref="RainDbFileDatabase.CatalogFileName"/> if present, wires new appends on tables created via <see cref="RainDbFileDatabase.CreateMemoryTable"/> to batch files under the directory.
    /// </summary>
    public static RainDbEngine OpenPersistent(string directoryPath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(directoryPath);
        var fileDb = RainDbFileDatabase.Open(directoryPath);
        return CreateDefault(fileDb.Catalog, fileDb);
    }

    private static RainDbEngine CreateDefault(ICatalog catalog, RainDbFileDatabase fileDatabase)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentNullException.ThrowIfNull(fileDatabase);
        var buffers = new HybridBufferPool();
        var executor = new DefaultQueryExecutor();
        var sql = new DefaultSqlCompiler();
        var linq = new DefaultLinqCompiler();
        return new RainDbEngine(catalog, buffers, buffers, executor, sql, linq, NoOpSpillWriter.Instance, fileDatabase);
    }

    public IExecutionContext CreateSession(CancellationToken cancellationToken = default) =>
        new RainDbExecutionContext(Catalog, BufferPool, AlignedBufferPool, SpillWriter, cancellationToken);

    public async ValueTask<IQueryResult> ExecuteSqlAsync(string sql, CancellationToken cancellationToken = default)
    {
        var ctx = CreateSession(cancellationToken);
        var plan = await SqlCompiler.CompileAsync(sql, Catalog, cancellationToken).ConfigureAwait(false);
        return await Executor.ExecuteAsync(plan, ctx).ConfigureAwait(false);
    }

    /// <summary>Execute a physical plan directly (Phase 1+ scan / OLAP operators).</summary>
    public async ValueTask<IQueryResult> ExecutePhysicalAsync(IPhysicalPlan plan, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(plan);
        var ctx = CreateSession(cancellationToken);
        return await Executor.ExecuteAsync(plan, ctx).ConfigureAwait(false);
    }
}
