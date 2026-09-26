using System.Linq.Expressions;
using RainDB.Catalog;
using RainDB.Core.Catalog;
using RainDB.Core.Memory;
using RainDB.Core.Tables;
using RainDB.Execution;
using RainDB.Linq.Compilation;
using RainDB.Query.Execution;
using RainDB.Query.Plans;
using RainDB.Query.Runtime;
using RainDB.Schema;

namespace RainDB.Tests;

public class DefaultQueryExecutorTests
{
    [Fact]
    public async Task Execute_throws_when_table_missing_from_catalog()
    {
        var executor = new DefaultQueryExecutor();
        var catalog = new InMemoryCatalog();
        var buffers = new HybridBufferPool();
        var ctx = new RainDbExecutionContext(catalog, buffers, buffers, NoOpSpillWriter.Instance, CancellationToken.None);
        var plan = new VectorizedScanPhysicalPlan(
            TableId.New(),
            outputColumnIndices: [0],
            filters: null,
            aggregate: null,
            options: new VectorizedScanExecutionOptions());

        await Assert.ThrowsAsync<InvalidOperationException>(() => executor.ExecuteAsync(plan, ctx).AsTask());
    }

    [Fact]
    public async Task Execute_throws_for_explain_only_linq_stub_plan()
    {
        var compiler = new DefaultLinqCompiler();
        var engine = TestDataBuilders.CreateEngine();
        var plan = await compiler.CompileAsync(Expression.Constant(1));
        var ctx = engine.CreateSession();

        var ex = await Assert.ThrowsAsync<NotSupportedException>(() =>
            new DefaultQueryExecutor().ExecuteAsync(plan, ctx).AsTask());
        Assert.Contains("LINQ", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Execute_throws_for_unknown_physical_plan_type()
    {
        var engine = TestDataBuilders.CreateEngine();
        var ctx = engine.CreateSession();
        var ex = await Assert.ThrowsAsync<NotSupportedException>(() =>
            new DefaultQueryExecutor().ExecuteAsync(new UnknownPlan(), ctx).AsTask());
        Assert.Contains("UnknownPlan", ex.Message, StringComparison.Ordinal);
    }

    private sealed class UnknownPlan : IPhysicalPlan
    {
        public string Explain(string indent = "") => "unknown";
    }
}
