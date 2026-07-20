using System.Linq.Expressions;
using RainDB.Execution;
using RainDB.Linq;
using RainDB.Query.Plans;

namespace RainDB.Linq.Compilation;

public sealed class DefaultLinqCompiler : ILinqCompiler
{
    public ValueTask<IPhysicalPlan> CompileAsync(Expression expression, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(expression);
        cancellationToken.ThrowIfCancellationRequested();
        IPhysicalPlan plan = new ExplainOnlyPhysicalPlan($"LINQ expression: {expression.NodeType}");
        return ValueTask.FromResult(plan);
    }
}
