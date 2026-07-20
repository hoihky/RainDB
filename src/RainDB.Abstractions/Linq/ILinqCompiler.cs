using System.Linq.Expressions;
using RainDB.Execution;

namespace RainDB.Linq;

/// <summary>Maps expression trees to the same physical plan IR as SQL (DRY across front-ends).</summary>
public interface ILinqCompiler
{
    /// <exception cref="LinqCompileException">When expression shape is unsupported.</exception>
    ValueTask<IPhysicalPlan> CompileAsync(Expression expression, CancellationToken cancellationToken = default);
}
