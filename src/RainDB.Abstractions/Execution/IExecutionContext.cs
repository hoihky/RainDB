using RainDB.Catalog;
using RainDB.Memory;

namespace RainDB.Execution;

/// <summary>Per-query resources and limits (OCP: add tracing, spill paths without changing operators).</summary>
public interface IExecutionContext
{
    ICatalog Catalog { get; }

    IBufferPool BufferPool { get; }

    IAlignedBufferPool AlignedBufferPool { get; }

    ISpillWriter SpillWriter { get; }

    CancellationToken CancellationToken { get; }
}
