using RainDB.Catalog;
using RainDB.Execution;
using RainDB.Memory;

namespace RainDB.Query.Runtime;

public sealed class RainDbExecutionContext : IExecutionContext
{
    public RainDbExecutionContext(
        ICatalog catalog,
        IBufferPool bufferPool,
        IAlignedBufferPool alignedBufferPool,
        ISpillWriter spillWriter,
        CancellationToken cancellationToken = default)
    {
        Catalog = catalog;
        BufferPool = bufferPool;
        AlignedBufferPool = alignedBufferPool;
        SpillWriter = spillWriter ?? NoOpSpillWriter.Instance;
        CancellationToken = cancellationToken;
    }

    public ICatalog Catalog { get; }

    public IBufferPool BufferPool { get; }

    public IAlignedBufferPool AlignedBufferPool { get; }

    public ISpillWriter SpillWriter { get; }

    public CancellationToken CancellationToken { get; }
}
