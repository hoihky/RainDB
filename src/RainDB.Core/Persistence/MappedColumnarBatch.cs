using RainDB.Columnar;

namespace RainDB.Core.Persistence;

/// <summary>Owns mmap resources backing a hydrated <see cref="IColumnarBatch"/>.</summary>
public sealed class MappedColumnarBatch : IDisposable
{
    public MappedColumnarBatch(IColumnarBatch batch, IDisposable mmapLifetime)
    {
        Batch = batch ?? throw new ArgumentNullException(nameof(batch));
        _mmapLifetime = mmapLifetime ?? throw new ArgumentNullException(nameof(mmapLifetime));
    }

    public IColumnarBatch Batch { get; }

    private readonly IDisposable _mmapLifetime;

    public void Dispose() => _mmapLifetime.Dispose();
}
