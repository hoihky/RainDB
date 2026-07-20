using System.Buffers;

internal sealed class PooledAlignedMemoryOwner : IMemoryOwner<byte>
{
    private byte[]? _rented;
    private readonly int _start;
    private readonly int _length;
    private readonly ArrayPool<byte> _pool;

    public PooledAlignedMemoryOwner(byte[] rented, int start, int length, ArrayPool<byte> pool)
    {
        _rented = rented;
        _start = start;
        _length = length;
        _pool = pool;
    }

    public Memory<byte> Memory
    {
        get
        {
            var r = _rented;
            if (r is null)
                throw new ObjectDisposedException(nameof(PooledAlignedMemoryOwner));
            return new Memory<byte>(r, _start, _length);
        }
    }

    public void Dispose()
    {
        var r = Interlocked.Exchange(ref _rented, null);
        if (r is not null)
            _pool.Return(r);
    }
}
