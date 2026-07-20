using System.Buffers;
using RainDB.Memory;

namespace RainDB.Core.Memory;

/// <summary><see cref="ArrayPool{T}"/> adapter — zero extra allocation on hot paths.</summary>
public sealed class ArrayPoolBufferPool : IBufferPool
{
    private readonly ArrayPool<byte> _pool;

    public ArrayPoolBufferPool(ArrayPool<byte>? pool = null) => _pool = pool ?? ArrayPool<byte>.Shared;

    public byte[] Rent(int minimumLength) => _pool.Rent(minimumLength);

    public void Return(byte[] buffer, bool clearArray = false) => _pool.Return(buffer, clearArray);
}
