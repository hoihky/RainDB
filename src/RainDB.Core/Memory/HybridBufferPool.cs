using System.Buffers;
using System.Runtime.CompilerServices;
using RainDB.Memory;

namespace RainDB.Core.Memory;

/// <summary>General <see cref="ArrayPool{T}"/> rent/return plus SIMD-friendly aligned sub-allocations from the same pool.</summary>
public sealed class HybridBufferPool : IBufferPool, IAlignedBufferPool
{
    private readonly ArrayPool<byte> _pool;

    public HybridBufferPool(ArrayPool<byte>? pool = null) => _pool = pool ?? ArrayPool<byte>.Shared;

    public byte[] Rent(int minimumLength) => _pool.Rent(minimumLength);

    public void Return(byte[] buffer, bool clearArray = false) => _pool.Return(buffer, clearArray);

    /// <summary>
    /// Rents a sub-slice of a pooled array aligned to <paramref name="alignment"/> (default 32 for <see cref="SimdAlignment.Vector256"/>).
    /// Uses <see cref="ArrayPool{T}"/> so buffers stay under LOH for typical OLAP vector sizes (&lt;85KB).
    /// </summary>
    public IMemoryOwner<byte> RentAligned(int minimumByteLength, int alignment = SimdAlignment.Vector256)
    {
        if (minimumByteLength < 0)
            throw new ArgumentOutOfRangeException(nameof(minimumByteLength));
        if (alignment < SimdAlignment.Vector256 || (alignment & (alignment - 1)) != 0)
            throw new ArgumentException($"Alignment must be a power of two >= {SimdAlignment.Vector256}.", nameof(alignment));
        var pad = alignment - 1;
        var rented = _pool.Rent(checked(minimumByteLength + pad));
        ref var r0 = ref rented[0];
        unsafe
        {
            var addr = (nuint)Unsafe.AsPointer(ref r0);
            var mis = addr % (nuint)alignment;
            var off = mis == 0 ? 0 : (int)((nuint)alignment - mis);
            if (off + minimumByteLength > rented.Length)
            {
                _pool.Return(rented);
                throw new InvalidOperationException("Rented buffer could not satisfy alignment; increase padding.");
            }

            return new PooledAlignedMemoryOwner(rented, off, minimumByteLength, _pool);
        }
    }
}
