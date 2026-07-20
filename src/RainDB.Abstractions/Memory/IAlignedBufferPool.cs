using System.Buffers;

namespace RainDB.Memory;

/// <summary>Allocations suitable for SIMD / native interop (power-of-two alignment; default <see cref="SimdAlignment.Vector256"/>).</summary>
public interface IAlignedBufferPool
{
    /// <param name="minimumByteLength">Usable byte length (not including alignment padding).</param>
    /// <param name="alignment">Power of two, at least <see cref="SimdAlignment.Vector256"/> (32) for AVX2 loads.</param>
    IMemoryOwner<byte> RentAligned(int minimumByteLength, int alignment = SimdAlignment.Vector256);
}
