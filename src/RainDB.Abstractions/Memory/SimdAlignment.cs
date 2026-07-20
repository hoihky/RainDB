namespace RainDB.Memory;

/// <summary>Common SIMD / cache-line alignment constants for <see cref="IAlignedBufferPool"/>.</summary>
public static class SimdAlignment
{
    /// <summary><c>Vector256&lt;T&gt;</c> requires 32-byte alignment for aligned loads.</summary>
    public const int Vector256 = 32;

    /// <summary>Two cache lines — useful for false-sharing avoidance on write-heavy buffers.</summary>
    public const int CacheLine128 = 128;
}
