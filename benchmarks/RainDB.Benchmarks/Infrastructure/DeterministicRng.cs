namespace RainDB.Benchmarks.Infrastructure;

/// <summary>Small deterministic PRNG for reproducible synthetic column data (LCG).</summary>
internal sealed class DeterministicRng
{
    private ulong _state;

    public DeterministicRng(ulong seed) => _state = seed == 0 ? 1 : seed;

    public int NextInt32(int minInclusive, int maxExclusive)
    {
        if (maxExclusive <= minInclusive)
            throw new ArgumentOutOfRangeException(nameof(maxExclusive));
        var range = (uint)(maxExclusive - minInclusive);
        return minInclusive + (int)(NextUInt32() % range);
    }

    public double NextUnitDouble()
    {
        var u = NextUInt32();
        return u / (double)uint.MaxValue;
    }

    private uint NextUInt32()
    {
        _state = unchecked((_state * 6364136223846793005UL) + 1UL);
        return (uint)(_state >> 32);
    }
}
