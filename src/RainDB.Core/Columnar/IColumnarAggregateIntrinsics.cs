namespace RainDB.Core.Columnar;

/// <summary>SIMD-backed column reductions with scalar fallbacks (injectable from scan operators).</summary>
public interface IColumnarAggregateIntrinsics
{
    double SumFloat64(ReadOnlySpan<byte> valuesLittleEndian, bool allowAvx2 = true);

    double MinFloat64(ReadOnlySpan<byte> valuesLittleEndian, bool allowAvx2 = true);

    double MaxFloat64(ReadOnlySpan<byte> valuesLittleEndian, bool allowAvx2 = true);

    long SumInt32(ReadOnlySpan<byte> valuesLittleEndian, bool allowSimd = true);

    long SumInt64(ReadOnlySpan<byte> valuesLittleEndian, bool allowAvx2 = true);

    ulong MixHash(ulong hash, ulong value);
}
