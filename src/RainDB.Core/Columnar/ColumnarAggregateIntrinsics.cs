using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace RainDB.Core.Columnar;

/// <summary>Hardware-accelerated reductions for contiguous fixed-width columns (scalar fallbacks always available).</summary>
public sealed class ColumnarAggregateIntrinsics : IColumnarAggregateIntrinsics
{
    public double SumFloat64(ReadOnlySpan<byte> valuesLittleEndian, bool allowAvx2 = true)
    {
        if (valuesLittleEndian.Length % sizeof(double) != 0)
            throw new ArgumentException("Length must be multiple of 8.", nameof(valuesLittleEndian));
        var doubles = MemoryMarshal.Cast<byte, double>(valuesLittleEndian);
        if (doubles.IsEmpty)
            return 0d;
        if (allowAvx2 && Avx2.IsSupported && doubles.Length >= Vector256<double>.Count)
            return SumDoubleAvx2(doubles);
        return SumDoubleScalar(doubles);
    }

    public double MinFloat64(ReadOnlySpan<byte> valuesLittleEndian, bool allowAvx2 = true)
    {
        if (valuesLittleEndian.Length % sizeof(double) != 0)
            throw new ArgumentException("Length must be multiple of 8.", nameof(valuesLittleEndian));
        var doubles = MemoryMarshal.Cast<byte, double>(valuesLittleEndian);
        if (doubles.IsEmpty)
            return double.NaN;
        if (allowAvx2 && Avx2.IsSupported && doubles.Length >= Vector256<double>.Count)
            return MinDoubleAvx2(doubles);
        return MinDoubleScalar(doubles);
    }

    public double MaxFloat64(ReadOnlySpan<byte> valuesLittleEndian, bool allowAvx2 = true)
    {
        if (valuesLittleEndian.Length % sizeof(double) != 0)
            throw new ArgumentException("Length must be multiple of 8.", nameof(valuesLittleEndian));
        var doubles = MemoryMarshal.Cast<byte, double>(valuesLittleEndian);
        if (doubles.IsEmpty)
            return double.NaN;
        if (allowAvx2 && Avx2.IsSupported && doubles.Length >= Vector256<double>.Count)
            return MaxDoubleAvx2(doubles);
        return MaxDoubleScalar(doubles);
    }

    public long SumInt32(ReadOnlySpan<byte> valuesLittleEndian, bool allowSimd = true)
    {
        if (valuesLittleEndian.Length % sizeof(int) != 0)
            throw new ArgumentException("Length must be multiple of 4.", nameof(valuesLittleEndian));
        var ints = MemoryMarshal.Cast<byte, int>(valuesLittleEndian);
        if (ints.IsEmpty)
            return 0L;
        if (allowSimd && Vector128.IsHardwareAccelerated && ints.Length >= Vector128<int>.Count)
            return SumInt32Simd(ints);
        return SumInt32Scalar(ints);
    }

    public long SumInt64(ReadOnlySpan<byte> valuesLittleEndian, bool allowAvx2 = true)
    {
        if (valuesLittleEndian.Length % sizeof(long) != 0)
            throw new ArgumentException("Length must be multiple of 8.", nameof(valuesLittleEndian));
        var longs = MemoryMarshal.Cast<byte, long>(valuesLittleEndian);
        if (longs.IsEmpty)
            return 0L;
        if (allowAvx2 && Avx2.IsSupported && longs.Length >= Vector256<long>.Count)
            return SumInt64Avx2(longs);
        return SumInt64Scalar(longs);
    }

    public ulong MixHash(ulong hash, ulong value) =>
        unchecked(hash ^ (value + 0x9e3779b97f4a7c15UL + (hash << 6) + (hash >> 2)));

    private static double SumDoubleScalar(ReadOnlySpan<double> values)
    {
        double s = 0;
        foreach (var v in values)
            s += v;
        return s;
    }

    private static unsafe double SumDoubleAvx2(ReadOnlySpan<double> values)
    {
        fixed (double* p = values)
        {
            var n = values.Length;
            var i = 0;
            var acc = Vector256<double>.Zero;
            var limit = n - (n % Vector256<double>.Count);
            for (; i < limit; i += Vector256<double>.Count)
                acc = Avx2.Add(acc, Avx.LoadVector256(p + i));

            var lo = acc.GetLower();
            var hi = acc.GetUpper();
            var s128 = lo + hi;
            var sum = s128.GetElement(0) + s128.GetElement(1);
            for (; i < n; i++)
                sum += p[i];
            return sum;
        }
    }

    private static double MinDoubleScalar(ReadOnlySpan<double> values)
    {
        var m = values[0];
        for (var i = 1; i < values.Length; i++)
        {
            var v = values[i];
            if (v < m)
                m = v;
        }

        return m;
    }

    private static unsafe double MinDoubleAvx2(ReadOnlySpan<double> values)
    {
        fixed (double* p = values)
        {
            var n = values.Length;
            var i = 0;
            var acc = Vector256.Create(double.PositiveInfinity);
            var limit = n - (n % Vector256<double>.Count);
            for (; i < limit; i += Vector256<double>.Count)
                acc = Avx.Min(acc, Avx.LoadVector256(p + i));

            var m = HorizontalMinDouble256(acc);
            for (; i < n; i++)
            {
                if (p[i] < m)
                    m = p[i];
            }

            return m;
        }
    }

    private static double MaxDoubleScalar(ReadOnlySpan<double> values)
    {
        var m = values[0];
        for (var i = 1; i < values.Length; i++)
        {
            var v = values[i];
            if (v > m)
                m = v;
        }

        return m;
    }

    private static unsafe double MaxDoubleAvx2(ReadOnlySpan<double> values)
    {
        fixed (double* p = values)
        {
            var n = values.Length;
            var i = 0;
            var acc = Vector256.Create(double.NegativeInfinity);
            var limit = n - (n % Vector256<double>.Count);
            for (; i < limit; i += Vector256<double>.Count)
                acc = Avx.Max(acc, Avx.LoadVector256(p + i));

            var m = HorizontalMaxDouble256(acc);
            for (; i < n; i++)
            {
                if (p[i] > m)
                    m = p[i];
            }

            return m;
        }
    }

    private static double HorizontalMinDouble256(Vector256<double> v)
    {
        var lo = v.GetLower();
        var hi = v.GetUpper();
        var s = Vector128.Min(lo, hi);
        return Math.Min(s.GetElement(0), s.GetElement(1));
    }

    private static double HorizontalMaxDouble256(Vector256<double> v)
    {
        var lo = v.GetLower();
        var hi = v.GetUpper();
        var s = Vector128.Max(lo, hi);
        return Math.Max(s.GetElement(0), s.GetElement(1));
    }

    private static long SumInt32Scalar(ReadOnlySpan<int> values)
    {
        long s = 0;
        foreach (var v in values)
            s += v;
        return s;
    }

    private static long SumInt32Simd(ReadOnlySpan<int> values)
    {
        long total = 0;
        var i = 0;
        var laneCount = Vector128<int>.Count;
        var limit = values.Length - (values.Length % laneCount);
        for (; i < limit; i += laneCount)
        {
            var block = Vector128.LoadUnsafe(ref MemoryMarshal.GetReference(values.Slice(i, laneCount)));
            for (var j = 0; j < laneCount; j++)
                total += block.GetElement(j);
        }

        for (; i < values.Length; i++)
            total += values[i];

        return total;
    }

    private static long SumInt64Scalar(ReadOnlySpan<long> values)
    {
        long s = 0;
        foreach (var v in values)
            s += v;
        return s;
    }

    private static unsafe long SumInt64Avx2(ReadOnlySpan<long> values)
    {
        fixed (long* p = values)
        {
            var n = values.Length;
            var i = 0;
            var acc = Vector256<long>.Zero;
            var limit = n - (n % Vector256<long>.Count);
            for (; i < limit; i += Vector256<long>.Count)
                acc = Avx2.Add(acc, Avx2.LoadVector256(p + i));

            var lo = acc.GetLower();
            var hi = acc.GetUpper();
            var sum = lo + hi;
            long total = sum.GetElement(0) + sum.GetElement(1) + sum.GetElement(2) + sum.GetElement(3);
            for (; i < n; i++)
                total += p[i];
            return total;
        }
    }
}
