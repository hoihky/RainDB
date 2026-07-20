using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace RainDB.Core.Columnar;

/// <summary>Optional hardware-accelerated reductions for fixed-width vectors (P1).</summary>
public static class AggregateIntrinsics
{
    /// <summary>Sum of contiguous little-endian <see cref="double"/> values (no nulls / caller filtered).</summary>
    public static double SumFloat64(ReadOnlySpan<byte> valuesLittleEndian, bool allowAvx2 = true)
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
                acc = Avx2.Add(acc, Avx2.LoadVector256(p + i));

            var lo = acc.GetLower();
            var hi = acc.GetUpper();
            var s128 = lo + hi;
            var sum = s128.GetElement(0) + s128.GetElement(1);
            for (; i < n; i++)
                sum += p[i];
            return sum;
        }
    }

    public static double MinFloat64(ReadOnlySpan<byte> valuesLittleEndian)
    {
        var doubles = MemoryMarshal.Cast<byte, double>(valuesLittleEndian);
        if (doubles.IsEmpty)
            return double.NaN;
        var m = doubles[0];
        for (var i = 1; i < doubles.Length; i++)
        {
            var v = doubles[i];
            if (v < m)
                m = v;
        }

        return m;
    }

    public static double MaxFloat64(ReadOnlySpan<byte> valuesLittleEndian)
    {
        var doubles = MemoryMarshal.Cast<byte, double>(valuesLittleEndian);
        if (doubles.IsEmpty)
            return double.NaN;
        var m = doubles[0];
        for (var i = 1; i < doubles.Length; i++)
        {
            var v = doubles[i];
            if (v > m)
                m = v;
        }

        return m;
    }
}
