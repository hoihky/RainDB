using System.Buffers.Binary;
using System.Runtime.InteropServices;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Core.Tables;
using RainDB.Execution;
using RainDB.Query.Plans;
using RainDB.Schema;

namespace RainDB.Tests;

public class AggregateIntrinsicsTests
{
    [Fact]
    public void SumFloat64_simd_and_scalar_paths_agree()
    {
        var values = CreateDoublePayload(1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 0.25d);
        var simd = AggregateIntrinsics.SumFloat64(values, allowAvx2: true);
        var scalar = AggregateIntrinsics.SumFloat64(values, allowAvx2: false);
        Assert.Equal(scalar, simd);
        Assert.Equal(36.25d, simd);
    }

    [Fact]
    public void MinMaxFloat64_simd_and_scalar_paths_agree()
    {
        var values = CreateDoublePayload(3d, -1d, 7d, 2d, -4d, 9d, 0d, 5d, 1d);
        var minSimd = AggregateIntrinsics.MinFloat64(values, allowAvx2: true);
        var minScalar = AggregateIntrinsics.MinFloat64(values, allowAvx2: false);
        var maxSimd = AggregateIntrinsics.MaxFloat64(values, allowAvx2: true);
        var maxScalar = AggregateIntrinsics.MaxFloat64(values, allowAvx2: false);
        Assert.Equal(minScalar, minSimd);
        Assert.Equal(maxScalar, maxSimd);
        Assert.Equal(-4d, minSimd);
        Assert.Equal(9d, maxSimd);
    }

    [Fact]
    public void MinMaxFloat64_empty_returns_nan()
    {
        Assert.True(double.IsNaN(AggregateIntrinsics.MinFloat64(ReadOnlySpan<byte>.Empty)));
        Assert.True(double.IsNaN(AggregateIntrinsics.MaxFloat64(ReadOnlySpan<byte>.Empty)));
    }

    [Fact]
    public void SumInt32_simd_and_scalar_paths_agree()
    {
        var values = CreateInt32Payload(1, 2, 3, 4, 5, 6, 7, 8, 9);
        var simd = AggregateIntrinsics.SumInt32(values, allowSimd: true);
        var scalar = AggregateIntrinsics.SumInt32(values, allowSimd: false);
        Assert.Equal(scalar, simd);
        Assert.Equal(45L, simd);
    }

    [Fact]
    public void SumInt64_simd_and_scalar_paths_agree()
    {
        var values = CreateInt64Payload(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
        var simd = AggregateIntrinsics.SumInt64(values, allowAvx2: true);
        var scalar = AggregateIntrinsics.SumInt64(values, allowAvx2: false);
        Assert.Equal(scalar, simd);
        Assert.Equal(45L, simd);
    }

    [Fact]
    public void SumInt32_handles_large_column_without_overflow_in_test_vectors()
    {
        var ints = new int[10_000];
        for (var i = 0; i < ints.Length; i++)
            ints[i] = (i % 7) - 3;
        var payload = MemoryMarshal.AsBytes(ints.AsSpan());
        var simd = AggregateIntrinsics.SumInt32(payload, allowSimd: true);
        var scalar = AggregateIntrinsics.SumInt32(payload, allowSimd: false);
        Assert.Equal(scalar, simd);
    }

    [Fact]
    public void MixHash_is_deterministic()
    {
        var a = AggregateIntrinsics.MixHash(0x1234UL, 0x5678UL);
        var b = AggregateIntrinsics.MixHash(0x1234UL, 0x5678UL);
        Assert.Equal(a, b);
        Assert.NotEqual(a, AggregateIntrinsics.MixHash(0x1234UL, 0x5679UL));
    }

    [Fact]
    public void SumFloat64_rejects_misaligned_length()
    {
        var bad = new byte[] { 1, 2, 3 };
        Assert.Throws<ArgumentException>(() => AggregateIntrinsics.SumFloat64(bad));
    }

    [Fact]
    public async Task Vectorized_scan_int_sum_uses_intrinsics_when_opted_in()
    {
        var engine = TestDataBuilders.CreateEngine();
        var schema = new TableSchema([new ColumnDef("v", RainDbType.Int32)]);
        var table = new MemoryTable("T", schema);
        var ints = new int[512];
        for (var i = 0; i < ints.Length; i++)
            ints[i] = i + 1;
        var bytes = new byte[ints.Length * sizeof(int)];
        Buffer.BlockCopy(ints, 0, bytes, 0, bytes.Length);
        table.AppendBatch(new ColumnarBatch(
            ints.Length,
            new IColumnChunk[]
            {
                new FixedWidthColumnChunk(
                    RainDbType.Int32,
                    ints.Length,
                    bytes,
                    ReadOnlyMemory<byte>.Empty,
                    false),
            }));
        engine.Catalog.Register(table);

        var plan = new VectorizedScanPhysicalPlan(
            table.Id,
            outputColumnIndices: [0],
            filters: null,
            aggregate: new AggregateSpec(0, AggregateKind.Sum),
            options: new VectorizedScanExecutionOptions { UseAvx2IntegerSum = true });

        await using var r = await engine.ExecutePhysicalAsync(plan);
        var agg = Assert.IsAssignableFrom<IAggregateQueryResult>(r);
        Assert.Equal(ints.Length, agg.ContributingRowCount);
        Assert.Equal(ints.Sum(i => (long)i), agg.Int64Value);
    }

    private static byte[] CreateDoublePayload(params double[] values)
    {
        var bytes = new byte[values.Length * sizeof(double)];
        for (var i = 0; i < values.Length; i++)
            BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(i * sizeof(double)), BitConverter.DoubleToInt64Bits(values[i]));
        return bytes;
    }

    private static byte[] CreateInt32Payload(params int[] values)
    {
        var bytes = new byte[values.Length * sizeof(int)];
        for (var i = 0; i < values.Length; i++)
            BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(i * sizeof(int)), values[i]);
        return bytes;
    }

    private static byte[] CreateInt64Payload(params long[] values)
    {
        var bytes = new byte[values.Length * sizeof(long)];
        for (var i = 0; i < values.Length; i++)
            BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(i * sizeof(long)), values[i]);
        return bytes;
    }
}
