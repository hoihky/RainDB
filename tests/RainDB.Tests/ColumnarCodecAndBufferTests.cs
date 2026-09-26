using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Core.Memory;
using RainDB.Core.Persistence;
using RainDB.Schema;

namespace RainDB.Tests;

public class ColumnarCodecAndBufferTests
{
    [Fact]
    public void Batch_codec_preserves_null_bitmap_on_fixed_width()
    {
        var nb = new byte[] { 0b0000_0010 };
        var vals = new byte[8];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(vals.AsSpan(0, 4), 1);
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(vals.AsSpan(4, 4), 2);
        var batch = new ColumnarBatch(2, [
            new FixedWidthColumnChunk(RainDbType.Int32, 2, vals, nb, hasNulls: true),
        ]);
        var decoded = RainDbBatchBinaryCodec.DecodeBatch(RainDbBatchBinaryCodec.EncodeBatch(batch));
        var col = Assert.IsType<FixedWidthColumnChunk>(decoded.Columns[0]);
        Assert.True(col.HasNulls);
        Assert.Equal(nb, col.NullBitmap.ToArray());
    }

    [Fact]
    public void Batch_codec_rejects_truncated_header()
    {
        Assert.Throws<InvalidDataException>(() => RainDbBatchBinaryCodec.DecodeBatch(new byte[] { 1, 2, 3 }));
    }

    [Fact]
    public void HybridBufferPool_general_rent_returns_requested_length()
    {
        var pool = new HybridBufferPool();
        var mem = pool.Rent(128);
        try
        {
            Assert.Equal(128, mem.Length);
        }
        finally
        {
            pool.Return(mem);
        }
    }

    [Fact]
    public void VectorChunkLimits_validate_rejects_above_max_when_strict()
    {
        var over = VectorChunkLimits.MaxRows + 1;
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            VectorChunkLimits.ValidateRowCount(over, enforce: true));
    }
}
