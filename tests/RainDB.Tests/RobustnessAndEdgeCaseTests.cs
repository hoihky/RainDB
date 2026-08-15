using System.Buffers.Binary;
using System.IO;
using RainDB;
using RainDB.Catalog;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Core.Persistence;
using RainDB.Core.Tables;
using RainDB.Execution;
using RainDB.Persistence;
using RainDB.Query.Plans;
using RainDB.Schema;

namespace RainDB.Tests;

public class RobustnessAndEdgeCaseTests
{
    [Fact]
    public async Task Group_by_negative_float64_keys_sorted_numerically()
    {
        var engine = RainDbEngine.CreateDefault();
        var schema = new TableSchema([
            new ColumnDef("k", RainDbType.Float64),
            new ColumnDef("n", RainDbType.Int32),
        ]);
        var t = new MemoryTable("t", schema);
        var keys = new byte[24];
        WriteF64(keys, 0, -2.0);
        WriteF64(keys, 8, -1.0);
        WriteF64(keys, 16, 0.0);
        var nums = new byte[12];
        BinaryPrimitives.WriteInt32LittleEndian(nums.AsSpan(0, 4), 1);
        BinaryPrimitives.WriteInt32LittleEndian(nums.AsSpan(4, 4), 2);
        BinaryPrimitives.WriteInt32LittleEndian(nums.AsSpan(8, 4), 3);
        t.AppendBatch(new ColumnarBatch(3, [
            new FixedWidthColumnChunk(RainDbType.Float64, 3, keys, ReadOnlyMemory<byte>.Empty, false),
            new FixedWidthColumnChunk(RainDbType.Int32, 3, nums, ReadOnlyMemory<byte>.Empty, false),
        ]));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT k, COUNT(n) FROM t GROUP BY k");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(3, col.Batches[0].RowCount);
        var outKeys = col.Batches[0].Columns[0];
        Assert.Equal(-2.0, ReadF64(outKeys, 0));
        Assert.Equal(-1.0, ReadF64(outKeys, 1));
        Assert.Equal(0.0, ReadF64(outKeys, 2));
    }

    [Fact]
    public async Task Group_by_null_key_aggregates_into_single_group()
    {
        var engine = RainDbEngine.CreateDefault();
        var schema = new TableSchema([
            new ColumnDef("k", RainDbType.Int32),
            new ColumnDef("v", RainDbType.Int64),
        ]);
        var t = new MemoryTable("t", schema);
        var k = new byte[8];
        BinaryPrimitives.WriteInt32LittleEndian(k.AsSpan(0, 4), 1);
        BinaryPrimitives.WriteInt32LittleEndian(k.AsSpan(4, 4), 0);
        var nb = new byte[] { 0b0000_0010 };
        var v = new byte[16];
        BinaryPrimitives.WriteInt64LittleEndian(v.AsSpan(0, 8), 10);
        BinaryPrimitives.WriteInt64LittleEndian(v.AsSpan(8, 8), 20);
        t.AppendBatch(new ColumnarBatch(2, [
            new FixedWidthColumnChunk(RainDbType.Int32, 2, k, nb, hasNulls: true),
            new FixedWidthColumnChunk(RainDbType.Int64, 2, v, ReadOnlyMemory<byte>.Empty, false),
        ]));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT k, SUM(v) FROM t GROUP BY k");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(2, col.Batches[0].RowCount);
        var sums = col.Batches[0].Columns[1];
        var total = BinaryPrimitives.ReadInt64LittleEndian(sums.Values.Span[..8])
            + BinaryPrimitives.ReadInt64LittleEndian(sums.Values.Span.Slice(8, 8));
        Assert.Equal(30L, total);
    }

    [Fact]
    public async Task Inner_join_null_key_does_not_match()
    {
        var engine = RainDbEngine.CreateDefault();
        var left = new MemoryTable("L", new TableSchema([new ColumnDef("id", RainDbType.Int32)]));
        var k = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(k, 1);
        left.AppendBatch(new ColumnarBatch(1, [
            new FixedWidthColumnChunk(RainDbType.Int32, 1, k, new byte[] { 0b0000_0001 }, hasNulls: true),
        ]));
        var right = new MemoryTable("R", new TableSchema([new ColumnDef("id", RainDbType.Int32)]));
        right.AppendBatch(SingleInt32Batch(1));
        engine.Catalog.Register(left);
        engine.Catalog.Register(right);

        await using var r = await engine.ExecuteSqlAsync("SELECT * FROM L INNER JOIN R ON L.id = R.id");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(0, col.RowCount);
    }

    [Fact]
    public async Task Where_boolean_greater_than_false()
    {
        var engine = RainDbEngine.CreateDefault();
        var t = new MemoryTable("t", new TableSchema([new ColumnDef("flag", RainDbType.Boolean)]));
        t.AppendBatch(new ColumnarBatch(2, [
            new FixedWidthColumnChunk(RainDbType.Boolean, 2, new byte[] { 0, 1 }, ReadOnlyMemory<byte>.Empty, false),
        ]));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT flag FROM t WHERE flag > FALSE");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(1, col.RowCount);
        Assert.Equal(1, col.Batches[0].Columns[0].Values.Span[0]);
    }

    [Fact]
    public void Batch_codec_rejects_trailing_bytes()
    {
        var batch = new ColumnarBatch(1, [
            new FixedWidthColumnChunk(RainDbType.Int32, 1, new byte[4], ReadOnlyMemory<byte>.Empty, false),
        ]);
        var bytes = RainDbBatchBinaryCodec.EncodeBatch(batch);
        var corrupt = bytes.Concat(new byte[] { 0xFF }).ToArray();
        Assert.Throws<InvalidDataException>(() => RainDbBatchBinaryCodec.DecodeBatch(corrupt));
    }

    [Fact]
    public void Batch_codec_rejects_wrong_column_type_on_append()
    {
        var schema = new TableSchema([new ColumnDef("x", RainDbType.Int32)]);
        var table = new MemoryTable("t", schema);
        var wrongBatch = new ColumnarBatch(1, [
            new FixedWidthColumnChunk(RainDbType.Int64, 1, new byte[8], ReadOnlyMemory<byte>.Empty, false),
        ]);
        Assert.Throws<ArgumentException>(() => table.AppendBatch(wrongBatch));
    }

    [Fact]
    public async Task Join_utf8_mixed_chunk_encodings_across_batches()
    {
        var engine = RainDbEngine.CreateDefault();
        var schema = new TableSchema([
            new ColumnDef("id", RainDbType.Int32),
            new ColumnDef("name", RainDbType.Utf8),
        ]);
        var left = new MemoryTable("L", schema);
        var id1 = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(id1, 1);
        left.AppendBatch(new ColumnarBatch(1, [
            new FixedWidthColumnChunk(RainDbType.Int32, 1, id1, ReadOnlyMemory<byte>.Empty, false),
            new Utf8ColumnChunk(1, new[] { 0, 2 }, "ab"u8.ToArray(), ReadOnlyMemory<byte>.Empty, false),
        ]));
        var id2 = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(id2, 2);
        var lpBlob = new byte[4 + 1];
        BinaryPrimitives.WriteInt32LittleEndian(lpBlob, 1);
        lpBlob[4] = (byte)'z';
        left.AppendBatch(new ColumnarBatch(1, [
            new FixedWidthColumnChunk(RainDbType.Int32, 1, id2, ReadOnlyMemory<byte>.Empty, false),
            new Utf8LengthPrefixedColumnChunk(1, lpBlob, ReadOnlyMemory<byte>.Empty, false),
        ]));

        var right = new MemoryTable("R", new TableSchema([new ColumnDef("id", RainDbType.Int32)]));
        var rid = new byte[8];
        BinaryPrimitives.WriteInt32LittleEndian(rid.AsSpan(0, 4), 1);
        BinaryPrimitives.WriteInt32LittleEndian(rid.AsSpan(4, 4), 2);
        right.AppendBatch(new ColumnarBatch(2, [
            new FixedWidthColumnChunk(RainDbType.Int32, 2, rid, ReadOnlyMemory<byte>.Empty, false),
        ]));
        engine.Catalog.Register(left);
        engine.Catalog.Register(right);

        await using var r = await engine.ExecuteSqlAsync("SELECT L.name FROM L INNER JOIN R ON L.id = R.id");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(2, col.RowCount);
        Assert.IsType<Utf8ColumnChunk>(col.Batches[0].Columns[0]);
    }

    [Fact]
    public async Task Scan_with_empty_batch_segment_then_data_batch()
    {
        var engine = RainDbEngine.CreateDefault();
        var schema = new TableSchema([new ColumnDef("x", RainDbType.Int32)]);
        var t = new MemoryTable("t", schema);
        t.AppendBatch(new ColumnarBatch(0, [
            new FixedWidthColumnChunk(RainDbType.Int32, 0, Array.Empty<byte>(), ReadOnlyMemory<byte>.Empty, false),
        ]));
        t.AppendBatch(SingleInt32Batch(42));
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT SUM(x) FROM t");
        var agg = Assert.IsAssignableFrom<IAggregateQueryResult>(r);
        Assert.Equal(42L, agg.Int64Value);
    }

    [Fact]
    public void AppendBatch_rolls_back_memory_when_persistence_fails()
    {
        var schema = new TableSchema([new ColumnDef("x", RainDbType.Int32)]);
        var table = new MemoryTable("t", schema, options: new MemoryTableOptions(BatchPersistence: new ThrowingPersistence()));
        var batch = SingleInt32Batch(1);
        Assert.Throws<IOException>(() => table.AppendBatch(batch));
        Assert.Equal(0, table.RowCount);
        Assert.Empty(table.Batches);
    }

    [Fact]
    public async Task Pooled_column_chunk_throws_after_dispose()
    {
        var engine = RainDbEngine.CreateDefault();
        var t = new MemoryTable("t", new TableSchema([new ColumnDef("x", RainDbType.Int32)]));
        t.AppendBatch(SingleInt32Batch(1));
        engine.Catalog.Register(t);

        var plan = new VectorizedScanPhysicalPlan(
            t.Id,
            outputColumnIndices: [0],
            filters: [new ColumnCompareFilter(0, ScalarCompareOp.Eq, 1)],
            aggregate: null,
            options: new VectorizedScanExecutionOptions { MaxDegreeOfParallelism = 1 });

        var result = await engine.ExecutePhysicalAsync(plan);
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(result);
        var chunk = col.Batches[0].Columns[0];
        await result.DisposeAsync();
        Assert.Throws<ObjectDisposedException>(() => chunk.Values);
    }

    private sealed class ThrowingPersistence : IRainDbBatchPersistence
    {
        public void OnBatchAppended(TableId tableId, string tableName, int zeroBasedBatchIndex, IColumnarBatch batch) =>
            throw new IOException("simulated persistence failure");
    }

    private static void WriteF64(byte[] buf, int offset, double v) =>
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(offset, 8), BitConverter.DoubleToInt64Bits(v));

    private static double ReadF64(IColumnChunk col, int row) =>
        BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(col.Values.Span.Slice(row * 8, 8)));

    private static ColumnarBatch SingleInt32Batch(int value)
    {
        var b = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(b, value);
        return new ColumnarBatch(1, [new FixedWidthColumnChunk(RainDbType.Int32, 1, b, ReadOnlyMemory<byte>.Empty, false)]);
    }
}
