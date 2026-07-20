using RainDB.Catalog;
using RainDB.Columnar;
using RainDB.Core.Catalog;
using RainDB.Core.Columnar;
using RainDB.Core.Memory;
using RainDB.Core.Tables;
using RainDB.Schema;

namespace RainDB.Tests;

public class ColumnarAndCatalogTests
{
    [Fact]
    public void HybridBufferPool_RentAligned_default_is_Vector256_aligned()
    {
        var pool = new HybridBufferPool();
        using var owner = pool.RentAligned(40);
        var span = owner.Memory.Span;
        unsafe
        {
            fixed (byte* p = span)
            {
                var addr = (nuint)p;
                Assert.Equal(0u, addr % 32);
            }
        }
    }

    [Fact]
    public void HybridBufferPool_RentAligned_rejects_alignment_below_32()
    {
        var pool = new HybridBufferPool();
        Assert.Throws<ArgumentException>(() => pool.RentAligned(8, 16));
    }

    [Fact]
    public void MemoryTable_strict_chunk_policy_enforces_64K_to_1M()
    {
        var schema = new TableSchema([new ColumnDef("x", RainDbType.Int32)]);
        var opts = new MemoryTableOptions(StrictVectorChunkRows: true);
        var table = new MemoryTable("t", schema, options: opts);
        var col = new FixedWidthColumnChunk(RainDbType.Int32, 2, new byte[8], ReadOnlyMemory<byte>.Empty, hasNulls: false);
        var batch = new ColumnarBatch(2, new IColumnChunk[] { col });
        Assert.Throws<ArgumentOutOfRangeException>(() => table.AppendBatch(batch));
    }

    [Fact]
    public void Utf8LengthPrefixedColumnChunk_roundtrip()
    {
        // row0: len=1 "a", row1: len=2 "bc"
        var blob = new byte[4 + 1 + 4 + 2];
        BitConverter.GetBytes(1).CopyTo(blob, 0);
        blob[4] = (byte)'a';
        BitConverter.GetBytes(2).CopyTo(blob, 5);
        blob[9] = (byte)'b';
        blob[10] = (byte)'c';
        var chunk = new Utf8LengthPrefixedColumnChunk(2, blob, ReadOnlyMemory<byte>.Empty, hasNulls: false);
        Assert.Equal("a", System.Text.Encoding.UTF8.GetString(chunk.GetPayloadSpan(0)));
        Assert.Equal("bc", System.Text.Encoding.UTF8.GetString(chunk.GetPayloadSpan(1)));
    }

    [Fact]
    public void MemoryTable_AppendBatch_validates_schema()
    {
        var schema = new TableSchema([new ColumnDef("x", RainDbType.Int32)]);
        var table = new MemoryTable("t", schema);
        var col = new FixedWidthColumnChunk(RainDbType.Int32, 2, new byte[8], ReadOnlyMemory<byte>.Empty, hasNulls: false);
        var batch = new ColumnarBatch(2, new IColumnChunk[] { col });
        table.AppendBatch(batch);
        Assert.Equal(2, table.RowCount);
    }

    [Fact]
    public void MemoryTable_AppendBatch_rejects_type_mismatch()
    {
        var schema = new TableSchema([new ColumnDef("x", RainDbType.Int32)]);
        var table = new MemoryTable("t", schema);
        var col = new FixedWidthColumnChunk(RainDbType.Int64, 1, new byte[8], ReadOnlyMemory<byte>.Empty, hasNulls: false);
        var batch = new ColumnarBatch(1, new IColumnChunk[] { col });
        Assert.Throws<ArgumentException>(() => table.AppendBatch(batch));
    }

    [Fact]
    public void MemoryTable_BumpSchemaVersion_fires_event_and_returns_version()
    {
        var schema = new TableSchema([new ColumnDef("x", RainDbType.Int32)]);
        var table = new MemoryTable("t", schema);
        int? seen = null;
        table.SchemaVersionChanged += (_, e) => seen = e.NewVersion;
        var v = table.BumpSchemaVersion();
        Assert.Equal(2, v);
        Assert.Equal(2, seen);
        Assert.Equal(2, table.SchemaVersion);
    }

    [Fact]
    public void MemoryTable_strict_mode_accepts_64K_row_batch()
    {
        const int n = VectorChunkLimits.MinRows;
        var schema = new TableSchema([new ColumnDef("x", RainDbType.Int32)]);
        var table = new MemoryTable("t", schema, options: new MemoryTableOptions(StrictVectorChunkRows: true));
        var values = new byte[n * sizeof(int)];
        var col = new FixedWidthColumnChunk(RainDbType.Int32, n, values, ReadOnlyMemory<byte>.Empty, hasNulls: false);
        var batch = new ColumnarBatch(n, new IColumnChunk[] { col });
        table.AppendBatch(batch);
        Assert.Equal(n, table.RowCount);
    }

    [Fact]
    public void InMemoryCatalog_resolves_by_name_and_id()
    {
        var cat = new InMemoryCatalog();
        var schema = new TableSchema([new ColumnDef("a", RainDbType.Boolean)]);
        var t = new MemoryTable("sales", schema);
        cat.Register(t);
        Assert.True(cat.TryGetTable("sales", out var byName));
        Assert.Same(t, byName);
        Assert.True(cat.TryGetTable(t.Id, out var byId));
        Assert.Same(t, byId);
        Assert.Contains(t.Id, cat.TableIds);
    }
}
