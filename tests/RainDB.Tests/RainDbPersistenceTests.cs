using RainDB;
using RainDB.Catalog;
using RainDB.Columnar;
using RainDB.Core.Catalog;
using RainDB.Core.Columnar;
using RainDB.Core.Persistence;
using RainDB.Core.Tables;
using RainDB.Execution;
using RainDB.Schema;

namespace RainDB.Tests;

public class RainDbPersistenceTests
{
    [Fact]
    public void RainDbBatchBinaryCodec_roundtrips_fixed_utf8_arrow_and_length_prefixed()
    {
        var fw = new FixedWidthColumnChunk(RainDbType.Int32, 2, new byte[] { 1, 0, 0, 0, 2, 0, 0, 0 }, ReadOnlyMemory<byte>.Empty, hasNulls: false);
        var utf8Arrow = new Utf8ColumnChunk(2, new[] { 0, 1, 2 }, "ab"u8.ToArray(), ReadOnlyMemory<byte>.Empty, hasNulls: false);
        var blob = new byte[4 + 1 + 4 + 2];
        BitConverter.GetBytes(1).CopyTo(blob, 0);
        blob[4] = (byte)'x';
        BitConverter.GetBytes(2).CopyTo(blob, 5);
        blob[9] = (byte)'y';
        blob[10] = (byte)'z';
        var utf8Lp = new Utf8LengthPrefixedColumnChunk(2, blob, ReadOnlyMemory<byte>.Empty, hasNulls: false);

        var b1 = new ColumnarBatch(2, new IColumnChunk[] { fw });
        var decoded1 = RainDbBatchBinaryCodec.DecodeBatch(RainDbBatchBinaryCodec.EncodeBatch(b1));
        Assert.Equal(2, decoded1.RowCount);
        Assert.IsType<FixedWidthColumnChunk>(decoded1.Columns[0]);

        var b2 = new ColumnarBatch(2, new IColumnChunk[] { utf8Arrow });
        var decoded2 = RainDbBatchBinaryCodec.DecodeBatch(RainDbBatchBinaryCodec.EncodeBatch(b2));
        Assert.IsType<Utf8ColumnChunk>(decoded2.Columns[0]);

        var b3 = new ColumnarBatch(2, new IColumnChunk[] { utf8Lp });
        var decoded3 = RainDbBatchBinaryCodec.DecodeBatch(RainDbBatchBinaryCodec.EncodeBatch(b3));
        var lp = Assert.IsType<Utf8LengthPrefixedColumnChunk>(decoded3.Columns[0]);
        Assert.Equal("x", System.Text.Encoding.UTF8.GetString(lp.GetPayloadSpan(0)));
        Assert.Equal("yz", System.Text.Encoding.UTF8.GetString(lp.GetPayloadSpan(1)));
    }

    [Fact]
    public async Task OpenPersistent_append_then_reopen_restores_batches()
    {
        var root = Path.Combine(Path.GetTempPath(), "raindb_persist_" + Guid.NewGuid().ToString("N"));
        try
        {
            var engine = RainDbEngine.OpenPersistent(root);
            var fileDb = engine.FileDatabase ?? throw new InvalidOperationException("Expected FileDatabase.");
            var schema = new TableSchema([
                new ColumnDef("region", RainDbType.Utf8),
                new ColumnDef("amount", RainDbType.Float64),
            ]);
            var table = fileDb.CreateMemoryTable("sales", schema);
            var utf8 = new Utf8ColumnChunk(2, new[] { 0, 2, 4 }, "usuk"u8.ToArray(), ReadOnlyMemory<byte>.Empty, hasNulls: false);
            var amt = new FixedWidthColumnChunk(
                RainDbType.Float64,
                2,
                BitConverter.GetBytes(1.0).Concat(BitConverter.GetBytes(2.0)).ToArray(),
                ReadOnlyMemory<byte>.Empty,
                hasNulls: false);
            table.AppendBatch(new ColumnarBatch(2, new IColumnChunk[] { utf8, amt }));

            await using var r1 = await engine.ExecuteSqlAsync("SELECT COUNT(*) FROM sales");
            var agg1 = Assert.IsAssignableFrom<IAggregateQueryResult>(r1);
            Assert.False(agg1.ValueIsNull);
            Assert.Equal(2L, agg1.Int64Value);

            var engine2 = RainDbEngine.OpenPersistent(root);
            await using var r2 = await engine2.ExecuteSqlAsync("SELECT COUNT(*) FROM sales");
            var agg2 = Assert.IsAssignableFrom<IAggregateQueryResult>(r2);
            Assert.False(agg2.ValueIsNull);
            Assert.Equal(2L, agg2.Int64Value);
        }
        finally
        {
            TryDeleteDir(root);
        }
    }

    [Fact]
    public void ExportCatalog_then_ImportCatalog_roundtrips_memory_tables()
    {
        var root = Path.Combine(Path.GetTempPath(), "raindb_export_" + Guid.NewGuid().ToString("N"));
        try
        {
            var engine = RainDbEngine.CreateDefault();
            var schema = new TableSchema([new ColumnDef("x", RainDbType.Int32)]);
            var table = new MemoryTable("t", schema);
            engine.Catalog.Register(table);
            var col = new FixedWidthColumnChunk(RainDbType.Int32, 3, new byte[] { 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0 }, ReadOnlyMemory<byte>.Empty, hasNulls: false);
            table.AppendBatch(new ColumnarBatch(3, new IColumnChunk[] { col }));

            RainDbFileDatabase.ExportCatalog(engine.Catalog, root);
            var imported = RainDbFileDatabase.ImportCatalog(root);
            Assert.True(imported.TryGetTable("t", out var ts));
            var mt = Assert.IsType<MemoryTable>(ts);
            Assert.Equal(3L, mt.RowCount);
            Assert.Single(mt.Batches);
        }
        finally
        {
            TryDeleteDir(root);
        }
    }

    private static void TryDeleteDir(string path)
    {
        try
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
        catch
        {
            // temp cleanup best-effort
        }
    }
}
