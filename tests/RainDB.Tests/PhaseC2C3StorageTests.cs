using System.Buffers.Binary;
using RainDB;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Core.Persistence;
using RainDB.Core.Tables;
using RainDB.Execution;
using RainDB.Schema;

namespace RainDB.Tests;

public class PhaseC2C3StorageTests
{
    [Fact]
    public void Int32_dictionary_encoding_roundtrips_through_batch_codec()
    {
        var values = new byte[40 * sizeof(int)];
        for (var row = 0; row < 40; row++)
        {
            var v = (row % 3) + 1;
            BinaryPrimitives.WriteInt32LittleEndian(values.AsSpan(row * sizeof(int), sizeof(int)), v);
        }

        var batch = new ColumnarBatch(40, [
            new FixedWidthColumnChunk(RainDbType.Int32, 40, values, ReadOnlyMemory<byte>.Empty, hasNulls: false),
        ]);
        var encoded = RainDbBatchBinaryCodec.EncodeBatch(
            batch,
            new RainDbBatchCodecOptions { EnableInt32DictionaryEncoding = true });
        var decoded = RainDbBatchBinaryCodec.DecodeBatch(encoded);
        var col = Assert.IsType<DictionaryEncodedInt32ColumnChunk>(decoded.Columns[0]);
        var materialized = col.Materialize();
        Assert.Equal(values, materialized.Values.ToArray());
    }

    [Fact]
    public async Task Sql_scan_reads_dictionary_encoded_persistent_batch()
    {
        var root = Path.Combine(Path.GetTempPath(), "raindb_dict_" + Guid.NewGuid().ToString("N"));
        try
        {
            var options = new RainDbFileDatabaseOptions { EnableInt32DictionaryEncoding = true };
            var engine = RainDbEngine.OpenPersistent(root, options);
            var fileDb = engine.FileDatabase!;
            var schema = new TableSchema([new ColumnDef("code", RainDbType.Int32)]);
            var table = fileDb.CreateMemoryTable("codes", schema);
            table.AppendBatch(LowCardinalityInt32Batch(64, distinct: 4));

            await using var r = await engine.ExecuteSqlAsync("SELECT SUM(code) FROM codes");
            var agg = Assert.IsAssignableFrom<IAggregateQueryResult>(r);
            Assert.Equal(160L, agg.Int64Value);
        }
        finally
        {
            TestDataBuilders.TryDeleteDir(root);
        }
    }

    [Fact]
    public async Task Mapped_batch_budget_evicts_cold_segments_but_queries_still_correct()
    {
        var root = Path.Combine(Path.GetTempPath(), "raindb_budget_" + Guid.NewGuid().ToString("N"));
        try
        {
            var options = new RainDbFileDatabaseOptions
            {
                MappedBatchMemoryBudgetBytes = 8_192,
                MappedBatchBudgetExceededBehavior = MappedBatchBudgetExceededBehavior.EvictColdBatches,
            };
            var engine = RainDbEngine.OpenPersistent(root, options);
            var fileDb = engine.FileDatabase!;
            var schema = new TableSchema([new ColumnDef("x", RainDbType.Int32)]);
            var table = fileDb.CreateMemoryTable("t", schema);
            table.AppendBatch(LargeInt32Batch(512, seed: 1));
            table.AppendBatch(LargeInt32Batch(512, seed: 2));

            Assert.True(fileDb.MappedBatchMemory.ResidentBytes <= options.MappedBatchMemoryBudgetBytes);

            await using var r = await engine.ExecuteSqlAsync("SELECT COUNT(*) FROM t");
            var agg = Assert.IsAssignableFrom<IAggregateQueryResult>(r);
            Assert.Equal(1024L, agg.Int64Value);
        }
        finally
        {
            TestDataBuilders.TryDeleteDir(root);
        }
    }

    [Fact]
    public void Mapped_batch_budget_fail_throws_when_segment_would_exceed_cap()
    {
        var manager = new RainDbMappedBatchMemoryManager(
            1024,
            MappedBatchBudgetExceededBehavior.Fail);
        var table = new MemoryTable("t", new TableSchema([new ColumnDef("x", RainDbType.Int32)]));
        table.AppendBatch(LargeInt32Batch(256, seed: 9));
        var path = Path.Combine(Path.GetTempPath(), "raindb_fail_" + Guid.NewGuid().ToString("N") + ".batch");
        try
        {
            File.WriteAllBytes(path, RainDbBatchBinaryCodec.EncodeBatch(table.Batches[0]));
            using var mapped = new RainDbBatchMmapReader().Open(path);
            Assert.Throws<InvalidOperationException>(() =>
                manager.RegisterMappedBatch(table, 0, mapped, path));
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    private static IColumnarBatch LowCardinalityInt32Batch(int rows, int distinct)
    {
        var values = new byte[rows * sizeof(int)];
        for (var i = 0; i < rows; i++)
            BinaryPrimitives.WriteInt32LittleEndian(values.AsSpan(i * sizeof(int), sizeof(int)), (i % distinct) + 1);
        var col = new FixedWidthColumnChunk(RainDbType.Int32, rows, values, ReadOnlyMemory<byte>.Empty, hasNulls: false);
        return new ColumnarBatch(rows, new IColumnChunk[] { col });
    }

    private static IColumnarBatch LargeInt32Batch(int rows, int seed)
    {
        var values = new byte[rows * sizeof(int)];
        for (var i = 0; i < rows; i++)
            BinaryPrimitives.WriteInt32LittleEndian(values.AsSpan(i * sizeof(int), sizeof(int)), seed * 10_000 + i);
        var col = new FixedWidthColumnChunk(RainDbType.Int32, rows, values, ReadOnlyMemory<byte>.Empty, hasNulls: false);
        return new ColumnarBatch(rows, new IColumnChunk[] { col });
    }
}
