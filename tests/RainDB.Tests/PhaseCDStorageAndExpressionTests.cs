using System.Buffers.Binary;
using System.Runtime.InteropServices;
using RainDB;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Core.Persistence;
using RainDB.Core.Tables;
using RainDB.Execution;
using RainDB.Schema;

namespace RainDB.Tests;

public class PhaseCDStorageAndExpressionTests
{
    [Fact]
    public void RainDbBatchMmapReader_maps_fixed_width_without_full_file_copy()
    {
        var path = Path.Combine(Path.GetTempPath(), "raindb_mmap_" + Guid.NewGuid().ToString("N") + ".batch");
        try
        {
            var col = new FixedWidthColumnChunk(
                RainDbType.Int32,
                2,
                new byte[] { 10, 0, 0, 0, 20, 0, 0, 0 },
                ReadOnlyMemory<byte>.Empty,
                hasNulls: false);
            var batch = new ColumnarBatch(2, new IColumnChunk[] { col });
            File.WriteAllBytes(path, RainDbBatchBinaryCodec.EncodeBatch(batch));

            using var mapped = new RainDbBatchMmapReader().Open(path);
            var fw = Assert.IsType<FixedWidthColumnChunk>(mapped.Batch.Columns[0]);
            Assert.False(MemoryMarshal.TryGetArray(fw.Values, out _));
            Assert.Equal(10, BinaryPrimitives.ReadInt32LittleEndian(fw.Values.Span));
            Assert.Equal(20, BinaryPrimitives.ReadInt32LittleEndian(fw.Values.Span.Slice(4, 4)));
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task Hydration_skips_incomplete_batch_tmp_files()
    {
        var root = Path.Combine(Path.GetTempPath(), "raindb_tmp_batch_" + Guid.NewGuid().ToString("N"));
        try
        {
            var engine = RainDbEngine.OpenPersistent(root);
            var fileDb = engine.FileDatabase!;
            var schema = new TableSchema([new ColumnDef("x", RainDbType.Int32)]);
            var table = fileDb.CreateMemoryTable("t", schema);
            table.AppendBatch(SingleInt32Batch(1));

            var tableDir = Path.Combine(root, RainDbFileDatabase.TablesDirectoryName, table.Id.ToString());
            File.WriteAllBytes(Path.Combine(tableDir, "000001.batch.tmp"), new byte[] { 1, 2, 3 });

            var engine2 = RainDbEngine.OpenPersistent(root);
            await using var r = await engine2.ExecuteSqlAsync("SELECT COUNT(*) FROM t");
            var agg = Assert.IsAssignableFrom<IAggregateQueryResult>(r);
            Assert.Equal(1L, agg.Int64Value);
        }
        finally
        {
            TestDataBuilders.TryDeleteDir(root);
        }
    }

    [Fact]
    public void Open_ignores_catalog_json_tmp_when_committed_catalog_exists()
    {
        var root = Path.Combine(Path.GetTempPath(), "raindb_cat_tmp_" + Guid.NewGuid().ToString("N"));
        try
        {
            var engine = RainDbEngine.OpenPersistent(root);
            var fileDb = engine.FileDatabase!;
            var schema = new TableSchema([new ColumnDef("x", RainDbType.Int32)]);
            fileDb.CreateMemoryTable("good", schema);

            File.WriteAllText(
                Path.Combine(root, RainDbFileDatabase.CatalogFileName + ".tmp"),
                """{"formatVersion":1,"tables":[{"id":"dead","name":"bad","columns":[{"name":"x","type":"Int32"}]}]}""");

            var engine2 = RainDbEngine.OpenPersistent(root);
            Assert.True(engine2.Catalog.TryGetTable("good", out _));
            Assert.False(engine2.Catalog.TryGetTable("bad", out _));
        }
        finally
        {
            TestDataBuilders.TryDeleteDir(root);
        }
    }

    [Fact]
    public async Task OnBatchAppended_rewrites_catalog_after_batch_is_durable()
    {
        var root = Path.Combine(Path.GetTempPath(), "raindb_cat_order_" + Guid.NewGuid().ToString("N"));
        try
        {
            var engine = RainDbEngine.OpenPersistent(root);
            var fileDb = engine.FileDatabase!;
            var schema = new TableSchema([new ColumnDef("x", RainDbType.Int32)]);
            var table = fileDb.CreateMemoryTable("t", schema);
            table.AppendBatch(SingleInt32Batch(42));
            var batchPath = Path.Combine(root, RainDbFileDatabase.TablesDirectoryName, table.Id.ToString(), "000000.batch");
            Assert.True(File.Exists(batchPath));
            Assert.False(File.Exists(batchPath + ".tmp"));
            var catalogPath = Path.Combine(root, RainDbFileDatabase.CatalogFileName);
            Assert.True(File.Exists(catalogPath));
            Assert.False(File.Exists(catalogPath + ".tmp"));

            var engine2 = RainDbEngine.OpenPersistent(root);
            await using var r = await engine2.ExecuteSqlAsync("SELECT x FROM t");
            var rows = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
            Assert.Equal(1, rows.RowCount);
        }
        finally
        {
            TestDataBuilders.TryDeleteDir(root);
        }
    }

    [Fact]
    public async Task Sql_where_int32_expression_compares_to_literal()
    {
        var engine = RainDbEngine.CreateDefault();
        var t = TestDataBuilders.TableWithInt32Column("t", "a", [(1, false), (5, false), (10, false)]);
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT a FROM t WHERE a + 2 > 6");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(2, col.RowCount);
        var values = col.Batches[0].Columns[0].Values.Span;
        Assert.Equal(5, BinaryPrimitives.ReadInt32LittleEndian(values));
        Assert.Equal(10, BinaryPrimitives.ReadInt32LittleEndian(values.Slice(4)));
    }

    [Fact]
    public async Task Sql_select_int32_arithmetic_expression()
    {
        var engine = RainDbEngine.CreateDefault();
        var t = TestDataBuilders.TableWithInt32Column("t", "k", [(3, false), (7, false)]);
        engine.Catalog.Register(t);

        await using var r = await engine.ExecuteSqlAsync("SELECT k + 1 FROM t");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(2, col.RowCount);
        var values = col.Batches[0].Columns[0].Values.Span;
        Assert.Equal(4, BinaryPrimitives.ReadInt32LittleEndian(values));
        Assert.Equal(8, BinaryPrimitives.ReadInt32LittleEndian(values.Slice(4)));
    }

    private static IColumnarBatch SingleInt32Batch(int value)
    {
        var bytes = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(bytes, value);
        var col = new FixedWidthColumnChunk(RainDbType.Int32, 1, bytes, ReadOnlyMemory<byte>.Empty, hasNulls: false);
        return new ColumnarBatch(1, new IColumnChunk[] { col });
    }
}
