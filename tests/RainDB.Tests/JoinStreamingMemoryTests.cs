using System.Buffers.Binary;
using RainDB;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Core.Tables;
using RainDB.Execution;
using RainDB.Logical;
using RainDB.Query.Execution;
using RainDB.Query.Execution.Joining;
using RainDB.Query.Plans;
using RainDB.Schema;
using RainDB.Sql;
using RainDB.Sql.Compilation;

namespace RainDB.Tests;

public class JoinStreamingMemoryTests
{
    [Fact]
    public void Hash_join_streaming_emits_multiple_batches_when_chunk_size_is_small()
    {
        const int rowsPerSide = 5;
        const int joinKey = 7;
        const int chunkRows = 4;

        var schema = new TableSchema([new ColumnDef("k", RainDbType.Int32)]);
        var left = new MemoryTable("L", schema);
        var right = new MemoryTable("R", schema);
        left.AppendBatch(Int32Batch(rowsPerSide, joinKey));
        right.AppendBatch(Int32Batch(rowsPerSide, joinKey));

        var engine = RainDbEngine.CreateDefault();
        engine.Catalog.Register(left);
        engine.Catalog.Register(right);

        var logical = StrictSqlSubset.ParseLogicalPlan("SELECT * FROM L INNER JOIN R ON L.k = R.k");
        var join = Assert.IsType<LogicalInnerJoin>(logical.Root);
        var joinPlan = Assert.IsType<JoinPhysicalPlan>(LogicalJoinBinder.BindAndLower(join, engine.Catalog, PhysicalJoinAlgorithm.Hash));

        var ctx = engine.CreateSession();
        var emitted = new List<ColumnarBatch>();
        JoinExecutionEngine.ExecuteStreaming(
            joinPlan,
            left,
            right,
            ctx,
            emitted.Add,
            chunkRows);

        var expectedRows = rowsPerSide * rowsPerSide;
        Assert.Equal(expectedRows, emitted.Sum(b => b.RowCount));
        Assert.True(emitted.Count > 1, "Expected chunked join output to span multiple batches.");
    }

    [Fact]
    public async Task Grouped_join_pipelines_without_materializing_full_join_rowset()
    {
        var engine = RainDbEngine.CreateDefault();
        var leftSchema = new TableSchema([
            new ColumnDef("id", RainDbType.Int32),
            new ColumnDef("tag", RainDbType.Utf8),
        ]);
        var rightSchema = new TableSchema([
            new ColumnDef("id", RainDbType.Int32),
            new ColumnDef("amt", RainDbType.Int64),
        ]);
        var left = new MemoryTable("L", leftSchema);
        var right = new MemoryTable("R", rightSchema);

        left.AppendBatch(new ColumnarBatch(2, new IColumnChunk[]
        {
            Int32Chunk([1, 2]),
            Utf8Chunk("east", "west"),
        }));

        var rid = new byte[12];
        BinaryPrimitives.WriteInt32LittleEndian(rid.AsSpan(0, 4), 1);
        BinaryPrimitives.WriteInt32LittleEndian(rid.AsSpan(4, 4), 1);
        BinaryPrimitives.WriteInt32LittleEndian(rid.AsSpan(8, 4), 2);
        var amts = new byte[24];
        BinaryPrimitives.WriteInt64LittleEndian(amts.AsSpan(0, 8), 10);
        BinaryPrimitives.WriteInt64LittleEndian(amts.AsSpan(8, 8), 20);
        BinaryPrimitives.WriteInt64LittleEndian(amts.AsSpan(16, 8), 5);
        right.AppendBatch(new ColumnarBatch(3, new IColumnChunk[]
        {
            new FixedWidthColumnChunk(RainDbType.Int32, 3, rid, ReadOnlyMemory<byte>.Empty, false),
            new FixedWidthColumnChunk(RainDbType.Int64, 3, amts, ReadOnlyMemory<byte>.Empty, false),
        }));

        engine.Catalog.Register(left);
        engine.Catalog.Register(right);

        const string sql =
            "SELECT L.id, L.tag, SUM(R.amt) FROM L INNER JOIN R ON L.id = R.id GROUP BY L.id, L.tag";
        var grouped = Assert.IsType<GroupedJoinPhysicalPlan>(
            StrictSqlSubset.CompilePhysicalPlan(sql, engine.Catalog));

        await using var direct = await GroupedJoinExecutionEngine.ExecuteAsync(
            grouped,
            left,
            right,
            engine.CreateSession());
        await using var sqlResult = await engine.ExecuteSqlAsync(sql);

        var directCol = Assert.IsAssignableFrom<IColumnarQueryResult>(direct);
        var sqlCol = Assert.IsAssignableFrom<IColumnarQueryResult>(sqlResult);
        Assert.Equal(sqlCol.RowCount, directCol.RowCount);
        Assert.Equal(2, directCol.RowCount);

        long directSum = 0;
        long sqlSum = 0;
        for (var row = 0; row < directCol.Batches[0].RowCount; row++)
        {
            directSum += BinaryPrimitives.ReadInt64LittleEndian(
                directCol.Batches[0].Columns[2].Values.Span.Slice(row * sizeof(long), sizeof(long)));
            sqlSum += BinaryPrimitives.ReadInt64LittleEndian(
                sqlCol.Batches[0].Columns[2].Values.Span.Slice(row * sizeof(long), sizeof(long)));
        }

        Assert.Equal(35L, directSum);
        Assert.Equal(directSum, sqlSum);
    }

    [Fact]
    public async Task Grouped_join_empty_join_produces_empty_aggregate_result()
    {
        var engine = RainDbEngine.CreateDefault();
        var leftSchema = new TableSchema([new ColumnDef("id", RainDbType.Int32)]);
        var rightSchema = new TableSchema([new ColumnDef("id", RainDbType.Int32)]);
        var left = new MemoryTable("L", leftSchema);
        var right = new MemoryTable("R", rightSchema);
        left.AppendBatch(Int32Batch(2, 1, 2));
        right.AppendBatch(Int32Batch(2, 3, 4));
        engine.Catalog.Register(left);
        engine.Catalog.Register(right);

        await using var r = await engine.ExecuteSqlAsync(
            "SELECT L.id, COUNT(*) FROM L INNER JOIN R ON L.id = R.id GROUP BY L.id");
        var col = Assert.IsAssignableFrom<IColumnarQueryResult>(r);
        Assert.Equal(0, col.RowCount);
    }

    [Fact]
    public void Join_match_chunk_emitter_default_size_matches_engine_default()
    {
        Assert.Equal(JoinMatchChunkEmitter.DefaultChunkRowCount, 8192);
    }

    private static ColumnarBatch Int32Batch(int count, int value)
    {
        var bytes = new byte[count * sizeof(int)];
        for (var i = 0; i < count; i++)
            BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(i * sizeof(int)), value);
        return new ColumnarBatch(count, new IColumnChunk[]
        {
            new FixedWidthColumnChunk(RainDbType.Int32, count, bytes, ReadOnlyMemory<byte>.Empty, false),
        });
    }

    private static ColumnarBatch Int32Batch(int count, params int[] values)
    {
        var bytes = new byte[count * sizeof(int)];
        for (var i = 0; i < count; i++)
            BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(i * sizeof(int)), values[i]);
        return new ColumnarBatch(count, new IColumnChunk[]
        {
            new FixedWidthColumnChunk(RainDbType.Int32, count, bytes, ReadOnlyMemory<byte>.Empty, false),
        });
    }

    private static FixedWidthColumnChunk Int32Chunk(int[] values)
    {
        var bytes = new byte[values.Length * sizeof(int)];
        for (var i = 0; i < values.Length; i++)
            BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(i * sizeof(int)), values[i]);
        return new FixedWidthColumnChunk(RainDbType.Int32, values.Length, bytes, ReadOnlyMemory<byte>.Empty, false);
    }

    private static Utf8ColumnChunk Utf8Chunk(params string[] rows)
    {
        var offsets = new int[rows.Length + 1];
        var blob = new List<byte>();
        for (var i = 0; i < rows.Length; i++)
        {
            offsets[i] = blob.Count;
            foreach (var b in System.Text.Encoding.UTF8.GetBytes(rows[i]))
                blob.Add(b);
        }

        offsets[^1] = blob.Count;
        return new Utf8ColumnChunk(rows.Length, offsets, blob.ToArray(), ReadOnlyMemory<byte>.Empty, false);
    }
}
