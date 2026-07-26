using System.Buffers.Binary;
using RainDB;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Core.Tables;
using RainDB.Execution;
using RainDB.Catalog;
using RainDB.Query.Plans;
using RainDB.Schema;

namespace RainDB.Tests;

/// <summary>Physical-plan execution paths not always covered by SQL integration tests.</summary>
public class PhysicalPlanCorrectnessTests
{
    [Fact]
    public async Task Hash_join_plan_matches_sort_merge_for_equi_keys()
    {
        var engine = RainDbEngine.CreateDefault();
        var (left, right) = JoinPairTables();
        engine.Catalog.Register(left);
        engine.Catalog.Register(right);

        var hashPlan = new JoinPhysicalPlan(
            PhysicalJoinAlgorithm.Hash,
            left.Id,
            right.Id,
            [0],
            [0],
            JoinOutputSchema(left, right),
            outputColumnOrder: null,
            probeSideFilters: null,
            buildSideFilters: null);

        var mergePlan = new JoinPhysicalPlan(
            PhysicalJoinAlgorithm.SortMerge,
            left.Id,
            right.Id,
            [0],
            [0],
            JoinOutputSchema(left, right),
            outputColumnOrder: null,
            probeSideFilters: null,
            buildSideFilters: null);

        await using var hashRes = await engine.ExecutePhysicalAsync(hashPlan);
        await using var mergeRes = await engine.ExecutePhysicalAsync(mergePlan);
        var hashCol = Assert.IsAssignableFrom<IColumnarQueryResult>(hashRes);
        var mergeCol = Assert.IsAssignableFrom<IColumnarQueryResult>(mergeRes);
        Assert.Equal(hashCol.RowCount, mergeCol.RowCount);
        Assert.Equal(hashCol.Batches[0].RowCount, mergeCol.Batches[0].RowCount);
    }

    [Fact]
    public async Task Global_count_star_with_where_uses_filtered_row_count()
    {
        var engine = RainDbEngine.CreateDefault();
        var t = new MemoryTable("t", new TableSchema([new ColumnDef("x", RainDbType.Int32)]));
        var bytes = new byte[8];
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(0, 4), 1);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(4, 4), 2);
        t.AppendBatch(new ColumnarBatch(2, [
            new FixedWidthColumnChunk(RainDbType.Int32, 2, bytes, ReadOnlyMemory<byte>.Empty, false),
        ]));
        engine.Catalog.Register(t);

        var plan = new VectorizedScanPhysicalPlan(
            t.Id,
            outputColumnIndices: [0],
            filters: [new ColumnCompareFilter(0, ScalarCompareOp.Eq, 2)],
            aggregate: new AggregateSpec(-1, AggregateKind.Count),
            options: new VectorizedScanExecutionOptions { MaxDegreeOfParallelism = 1 });

        await using var r = await engine.ExecutePhysicalAsync(plan);
        var agg = Assert.IsAssignableFrom<IAggregateQueryResult>(r);
        Assert.Equal(1L, agg.Int64Value);
    }

    private static (MemoryTable left, MemoryTable right) JoinPairTables()
    {
        var left = new MemoryTable("L", new TableSchema([new ColumnDef("id", RainDbType.Int32)]));
        var right = new MemoryTable("R", new TableSchema([new ColumnDef("id", RainDbType.Int32)]));
        var lid = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(lid, 7);
        var rid = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(rid, 7);
        left.AppendBatch(new ColumnarBatch(1, [new FixedWidthColumnChunk(RainDbType.Int32, 1, lid, ReadOnlyMemory<byte>.Empty, false)]));
        right.AppendBatch(new ColumnarBatch(1, [new FixedWidthColumnChunk(RainDbType.Int32, 1, rid, ReadOnlyMemory<byte>.Empty, false)]));
        return (left, right);
    }

    private static TableSchema JoinOutputSchema(MemoryTable left, MemoryTable right)
    {
        var cols = new List<ColumnDef>();
        foreach (var c in left.Schema.Columns)
            cols.Add(new ColumnDef($"{left.Name}_{c.Name}", c.Type));
        foreach (var c in right.Schema.Columns)
            cols.Add(new ColumnDef($"{right.Name}_{c.Name}", c.Type));
        return new TableSchema(cols);
    }
}
