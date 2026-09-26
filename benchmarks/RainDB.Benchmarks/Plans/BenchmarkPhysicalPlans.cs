using RainDB.Core.Tables;
using RainDB.Execution;
using RainDB.Query.Plans;
using RainDB.Schema;

namespace RainDB.Benchmarks.Plans;

/// <summary>Factory for physical plans used across benchmarks and smoke tests (DRY, single place to tune plans).</summary>
public static class BenchmarkPhysicalPlans
{
    public static VectorizedScanPhysicalPlan FullTableScan(MemoryTable table) =>
        new(
            table.Id,
            outputColumnIndices: [0, 1, 2],
            filters: null,
            aggregate: null,
            options: DefaultScanOptions());

    public static VectorizedScanPhysicalPlan FilterAndProject(MemoryTable table)
    {
        var threshold = BitConverter.DoubleToInt64Bits(250.0);
        return new VectorizedScanPhysicalPlan(
            table.Id,
            outputColumnIndices: [0, 2],
            filters: [new ColumnCompareFilter(2, ScalarCompareOp.Gt, threshold)],
            aggregate: null,
            options: DefaultScanOptions());
    }

    public static HashAggregatePhysicalPlan GroupByCategorySumAmount(MemoryTable table) =>
        new(
            table.Id,
            groupKeyColumnIndices: [0],
            aggregates: [new AggregateSpec(2, AggregateKind.Sum)],
            outputColumns: null,
            filters: null,
            options: DefaultScanOptions());

    public static JoinPhysicalPlan InnerHashJoinSalesToRegion(MemoryTable fact, MemoryTable dimension)
    {
        var outputSchema = new TableSchema([
            new ColumnDef("category_id", RainDbType.Int32),
            new ColumnDef("region_id", RainDbType.Int32),
            new ColumnDef("amount", RainDbType.Float64),
            new ColumnDef("dim_region_id", RainDbType.Int32),
        ]);
        return new JoinPhysicalPlan(
            PhysicalJoinAlgorithm.Hash,
            probeTableId: fact.Id,
            buildTableId: dimension.Id,
            probeKeyColumnIndices: [1],
            buildKeyColumnIndices: [0],
            outputSchema: outputSchema,
            outputColumnOrder: null,
            probeSideFilters: null,
            buildSideFilters: null);
    }

    public static SortTopNPhysicalPlan OrderByAmountTop100(MemoryTable table) =>
        new(
            table.Id,
            outputColumnIndices: [0, 1, 2],
            filters: null,
            sortKeys: [new SortKeyPhysicalSpec(2, Descending: true)],
            limit: 100,
            options: DefaultScanOptions());

    private static VectorizedScanExecutionOptions DefaultScanOptions() =>
        new()
        {
            MaxDegreeOfParallelism = -1,
            UseChannelScheduler = false,
            UseAvx2DoubleSum = true,
        };
}
