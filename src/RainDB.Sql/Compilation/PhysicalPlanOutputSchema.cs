using RainDB.Catalog;
using RainDB.Core.Columnar;
using RainDB.Execution;
using RainDB.Query.Plans;
using RainDB.Schema;
using RainDB.Sql;

namespace RainDB.Sql.Compilation;

/// <summary>Resolves the row shape produced by a physical plan root (for UNION ALL compatibility checks).</summary>
internal static class PhysicalPlanOutputSchema
{
    internal static TableSchema Resolve(IPhysicalPlan plan, ICatalog catalog)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(catalog);
        return plan switch
        {
            UnionAllPhysicalPlan u => u.OutputSchema,
            DistinctPhysicalPlan d => d.OutputSchema,
            GroupedSortTopNPhysicalPlan g => g.OutputSchema,
            JoinPhysicalPlan j => j.OutputSchema,
            JoinSortTopNPhysicalPlan jst => jst.Join.OutputSchema,
            GroupedJoinPhysicalPlan => throw new SqlCompileException("UNION ALL cannot include grouped join queries."),
            VectorizedScanPhysicalPlan v => ResolveScan(v, catalog),
            SortTopNPhysicalPlan st => ResolveScanTable(st.TableId, st.OutputColumnIndices, catalog),
            HashAggregatePhysicalPlan => throw new SqlCompileException("UNION ALL cannot include GROUP BY queries."),
            DerivedTableScanPhysicalPlan d => d.DerivedSchema,
            _ => throw new SqlCompileException($"Physical plan type '{plan.GetType().Name}' is not supported in UNION ALL."),
        };
    }

    internal static void AssertCompatible(TableSchema expected, TableSchema actual)
    {
        if (expected.Columns.Count != actual.Columns.Count)
            throw new SqlCompileException(
                $"UNION ALL column count mismatch: first branch has {expected.Columns.Count} columns, later branch has {actual.Columns.Count}.");

        for (var i = 0; i < expected.Columns.Count; i++)
        {
            if (expected.Columns[i].Type != actual.Columns[i].Type)
            {
                throw new SqlCompileException(
                    $"UNION ALL column {i + 1} type mismatch: {expected.Columns[i].Type} vs {actual.Columns[i].Type}.");
            }
        }
    }

    private static TableSchema ResolveScan(VectorizedScanPhysicalPlan plan, ICatalog catalog)
    {
        if (!catalog.TryGetTable(plan.TableId, out var ts) || ts is not IColumnarTableSource col)
            throw new SqlCompileException("UNION ALL branch table was not found in the catalog.");

        if (plan.Aggregate is { } agg)
        {
            var srcType = col.Schema.Columns[agg.SourceColumnIndex].Type;
            var resultType = agg.Kind switch
            {
                AggregateKind.Count => RainDbType.Int64,
                AggregateKind.Sum when srcType == RainDbType.Float64 => RainDbType.Float64,
                AggregateKind.Sum => RainDbType.Int64,
                AggregateKind.Min or AggregateKind.Max => srcType,
                _ => throw new SqlCompileException($"Aggregate {agg.Kind} is not supported in UNION ALL branch."),
            };
            return new TableSchema([new ColumnDef("agg", resultType)]);
        }

        var cols = new ColumnDef[plan.OutputColumns.Length];
        for (var i = 0; i < plan.OutputColumns.Length; i++)
        {
            var slot = plan.OutputColumns[i];
            if (slot.Int32Expression is not null)
            {
                cols[i] = new ColumnDef($"expr{i}", RainDbType.Int32);
                continue;
            }

            if (slot.Float64Expression is not null)
            {
                cols[i] = new ColumnDef($"expr{i}", RainDbType.Float64);
                continue;
            }

            cols[i] = col.Schema.Columns[slot.ColumnIndex];
        }

        return new TableSchema(cols);
    }

    private static TableSchema ResolveScanTable(TableId tableId, int[] outputColumnIndices, ICatalog catalog)
    {
        if (!catalog.TryGetTable(tableId, out var ts) || ts is not IColumnarTableSource col)
            throw new SqlCompileException("UNION ALL branch table was not found in the catalog.");
        var cols = new ColumnDef[outputColumnIndices.Length];
        for (var i = 0; i < outputColumnIndices.Length; i++)
            cols[i] = col.Schema.Columns[outputColumnIndices[i]];
        return new TableSchema(cols);
    }
}
