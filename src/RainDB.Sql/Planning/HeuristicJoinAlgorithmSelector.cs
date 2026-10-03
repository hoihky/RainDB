using RainDB.Catalog;
using RainDB.Execution;
using RainDB.Logical;
using RainDB.Query.Plans;

namespace RainDB.Sql.Planning;

/// <summary>Row-count and key-shape heuristics for hash vs sort-merge join.</summary>
public sealed class HeuristicJoinAlgorithmSelector : IJoinAlgorithmSelector
{
    public PhysicalJoinAlgorithm Select(LogicalInnerJoin join, ICatalog catalog, PhysicalPlanningOptions options)
    {
        ArgumentNullException.ThrowIfNull(join);
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentNullException.ThrowIfNull(options);

        if (options.ForcedJoinAlgorithm is { } forced)
            return forced;

        return options.JoinPreference switch
        {
            JoinAlgorithmPreference.PreferHash => PhysicalJoinAlgorithm.Hash,
            JoinAlgorithmPreference.PreferSortMerge => PhysicalJoinAlgorithm.SortMerge,
            _ => SelectAuto(join, catalog),
        };
    }

    private static PhysicalJoinAlgorithm SelectAuto(LogicalInnerJoin join, ICatalog catalog)
    {
        if (!catalog.TryGetTable(join.LeftTableName, out var left) || left is null)
            return PhysicalJoinAlgorithm.Hash;
        if (!catalog.TryGetTable(join.RightTableName, out var right) || right is null)
            return PhysicalJoinAlgorithm.Hash;

        var leftRows = EstimateRowCount(left);
        var rightRows = EstimateRowCount(right);
        if (leftRows == 0 || rightRows == 0)
            return PhysicalJoinAlgorithm.Hash;

        var ratio = (double)Math.Max(leftRows, rightRows) / Math.Max(1, Math.Min(leftRows, rightRows));
        if (ratio <= 4.0 && join.LeftKeyColumns.Count == 1)
            return PhysicalJoinAlgorithm.SortMerge;

        return PhysicalJoinAlgorithm.Hash;
    }

    private static long EstimateRowCount(ITableSource table)
    {
        if (table is not IColumnarTableSource col)
            return 0;
        long rows = 0;
        foreach (var batch in col.Batches)
            rows += batch.RowCount;
        return rows;
    }
}
