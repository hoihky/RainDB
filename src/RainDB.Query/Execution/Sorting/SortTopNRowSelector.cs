using RainDB.Columnar;
using RainDB.Query.Plans;
using RainDB.Query.Vectorized;
using RainDB.Schema;

namespace RainDB.Query.Execution.Sorting;

/// <summary>Chooses full sort vs bounded heap top-k for ORDER BY / LIMIT.</summary>
internal sealed class SortTopNRowSelector
{
    private readonly SelectionEvaluator _selection;
    private readonly BoundedTopKHeap _heap = new();

    public SortTopNRowSelector(SelectionEvaluator selection) =>
        _selection = selection ?? throw new ArgumentNullException(nameof(selection));

    public RowLocation[] SelectInSortOrder(
        RowLocation[] rows,
        SortKeyPhysicalSpec[] sortKeys,
        int? limit,
        TableSchema schema,
        IReadOnlyList<IColumnarBatch> batches)
    {
        ArgumentNullException.ThrowIfNull(rows);
        ArgumentNullException.ThrowIfNull(sortKeys);
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(batches);

        if (rows.Length == 0)
            return rows;

        if (sortKeys.Length == 0)
            return TruncateWithoutSort(rows, limit);

        var comparer = new SchemaRowLocationComparer(schema, sortKeys, batches, _selection);

        if (limit is not { } k)
        {
            Array.Sort(rows, comparer);
            return rows;
        }

        k = Math.Min(k, rows.Length);
        if (k == rows.Length)
        {
            Array.Sort(rows, comparer);
            return rows;
        }

        var top = rows.Length > k
            ? _heap.Select(rows, k, comparer)
            : rows;

        Array.Sort(top, comparer);
        return top;
    }

    private static RowLocation[] TruncateWithoutSort(RowLocation[] rows, int? limit)
    {
        if (limit is not { } k)
            return rows;
        k = Math.Min(k, rows.Length);
        if (k == rows.Length)
            return rows;
        var slice = new RowLocation[k];
        Array.Copy(rows, slice, k);
        return slice;
    }
}
