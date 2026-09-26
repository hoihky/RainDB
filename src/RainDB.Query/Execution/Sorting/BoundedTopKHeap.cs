namespace RainDB.Query.Execution.Sorting;

/// <summary>
/// Retains the <paramref name="k"/> best rows per an <see cref="IComparer{T}"/> (sort-order) using a bounded max-heap.
/// Memory is O(k); does not sort the result — callers sort the returned span for output order.
/// </summary>
internal static class BoundedTopKHeap
{
    /// <summary>
    /// Returns exactly <paramref name="k"/> row locations that belong in the global top-k (best k per comparer).
    /// </summary>
    public static RowLocation[] Select(ReadOnlySpan<RowLocation> candidates, int k, IComparer<RowLocation> comparer)
    {
        if (k <= 0)
            throw new ArgumentOutOfRangeException(nameof(k));
        if (candidates.Length <= k)
            throw new ArgumentException("Candidate count must exceed k for heap selection.", nameof(candidates));

        var heap = new RowLocation[k];
        var count = 0;

        foreach (var row in candidates)
        {
            if (count < k)
            {
                heap[count] = row;
                count++;
                if (count == k)
                    BuildMaxHeap(heap, comparer);
                continue;
            }

            if (comparer.Compare(row, heap[0]) >= 0)
                continue;

            heap[0] = row;
            SiftDownMaxHeap(heap, 0, comparer);
        }

        if (count < k)
        {
            var trimmed = new RowLocation[count];
            Array.Copy(heap, trimmed, count);
            return trimmed;
        }

        return heap;
    }

    private static void BuildMaxHeap(RowLocation[] heap, IComparer<RowLocation> comparer)
    {
        for (var i = heap.Length / 2 - 1; i >= 0; i--)
            SiftDownMaxHeap(heap, i, comparer);
    }

    /// <summary>Max-heap on sort order: root is the worst among retained rows (largest per comparer).</summary>
    private static void SiftDownMaxHeap(RowLocation[] heap, int start, IComparer<RowLocation> comparer)
    {
        var n = heap.Length;
        var i = start;
        while (true)
        {
            var worst = i;
            var left = (i * 2) + 1;
            var right = left + 1;

            if (left < n && comparer.Compare(heap[left], heap[worst]) > 0)
                worst = left;
            if (right < n && comparer.Compare(heap[right], heap[worst]) > 0)
                worst = right;
            if (worst == i)
                return;

            (heap[i], heap[worst]) = (heap[worst], heap[i]);
            i = worst;
        }
    }
}
