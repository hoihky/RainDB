using System.Buffers;
using System.Threading.Channels;
using RainDB.Catalog;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Execution;
using RainDB.Query.Plans;
using RainDB.Query.Results;
using RainDB.Query.Vectorized;
using RainDB.Schema;

namespace RainDB.Query.Execution;

/// <summary>Phase 1 vectorized scan / filter / project / aggregate with morsel parallelism.</summary>
public static class VectorizedScanEngine
{
    public static async ValueTask<IQueryResult> ExecuteAsync(
        VectorizedScanPhysicalPlan plan,
        IColumnarTableSource table,
        IExecutionContext context)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(context);
        ValidatePlan(plan, table);

        if (plan.Aggregate is { } agg)
            return await ComputeAggregateAsync(plan, table, agg, context).ConfigureAwait(false);

        var batches = await ProjectAllBatchesAsync(plan, table, context).ConfigureAwait(false);
        return new ColumnarMaterializedQueryResult(batches);
    }

    private static void ValidatePlan(VectorizedScanPhysicalPlan plan, IColumnarTableSource table)
    {
        if (plan.TableId != table.Id)
            throw new ArgumentException("Physical plan table id does not match resolved table.", nameof(table));

        var colCount = table.Schema.Columns.Count;
        foreach (var idx in plan.OutputColumnIndices)
        {
            if ((uint)idx >= (uint)colCount)
                throw new ArgumentException($"Output column index {idx} is out of range.", nameof(plan));
        }

        if (plan.Filters is { } fa)
        {
            foreach (var f in fa)
            {
                if ((uint)f.ColumnIndex >= (uint)colCount)
                    throw new ArgumentException("Filter column index is out of range.", nameof(plan));
            }
        }

        if (plan.Aggregate is { } a)
        {
            if (a.SourceColumnIndex < -1 || a.SourceColumnIndex >= colCount)
                throw new ArgumentException("Aggregate column index is out of range.", nameof(plan));
            if (a.SourceColumnIndex < 0)
            {
                if (a.Kind != AggregateKind.Count)
                    throw new ArgumentException("Negative aggregate column index is only valid for COUNT(*).", nameof(plan));
            }
            else
            {
                var t = table.Schema.Columns[a.SourceColumnIndex].Type;
                switch (a.Kind)
                {
                    case AggregateKind.Sum when t is RainDbType.Int32 or RainDbType.Int64 or RainDbType.Float64:
                        break;
                    case AggregateKind.Min or AggregateKind.Max when t == RainDbType.Float64:
                        break;
                    case AggregateKind.Count:
                        break;
                    default:
                        throw new NotSupportedException($"Aggregate {a.Kind} on {t} is not supported in Phase 1.");
                }
            }
        }
    }

    private static async ValueTask<IReadOnlyList<IColumnarBatch>> ProjectAllBatchesAsync(
        VectorizedScanPhysicalPlan plan,
        IColumnarTableSource table,
        IExecutionContext context)
    {
        var batches = table.Batches;
        var n = batches.Count;
        if (n == 0)
            return Array.Empty<IColumnarBatch>();

        var dop = EffectiveDop(plan.Options.MaxDegreeOfParallelism);
        var outArr = new IColumnarBatch[n];
        var ct = context.CancellationToken;
        if (dop <= 1 || n == 1)
        {
            for (var i = 0; i < n; i++)
                outArr[i] = ProcessOneBatch(plan, batches[i], ct);
        }
        else if (plan.Options.UseChannelScheduler)
        {
            await RunChannelMorselsAsync(
                n,
                dop,
                i => outArr[i] = ProcessOneBatch(plan, batches[i], ct),
                ct).ConfigureAwait(false);
        }
        else
        {
            Parallel.For(
                0,
                n,
                new ParallelOptions { MaxDegreeOfParallelism = dop, CancellationToken = ct },
                i => outArr[i] = ProcessOneBatch(plan, batches[i], ct));
        }

        return outArr;
    }

    private static async ValueTask RunChannelMorselsAsync(
        int batchCount,
        int dop,
        Action<int> body,
        CancellationToken cancellationToken)
    {
        var ch = Channel.CreateBounded<int>(
            new BoundedChannelOptions(Math.Max(16, dop * 4))
            {
                SingleWriter = true,
                SingleReader = false,
                FullMode = BoundedChannelFullMode.Wait,
            });
        var workers = new Task[dop];
        for (var w = 0; w < dop; w++)
        {
            workers[w] = Task.Run(
                async () =>
                {
                    while (await ch.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        while (ch.Reader.TryRead(out var idx))
                            body(idx);
                    }
                },
                cancellationToken);
        }

        for (var i = 0; i < batchCount; i++)
            await ch.Writer.WriteAsync(i, cancellationToken).ConfigureAwait(false);

        ch.Writer.Complete();
        await Task.WhenAll(workers).ConfigureAwait(false);
    }

    private static IColumnarBatch ProcessOneBatch(
        VectorizedScanPhysicalPlan plan,
        IColumnarBatch batch,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var rent = ArrayPool<int>.Shared.Rent(batch.RowCount);
        try
        {
            var span = rent.AsSpan(0, batch.RowCount);
            int selected;
            if (plan.Filters is { Length: > 0 } filters)
                selected = SelectionEvaluator.FillSelectedRowsConjunctive(batch, filters, span);
            else
                selected = batch.RowCount;

            return ProjectGather.Project(
                batch,
                plan.OutputColumnIndices.AsSpan(),
                useRowSelection: plan.Filters is { Length: > 0 },
                selectedRows: plan.Filters is { Length: > 0 } ? span[..selected] : ReadOnlySpan<int>.Empty,
                selected);
        }
        finally
        {
            ArrayPool<int>.Shared.Return(rent);
        }
    }

    private static async ValueTask<IAggregateQueryResult> ComputeAggregateAsync(
        VectorizedScanPhysicalPlan plan,
        IColumnarTableSource table,
        AggregateSpec spec,
        IExecutionContext context)
    {
        var batches = table.Batches;
        var n = batches.Count;
        var ct = context.CancellationToken;
        if (n == 0)
            return EmptyAggregate(spec, table.Schema);

        var dop = EffectiveDop(plan.Options.MaxDegreeOfParallelism);
        var partials = new PartialAgg[n];
        if (dop <= 1 || n == 1)
        {
            for (var i = 0; i < n; i++)
                partials[i] = AccumulateAggregateBatch(plan, batches[i], spec, plan.Options);
        }
        else if (plan.Options.UseChannelScheduler)
        {
            await RunChannelMorselsAsync(
                n,
                dop,
                i => partials[i] = AccumulateAggregateBatch(plan, batches[i], spec, plan.Options),
                ct).ConfigureAwait(false);
        }
        else
        {
            Parallel.For(
                0,
                n,
                new ParallelOptions { MaxDegreeOfParallelism = dop, CancellationToken = ct },
                i => partials[i] = AccumulateAggregateBatch(plan, batches[i], spec, plan.Options));
        }

        var combined = partials[0];
        for (var i = 1; i < n; i++)
            combined = PartialAgg.Combine(combined, partials[i], spec.Kind);

        var resultType = spec.SourceColumnIndex >= 0 ? table.Schema.Columns[spec.SourceColumnIndex].Type : RainDbType.Int64;
        return combined.ToResult(resultType, spec.Kind);
    }

    private static IAggregateQueryResult EmptyAggregate(AggregateSpec spec, TableSchema schema)
    {
        var columnType = spec.SourceColumnIndex >= 0 ? schema.Columns[spec.SourceColumnIndex].Type : RainDbType.Int64;
        return spec.Kind switch
        {
            AggregateKind.Count => new AggregateQueryResult(RainDbType.Int64, 0d, 0L, 0, valueIsNull: false),
            AggregateKind.Sum when columnType == RainDbType.Float64 =>
                new AggregateQueryResult(RainDbType.Float64, 0d, 0L, 0, valueIsNull: true),
            AggregateKind.Sum => new AggregateQueryResult(RainDbType.Int64, 0d, 0L, 0, valueIsNull: true),
            AggregateKind.Min or AggregateKind.Max when columnType == RainDbType.Float64 =>
                new AggregateQueryResult(RainDbType.Float64, 0d, 0L, 0, valueIsNull: true),
            _ => throw new InvalidOperationException("Unsupported empty aggregate."),
        };
    }

    private static int EffectiveDop(int maxDegreeOfParallelism) =>
        maxDegreeOfParallelism < 0 ? Environment.ProcessorCount : maxDegreeOfParallelism == 0 ? 1 : maxDegreeOfParallelism;

    private static PartialAgg AccumulateAggregateBatch(
        VectorizedScanPhysicalPlan plan,
        IColumnarBatch batch,
        AggregateSpec spec,
        VectorizedScanExecutionOptions options)
    {
        if (spec.Kind == AggregateKind.Count && spec.SourceColumnIndex < 0)
            return AccumulateFilteredRowCount(plan, batch);
        if (spec.Kind == AggregateKind.Count)
        {
            var col = batch.Columns[spec.SourceColumnIndex];
            var rent = ArrayPool<int>.Shared.Rent(batch.RowCount);
            try
            {
                ReadOnlySpan<int> sel;
                int k;
                if (plan.Filters is { Length: > 0 } filters)
                {
                    k = SelectionEvaluator.FillSelectedRowsConjunctive(batch, filters, rent.AsSpan(0, batch.RowCount));
                    sel = rent.AsSpan(0, k);
                }
                else
                {
                    k = batch.RowCount;
                    sel = ReadOnlySpan<int>.Empty;
                }

                return PartialAgg.FromCountColumn(col, sel, k);
            }
            finally
            {
                ArrayPool<int>.Shared.Return(rent);
            }
        }

        var measureCol = batch.Columns[spec.SourceColumnIndex];
        var rent2 = ArrayPool<int>.Shared.Rent(batch.RowCount);
        try
        {
            ReadOnlySpan<int> sel;
            int k;
            if (plan.Filters is { Length: > 0 } filters)
            {
                k = SelectionEvaluator.FillSelectedRowsConjunctive(batch, filters, rent2.AsSpan(0, batch.RowCount));
                sel = rent2.AsSpan(0, k);
            }
            else
            {
                k = batch.RowCount;
                sel = ReadOnlySpan<int>.Empty;
            }

            return PartialAgg.FromColumn(measureCol, spec.Kind, sel, k, options);
        }
        finally
        {
            ArrayPool<int>.Shared.Return(rent2);
        }
    }

    private static PartialAgg AccumulateFilteredRowCount(VectorizedScanPhysicalPlan plan, IColumnarBatch batch)
    {
        var rent = ArrayPool<int>.Shared.Rent(batch.RowCount);
        try
        {
            int k;
            if (plan.Filters is { Length: > 0 } filters)
                k = SelectionEvaluator.FillSelectedRowsConjunctive(batch, filters, rent.AsSpan(0, batch.RowCount));
            else
                k = batch.RowCount;

            return PartialAgg.FromCountStar(k);
        }
        finally
        {
            ArrayPool<int>.Shared.Return(rent);
        }
    }

    private readonly struct PartialAgg
    {
        public readonly long ContributingRows;
        public readonly double FloatSum;
        public readonly double FloatMin;
        public readonly double FloatMax;
        public readonly long IntSum;
        public readonly bool HasMin;
        public readonly bool HasMax;
        public readonly long CountAgg;

        private PartialAgg(
            long contributingRows,
            double floatSum,
            double floatMin,
            double floatMax,
            long intSum,
            bool hasMin,
            bool hasMax,
            long countAgg = 0)
        {
            ContributingRows = contributingRows;
            FloatSum = floatSum;
            FloatMin = floatMin;
            FloatMax = floatMax;
            IntSum = intSum;
            HasMin = hasMin;
            HasMax = hasMax;
            CountAgg = countAgg;
        }

        public static PartialAgg FromCountStar(int selectedRowCount) =>
            new PartialAgg(0, 0d, 0d, 0d, 0L, false, false, selectedRowCount);

        public static PartialAgg FromCountColumn(IColumnChunk col, ReadOnlySpan<int> selectedRows, int selectedCount)
        {
            var nb = col.HasNulls ? col.NullBitmap.Span : ReadOnlySpan<byte>.Empty;
            long c = 0;
            for (var i = 0; i < selectedCount; i++)
            {
                var r = Row(selectedRows, i);
                if (!SelectionEvaluator.IsNull(nb, r, col.HasNulls))
                    c++;
            }

            return new PartialAgg(0, 0d, 0d, 0d, 0L, false, false, c);
        }

        public static PartialAgg Combine(PartialAgg a, PartialAgg b, AggregateKind kind) =>
            kind switch
            {
                AggregateKind.Sum => new PartialAgg(
                    a.ContributingRows + b.ContributingRows,
                    a.FloatSum + b.FloatSum,
                    0d,
                    0d,
                    a.IntSum + b.IntSum,
                    false,
                    false,
                    0),
                AggregateKind.Count => new PartialAgg(0, 0d, 0d, 0d, 0L, false, false, a.CountAgg + b.CountAgg),
                AggregateKind.Min => CombineMinMax(a, b, isMin: true),
                AggregateKind.Max => CombineMinMax(a, b, isMin: false),
                _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null),
            };

        private static PartialAgg CombineMinMax(PartialAgg a, PartialAgg b, bool isMin)
        {
            long rows = a.ContributingRows + b.ContributingRows;
            if (!a.HasMin && !b.HasMin && !a.HasMax && !b.HasMax)
                return new PartialAgg(0, 0d, 0d, 0d, 0L, false, false, 0);

            if (isMin)
            {
                if (!a.HasMin)
                    return b;
                if (!b.HasMin)
                    return a;
                return new PartialAgg(rows, 0d, Math.Min(a.FloatMin, b.FloatMin), 0d, 0L, true, false, 0);
            }

            if (!a.HasMax)
                return b;
            if (!b.HasMax)
                return a;
            return new PartialAgg(rows, 0d, 0d, Math.Max(a.FloatMax, b.FloatMax), 0L, false, true, 0);
        }

        public static PartialAgg FromColumn(
            IColumnChunk col,
            AggregateKind kind,
            ReadOnlySpan<int> selectedRows,
            int selectedCount,
            VectorizedScanExecutionOptions options)
        {
            var nb = col.HasNulls ? col.NullBitmap.Span : ReadOnlySpan<byte>.Empty;
            var values = col.Values.Span;
            return kind switch
            {
                AggregateKind.Sum when col.PhysicalType == RainDbType.Float64 => SumFloat64(col, selectedRows, selectedCount, nb, values, options),
                AggregateKind.Sum when col.PhysicalType == RainDbType.Int32 => SumInt32(selectedRows, selectedCount, col.HasNulls, nb, values),
                AggregateKind.Sum when col.PhysicalType == RainDbType.Int64 => SumInt64(selectedRows, selectedCount, col.HasNulls, nb, values),
                AggregateKind.Min or AggregateKind.Max when col.PhysicalType == RainDbType.Float64 =>
                    MinMaxFloat64(kind, selectedRows, selectedCount, col.HasNulls, nb, values),
                _ => throw new NotSupportedException($"Aggregate on {col.PhysicalType} is not supported."),
            };
        }

        private static PartialAgg SumFloat64(
            IColumnChunk col,
            ReadOnlySpan<int> selectedRows,
            int selectedCount,
            ReadOnlySpan<byte> nb,
            ReadOnlySpan<byte> values,
            VectorizedScanExecutionOptions options)
        {
            if (selectedCount == 0)
                return new PartialAgg(0, 0d, 0d, 0d, 0L, false, false, 0);

            if (selectedRows.IsEmpty && !col.HasNulls && options.UseAvx2DoubleSum)
            {
                var sum = AggregateIntrinsics.SumFloat64(values, allowAvx2: true);
                return new PartialAgg(selectedCount, sum, 0d, 0d, 0L, false, false, 0);
            }

            double s = 0;
            long contrib = 0;
            for (var i = 0; i < selectedCount; i++)
            {
                var r = Row(selectedRows, i);
                if (SelectionEvaluator.IsNull(nb, r, col.HasNulls))
                    continue;
                var bits = System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(values.Slice(r * sizeof(double), sizeof(double)));
                s += BitConverter.Int64BitsToDouble(bits);
                contrib++;
            }

            return new PartialAgg(contrib, s, 0d, 0d, 0L, false, false, 0);
        }

        private static PartialAgg SumInt32(
            ReadOnlySpan<int> selectedRows,
            int selectedCount,
            bool hasNulls,
            ReadOnlySpan<byte> nb,
            ReadOnlySpan<byte> values)
        {
            long s = 0;
            long contrib = 0;
            for (var i = 0; i < selectedCount; i++)
            {
                var r = Row(selectedRows, i);
                if (SelectionEvaluator.IsNull(nb, r, hasNulls))
                    continue;
                s += System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(values.Slice(r * sizeof(int), sizeof(int)));
                contrib++;
            }

            return new PartialAgg(contrib, 0d, 0d, 0d, s, false, false, 0);
        }

        private static PartialAgg SumInt64(
            ReadOnlySpan<int> selectedRows,
            int selectedCount,
            bool hasNulls,
            ReadOnlySpan<byte> nb,
            ReadOnlySpan<byte> values)
        {
            long s = 0;
            long contrib = 0;
            for (var i = 0; i < selectedCount; i++)
            {
                var r = Row(selectedRows, i);
                if (SelectionEvaluator.IsNull(nb, r, hasNulls))
                    continue;
                s += System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(values.Slice(r * sizeof(long), sizeof(long)));
                contrib++;
            }

            return new PartialAgg(contrib, 0d, 0d, 0d, s, false, false, 0);
        }

        private static PartialAgg MinMaxFloat64(
            AggregateKind kind,
            ReadOnlySpan<int> selectedRows,
            int selectedCount,
            bool hasNulls,
            ReadOnlySpan<byte> nb,
            ReadOnlySpan<byte> values)
        {
            double? cur = null;
            long contrib = 0;
            for (var i = 0; i < selectedCount; i++)
            {
                var r = Row(selectedRows, i);
                if (SelectionEvaluator.IsNull(nb, r, hasNulls))
                    continue;
                var bits = System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(values.Slice(r * sizeof(double), sizeof(double)));
                var v = BitConverter.Int64BitsToDouble(bits);
                cur = cur.HasValue
                    ? kind == AggregateKind.Min ? Math.Min(cur.Value, v) : Math.Max(cur.Value, v)
                    : v;
                contrib++;
            }

            if (!cur.HasValue)
                return new PartialAgg(0, 0d, 0d, 0d, 0L, false, false, 0);
            var x = cur.Value;
            return kind == AggregateKind.Min
                ? new PartialAgg(contrib, 0d, x, 0d, 0L, true, false, 0)
                : new PartialAgg(contrib, 0d, 0d, x, 0L, false, true, 0);
        }

        private static int Row(ReadOnlySpan<int> selectedRows, int i) => selectedRows.IsEmpty ? i : selectedRows[i];

        public IAggregateQueryResult ToResult(RainDbType columnType, AggregateKind kind)
        {
            return kind switch
            {
                AggregateKind.Count =>
                    new AggregateQueryResult(RainDbType.Int64, 0d, CountAgg, CountAgg, valueIsNull: false),
                AggregateKind.Sum when columnType == RainDbType.Float64 =>
                    new AggregateQueryResult(RainDbType.Float64, FloatSum, 0L, ContributingRows, valueIsNull: ContributingRows == 0),
                AggregateKind.Sum =>
                    new AggregateQueryResult(RainDbType.Int64, 0d, IntSum, ContributingRows, valueIsNull: ContributingRows == 0),
                AggregateKind.Min when columnType == RainDbType.Float64 =>
                    new AggregateQueryResult(RainDbType.Float64, HasMin ? FloatMin : 0d, 0L, ContributingRows, valueIsNull: !HasMin),
                AggregateKind.Max when columnType == RainDbType.Float64 =>
                    new AggregateQueryResult(RainDbType.Float64, HasMax ? FloatMax : 0d, 0L, ContributingRows, valueIsNull: !HasMax),
                _ => throw new InvalidOperationException("Unsupported aggregate result mapping."),
            };
        }
    }
}
