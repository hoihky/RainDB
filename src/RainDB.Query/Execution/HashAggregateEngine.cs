using System.Buffers;
using System.Buffers.Binary;
using RainDB.Catalog;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Execution;
using RainDB.Query.Plans;
using RainDB.Query.Results;
using RainDB.Query.Vectorized;
using RainDB.Schema;

namespace RainDB.Query.Execution;

/// <summary>Parallel partial hash maps per source batch, deterministic global merge, sorted key materialization.</summary>
public static class HashAggregateEngine
{
    public static async ValueTask<IQueryResult> ExecuteAsync(
        HashAggregatePhysicalPlan plan,
        IColumnarTableSource table,
        IExecutionContext context)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(context);

        ValidatePlan(plan, table);

        if (AnyUtf8GroupKey(plan, table.Schema))
            return await ExecuteWithCompositeKeysAsync(plan, table, context).ConfigureAwait(false);

        var batches = table.Batches;
        var n = batches.Count;
        var schema = table.Schema;
        var ct = context.CancellationToken;

        if (n == 0)
        {
            var emptyCols = MaterializeEmptyOutput(plan, schema);
            return new ColumnarMaterializedQueryResult([new ColumnarBatch(0, emptyCols)]);
        }

        var dop = EffectiveDop(plan.Options.MaxDegreeOfParallelism);
        var partials = new Dictionary<GroupKey, AggregateAccumulator[]>[n];
        if (dop <= 1 || n == 1)
        {
            for (var i = 0; i < n; i++)
                partials[i] = AccumulateBatch(batches[i], plan, ct);
        }
        else if (plan.Options.UseChannelScheduler)
        {
            await RunChannelMorselsAsync(
                    n,
                    dop,
                    i => partials[i] = AccumulateBatch(batches[i], plan, ct),
                    ct)
                .ConfigureAwait(false);
        }
        else
        {
            Parallel.For(
                0,
                n,
                new ParallelOptions { MaxDegreeOfParallelism = dop, CancellationToken = ct },
                i => partials[i] = AccumulateBatch(batches[i], plan, ct));
        }

        if (context.SpillWriter.IsEnabled && plan.SpillPartialEntryThreshold > 0)
        {
            for (var i = 0; i < n; i++)
            {
                if (partials[i].Count >= plan.SpillPartialEntryThreshold)
                {
                    var payload = System.Text.Encoding.UTF8.GetBytes(
                        $"{{\"op\":\"hash_agg_partial\",\"batch\":{i},\"entries\":{partials[i].Count}}}\n");
                    await context.SpillWriter.SpillChunkAsync(payload, ct).ConfigureAwait(false);
                }
            }
        }

        var global = MergePartials(partials, plan.Aggregates);
        var sortedKeys = SortKeys(global.Keys, schema, plan.GroupKeyColumnIndices);
        var outBatch = MaterializeOutput(sortedKeys, global, plan, schema);
        return new ColumnarMaterializedQueryResult([outBatch]);
    }

    private static void ValidatePlan(HashAggregatePhysicalPlan plan, IColumnarTableSource table)
    {
        if (plan.TableId != table.Id)
            throw new ArgumentException("Physical plan table id does not match resolved table.", nameof(table));

        var colCount = table.Schema.Columns.Count;
        foreach (var idx in plan.GroupKeyColumnIndices)
        {
            if ((uint)idx >= (uint)colCount)
                throw new ArgumentException($"Group key column index {idx} is out of range.", nameof(plan));
            var kt = table.Schema.Columns[idx].Type;
            if (kt != RainDbType.Utf8 && !ColumnTypeSizes.IsFixedWidth(kt))
                throw new NotSupportedException($"Group key type {kt} is not supported.");
        }

        if (plan.Filters is { } fa)
        {
            foreach (var f in fa)
            {
                if ((uint)f.ColumnIndex >= (uint)colCount)
                    throw new ArgumentException("Filter column index is out of range.", nameof(plan));
            }
        }

        foreach (var a in plan.Aggregates)
        {
            if (a.SourceColumnIndex < -1 || a.SourceColumnIndex >= colCount)
                throw new ArgumentException($"Aggregate column index {a.SourceColumnIndex} is out of range.", nameof(plan));
            if (a.Kind == AggregateKind.Count && a.SourceColumnIndex < 0)
                continue;
            var t = table.Schema.Columns[a.SourceColumnIndex].Type;
            ValidateAggregate(t, a.Kind);
        }
    }

    private static void ValidateAggregate(RainDbType columnType, AggregateKind kind)
    {
        switch (kind)
        {
            case AggregateKind.Count:
                return;
            case AggregateKind.Sum when columnType is RainDbType.Int32 or RainDbType.Int64 or RainDbType.Float64:
                return;
            case AggregateKind.Min or AggregateKind.Max when columnType == RainDbType.Float64:
                return;
            default:
                throw new NotSupportedException($"Aggregate {kind} on {columnType} is not supported for hash aggregation.");
        }
    }

    private static Dictionary<GroupKey, AggregateAccumulator[]> AccumulateBatch(
        IColumnarBatch batch,
        HashAggregatePhysicalPlan plan,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var specs = plan.Aggregates;
        var aggCount = specs.Length;
        var dict = new Dictionary<GroupKey, AggregateAccumulator[]>();
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

            var scratch = ArrayPool<ulong>.Shared.Rent(plan.GroupKeyColumnIndices.Length);
            try
            {
                for (var i = 0; i < k; i++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var row = sel.IsEmpty ? i : sel[i];
                    var key = FixedWidthGroupKeyBuilder.BuildKey(batch, row, plan.GroupKeyColumnIndices, scratch);
                    if (!dict.TryGetValue(key, out var accs))
                    {
                        accs = new AggregateAccumulator[aggCount];
                        dict[key] = accs;
                    }

                    for (var a = 0; a < aggCount; a++)
                    {
                        ref var slot = ref accs[a];
                        var spec = specs[a];
                        if (spec.Kind == AggregateKind.Count && spec.SourceColumnIndex < 0)
                            AggregateRowOps.AddCountStar(ref slot);
                        else if (spec.Kind == AggregateKind.Count)
                            AggregateRowOps.AddCountColumn(ref slot, batch.Columns[spec.SourceColumnIndex], row);
                        else
                            AggregateRowOps.AddRow(ref slot, batch.Columns[spec.SourceColumnIndex], spec.Kind, row);
                    }
                }
            }
            finally
            {
                ArrayPool<ulong>.Shared.Return(scratch);
            }

            return dict;
        }
        finally
        {
            ArrayPool<int>.Shared.Return(rent);
        }
    }

    private static Dictionary<GroupKey, AggregateAccumulator[]> MergePartials(
        Dictionary<GroupKey, AggregateAccumulator[]>[] partials,
        AggregateSpec[] specs)
    {
        var aggCount = specs.Length;
        var global = new Dictionary<GroupKey, AggregateAccumulator[]>();
        for (var bi = 0; bi < partials.Length; bi++)
        {
            foreach (var kv in partials[bi])
            {
                if (!global.TryGetValue(kv.Key, out var merged))
                {
                    merged = new AggregateAccumulator[aggCount];
                    for (var j = 0; j < aggCount; j++)
                        merged[j] = kv.Value[j];
                    global[new GroupKey(kv.Key.Parts.ToArray(), kv.Key.NullMask)] = merged;
                }
                else
                {
                    for (var j = 0; j < aggCount; j++)
                        merged[j] = AggregateRowOps.Combine(merged[j], kv.Value[j], specs[j].Kind);
                }
            }
        }

        return global;
    }

    private static GroupKey[] SortKeys(
        Dictionary<GroupKey, AggregateAccumulator[]>.KeyCollection keys,
        TableSchema schema,
        int[] keyIndices)
    {
        var arr = new GroupKey[keys.Count];
        keys.CopyTo(arr, 0);
        Array.Sort(arr, new GroupKeyComparer(schema, keyIndices));
        return arr;
    }

    private static ColumnarBatch MaterializeOutput(
        GroupKey[] sortedKeys,
        Dictionary<GroupKey, AggregateAccumulator[]> global,
        HashAggregatePhysicalPlan plan,
        TableSchema schema)
    {
        var rowCount = sortedKeys.Length;
        var cols = new List<IColumnChunk>();
        foreach (var outSlot in plan.OutputColumns)
        {
            switch (outSlot.Kind)
            {
                case HashAggregateOutputColumnKind.GroupKey:
                {
                    var schemaCol = schema.Columns[plan.GroupKeyColumnIndices[outSlot.Ordinal]];
                    cols.Add(MaterializeKeyColumn(sortedKeys, outSlot.Ordinal, schemaCol.Type, rowCount));
                    break;
                }
                case HashAggregateOutputColumnKind.Aggregate:
                {
                    var spec = plan.Aggregates[outSlot.Ordinal];
                    cols.Add(MaterializeAggregateColumn(sortedKeys, global, outSlot.Ordinal, spec, schema, rowCount));
                    break;
                }
            }
        }

        return new ColumnarBatch(rowCount, cols);
    }

    private static IColumnChunk MaterializeKeyColumn(GroupKey[] keys, int keyPartIndex, RainDbType type, int rowCount)
    {
        var w = ColumnTypeSizes.FixedWidthBytes(type);
        var values = new byte[rowCount * w];
        var nbBytes = ColumnTypeSizes.NullBitmapBytes(rowCount);
        var nb = nbBytes > 0 ? new byte[nbBytes] : Array.Empty<byte>();
        var anyNull = false;
        for (var r = 0; r < rowCount; r++)
        {
            var isNull = (keys[r].NullMask & (1u << keyPartIndex)) != 0;
            if (isNull)
            {
                anyNull = true;
                SetNull(nb, r);
                continue;
            }

            WritePhysical(values.AsSpan(r * w, w), type, keys[r].Parts[keyPartIndex]);
        }

        return new FixedWidthColumnChunk(type, rowCount, values, nb, anyNull);
    }

    private static void WritePhysical(Span<byte> dest, RainDbType type, ulong bits)
    {
        switch (type)
        {
            case RainDbType.Int32:
                BinaryPrimitives.WriteInt32LittleEndian(dest, (int)(uint)bits);
                break;
            case RainDbType.Int64:
                BinaryPrimitives.WriteInt64LittleEndian(dest, (long)bits);
                break;
            case RainDbType.Float64:
                BinaryPrimitives.WriteInt64LittleEndian(dest, (long)bits);
                break;
            case RainDbType.Boolean:
                dest[0] = bits != 0 ? (byte)1 : (byte)0;
                break;
            default:
                throw new InvalidOperationException($"Unexpected key type {type}.");
        }
    }

    private static void SetNull(byte[] nb, int row)
    {
        var b = row >> 3;
        nb[b] |= (byte)(1 << (row & 7));
    }

    private static IColumnChunk MaterializeAggregateColumn(
        GroupKey[] sortedKeys,
        Dictionary<GroupKey, AggregateAccumulator[]> global,
        int aggIndex,
        AggregateSpec spec,
        TableSchema schema,
        int rowCount)
    {
        var resultType = AggregateResultType(spec, schema);
        var w = ColumnTypeSizes.FixedWidthBytes(resultType);
        var values = new byte[rowCount * w];
        var nbBytes = ColumnTypeSizes.NullBitmapBytes(rowCount);
        var nb = nbBytes > 0 ? new byte[nbBytes] : Array.Empty<byte>();
        var anyNull = false;
        for (var r = 0; r < rowCount; r++)
        {
            var accs = global[sortedKeys[r]];
            var acc = accs[aggIndex];
            if (ShouldEmitAggregateNull(spec, acc))
            {
                anyNull = true;
                SetNull(nb, r);
                continue;
            }

            WriteAggregateValue(values.AsSpan(r * w, w), spec, schema, acc);
        }

        var hasNulls = anyNull;
        if (spec.Kind == AggregateKind.Count)
            hasNulls = false;
        return new FixedWidthColumnChunk(resultType, rowCount, values, nb, hasNulls);
    }

    private static bool ShouldEmitAggregateNull(AggregateSpec spec, AggregateAccumulator acc)
    {
        return spec.Kind switch
        {
            AggregateKind.Sum => acc.ContributingRows == 0,
            AggregateKind.Min => !acc.HasMin,
            AggregateKind.Max => !acc.HasMax,
            AggregateKind.Count => false,
            _ => false,
        };
    }

    private static void WriteAggregateValue(Span<byte> dest, AggregateSpec spec, TableSchema schema, AggregateAccumulator acc)
    {
        var srcType = spec.SourceColumnIndex >= 0 ? schema.Columns[spec.SourceColumnIndex].Type : RainDbType.Int64;
        switch (spec.Kind)
        {
            case AggregateKind.Count:
                BinaryPrimitives.WriteInt64LittleEndian(dest, acc.Count);
                break;
            case AggregateKind.Sum when srcType == RainDbType.Float64:
                BinaryPrimitives.WriteInt64LittleEndian(dest, BitConverter.DoubleToInt64Bits(acc.FloatSum));
                break;
            case AggregateKind.Sum:
                BinaryPrimitives.WriteInt64LittleEndian(dest, acc.IntSum);
                break;
            case AggregateKind.Min:
                BinaryPrimitives.WriteInt64LittleEndian(dest, BitConverter.DoubleToInt64Bits(acc.FloatMin));
                break;
            case AggregateKind.Max:
                BinaryPrimitives.WriteInt64LittleEndian(dest, BitConverter.DoubleToInt64Bits(acc.FloatMax));
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(spec.Kind), spec.Kind, null);
        }
    }

    private static RainDbType AggregateResultType(AggregateSpec spec, TableSchema schema) =>
        spec.Kind switch
        {
            AggregateKind.Count => RainDbType.Int64,
            AggregateKind.Sum when spec.SourceColumnIndex >= 0 && schema.Columns[spec.SourceColumnIndex].Type == RainDbType.Float64 => RainDbType.Float64,
            AggregateKind.Sum => RainDbType.Int64,
            AggregateKind.Min or AggregateKind.Max => RainDbType.Float64,
            _ => throw new ArgumentOutOfRangeException(nameof(spec.Kind), spec.Kind, null),
        };

    private static IReadOnlyList<IColumnChunk> MaterializeEmptyOutput(HashAggregatePhysicalPlan plan, TableSchema schema)
    {
        var cols = new List<IColumnChunk>();
        foreach (var slot in plan.OutputColumns)
        {
            switch (slot.Kind)
            {
                case HashAggregateOutputColumnKind.GroupKey:
                {
                    var t = schema.Columns[plan.GroupKeyColumnIndices[slot.Ordinal]].Type;
                    if (t == RainDbType.Utf8)
                    {
                        cols.Add(new Utf8ColumnChunk(0, new[] { 0 }, Array.Empty<byte>(), ReadOnlyMemory<byte>.Empty, false));
                    }
                    else
                    {
                        cols.Add(new FixedWidthColumnChunk(t, 0, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, false));
                    }

                    break;
                }
                case HashAggregateOutputColumnKind.Aggregate:
                {
                    var spec = plan.Aggregates[slot.Ordinal];
                    var rt = AggregateResultType(spec, schema);
                    cols.Add(new FixedWidthColumnChunk(rt, 0, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, false));
                    break;
                }
            }
        }

        return cols;
    }

    private static bool AnyUtf8GroupKey(HashAggregatePhysicalPlan plan, TableSchema schema)
    {
        foreach (var ix in plan.GroupKeyColumnIndices)
        {
            if (schema.Columns[ix].Type == RainDbType.Utf8)
                return true;
        }

        return false;
    }

    private static async ValueTask<IQueryResult> ExecuteWithCompositeKeysAsync(
        HashAggregatePhysicalPlan plan,
        IColumnarTableSource table,
        IExecutionContext context)
    {
        var batches = table.Batches;
        var n = batches.Count;
        var schema = table.Schema;
        var ct = context.CancellationToken;

        if (n == 0)
        {
            var emptyCols = MaterializeEmptyOutput(plan, schema);
            return new ColumnarMaterializedQueryResult([new ColumnarBatch(0, emptyCols)]);
        }

        var dop = EffectiveDop(plan.Options.MaxDegreeOfParallelism);
        var partials = new Dictionary<CompositeJoinKey, AggregateAccumulator[]>[n];
        if (dop <= 1 || n == 1)
        {
            for (var i = 0; i < n; i++)
                partials[i] = AccumulateBatchComposite(batches[i], plan, schema, ct);
        }
        else if (plan.Options.UseChannelScheduler)
        {
            await RunChannelMorselsAsync(
                    n,
                    dop,
                    i => partials[i] = AccumulateBatchComposite(batches[i], plan, schema, ct),
                    ct)
                .ConfigureAwait(false);
        }
        else
        {
            Parallel.For(
                0,
                n,
                new ParallelOptions { MaxDegreeOfParallelism = dop, CancellationToken = ct },
                i => partials[i] = AccumulateBatchComposite(batches[i], plan, schema, ct));
        }

        if (context.SpillWriter.IsEnabled && plan.SpillPartialEntryThreshold > 0)
        {
            for (var i = 0; i < n; i++)
            {
                if (partials[i].Count >= plan.SpillPartialEntryThreshold)
                {
                    var payload = System.Text.Encoding.UTF8.GetBytes(
                        $"{{\"op\":\"hash_agg_partial_utf8\",\"batch\":{i},\"entries\":{partials[i].Count}}}\n");
                    await context.SpillWriter.SpillChunkAsync(payload, ct).ConfigureAwait(false);
                }
            }
        }

        var global = MergePartialsComposite(partials, plan.Aggregates);
        var sortedKeys = SortCompositeKeys(global.Keys, schema, plan.GroupKeyColumnIndices);
        var outBatch = MaterializeOutputComposite(sortedKeys, global, plan, schema);
        return new ColumnarMaterializedQueryResult([outBatch]);
    }

    private static Dictionary<CompositeJoinKey, AggregateAccumulator[]> AccumulateBatchComposite(
        IColumnarBatch batch,
        HashAggregatePhysicalPlan plan,
        TableSchema schema,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var specs = plan.Aggregates;
        var aggCount = specs.Length;
        var dict = new Dictionary<CompositeJoinKey, AggregateAccumulator[]>();
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

            for (var i = 0; i < k; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var row = sel.IsEmpty ? i : sel[i];
                var key = CompositeJoinKeyBuilder.Build(schema, batch, row, plan.GroupKeyColumnIndices);
                if (!dict.TryGetValue(key, out var accs))
                {
                    accs = new AggregateAccumulator[aggCount];
                    dict[key] = accs;
                }

                for (var a = 0; a < aggCount; a++)
                {
                    ref var slot = ref accs[a];
                    var spec = specs[a];
                    if (spec.Kind == AggregateKind.Count && spec.SourceColumnIndex < 0)
                        AggregateRowOps.AddCountStar(ref slot);
                    else if (spec.Kind == AggregateKind.Count)
                        AggregateRowOps.AddCountColumn(ref slot, batch.Columns[spec.SourceColumnIndex], row);
                    else
                        AggregateRowOps.AddRow(ref slot, batch.Columns[spec.SourceColumnIndex], spec.Kind, row);
                }
            }

            return dict;
        }
        finally
        {
            ArrayPool<int>.Shared.Return(rent);
        }
    }

    private static Dictionary<CompositeJoinKey, AggregateAccumulator[]> MergePartialsComposite(
        Dictionary<CompositeJoinKey, AggregateAccumulator[]>[] partials,
        AggregateSpec[] specs)
    {
        var aggCount = specs.Length;
        var global = new Dictionary<CompositeJoinKey, AggregateAccumulator[]>();
        for (var bi = 0; bi < partials.Length; bi++)
        {
            foreach (var kv in partials[bi])
            {
                if (!global.TryGetValue(kv.Key, out var merged))
                {
                    merged = new AggregateAccumulator[aggCount];
                    for (var j = 0; j < aggCount; j++)
                        merged[j] = kv.Value[j];
                    global[kv.Key.DeepClone()] = merged;
                }
                else
                {
                    for (var j = 0; j < aggCount; j++)
                        merged[j] = AggregateRowOps.Combine(merged[j], kv.Value[j], specs[j].Kind);
                }
            }
        }

        return global;
    }

    private static CompositeJoinKey[] SortCompositeKeys(
        Dictionary<CompositeJoinKey, AggregateAccumulator[]>.KeyCollection keys,
        TableSchema schema,
        int[] keyIndices)
    {
        var arr = new CompositeJoinKey[keys.Count];
        keys.CopyTo(arr, 0);
        Array.Sort(arr, new CompositeJoinKeyComparer(schema, keyIndices));
        return arr;
    }

    private static ColumnarBatch MaterializeOutputComposite(
        CompositeJoinKey[] sortedKeys,
        Dictionary<CompositeJoinKey, AggregateAccumulator[]> global,
        HashAggregatePhysicalPlan plan,
        TableSchema schema)
    {
        var rowCount = sortedKeys.Length;
        var cols = new List<IColumnChunk>();
        foreach (var outSlot in plan.OutputColumns)
        {
            switch (outSlot.Kind)
            {
                case HashAggregateOutputColumnKind.GroupKey:
                {
                    var schemaCol = schema.Columns[plan.GroupKeyColumnIndices[outSlot.Ordinal]];
                    cols.Add(MaterializeCompositeKeyColumn(sortedKeys, outSlot.Ordinal, schemaCol.Type, rowCount));
                    break;
                }
                case HashAggregateOutputColumnKind.Aggregate:
                {
                    var spec = plan.Aggregates[outSlot.Ordinal];
                    cols.Add(MaterializeAggregateColumnComposite(sortedKeys, global, outSlot.Ordinal, spec, schema, rowCount));
                    break;
                }
            }
        }

        return new ColumnarBatch(rowCount, cols);
    }

    private static IColumnChunk MaterializeCompositeKeyColumn(
        CompositeJoinKey[] keys,
        int keyPartIndex,
        RainDbType type,
        int rowCount)
    {
        if (type == RainDbType.Utf8)
            return MaterializeUtf8KeyColumn(keys, keyPartIndex, rowCount);

        var w = ColumnTypeSizes.FixedWidthBytes(type);
        var values = new byte[rowCount * w];
        var nbBytes = ColumnTypeSizes.NullBitmapBytes(rowCount);
        var nb = nbBytes > 0 ? new byte[nbBytes] : Array.Empty<byte>();
        var anyNull = false;
        for (var r = 0; r < rowCount; r++)
        {
            var isNull = (keys[r].NullMask & (1u << keyPartIndex)) != 0;
            if (isNull)
            {
                anyNull = true;
                SetNull(nb, r);
                continue;
            }

            WritePhysical(values.AsSpan(r * w, w), type, keys[r].NumericParts[keyPartIndex]);
        }

        return new FixedWidthColumnChunk(type, rowCount, values, nb, anyNull);
    }

    private static IColumnChunk MaterializeUtf8KeyColumn(CompositeJoinKey[] keys, int keyPartIndex, int rowCount)
    {
        var offsets = new int[rowCount + 1];
        using var blob = new MemoryStream();
        var nbBytes = ColumnTypeSizes.NullBitmapBytes(rowCount);
        var nb = nbBytes > 0 ? new byte[nbBytes] : Array.Empty<byte>();
        var anyNull = false;
        for (var r = 0; r < rowCount; r++)
        {
            offsets[r] = (int)blob.Length;
            var isNull = (keys[r].NullMask & (1u << keyPartIndex)) != 0;
            if (isNull)
            {
                anyNull = true;
                SetNull(nb, r);
                continue;
            }

            var payload = keys[r].Utf8Payloads[keyPartIndex];
            if (payload is null)
            {
                anyNull = true;
                SetNull(nb, r);
                continue;
            }

            blob.Write(payload);
        }

        offsets[rowCount] = (int)blob.Length;
        return new Utf8ColumnChunk(rowCount, offsets, blob.ToArray(), nb, anyNull);
    }

    private static IColumnChunk MaterializeAggregateColumnComposite(
        CompositeJoinKey[] sortedKeys,
        Dictionary<CompositeJoinKey, AggregateAccumulator[]> global,
        int aggIndex,
        AggregateSpec spec,
        TableSchema schema,
        int rowCount)
    {
        var resultType = AggregateResultType(spec, schema);
        var w = ColumnTypeSizes.FixedWidthBytes(resultType);
        var values = new byte[rowCount * w];
        var nbBytes = ColumnTypeSizes.NullBitmapBytes(rowCount);
        var nb = nbBytes > 0 ? new byte[nbBytes] : Array.Empty<byte>();
        var anyNull = false;
        for (var r = 0; r < rowCount; r++)
        {
            var accs = global[sortedKeys[r]];
            var acc = accs[aggIndex];
            if (ShouldEmitAggregateNull(spec, acc))
            {
                anyNull = true;
                SetNull(nb, r);
                continue;
            }

            WriteAggregateValue(values.AsSpan(r * w, w), spec, schema, acc);
        }

        var hasNulls = anyNull;
        if (spec.Kind == AggregateKind.Count)
            hasNulls = false;
        return new FixedWidthColumnChunk(resultType, rowCount, values, nb, hasNulls);
    }

    private static int EffectiveDop(int maxDegreeOfParallelism) =>
        maxDegreeOfParallelism < 0 ? Environment.ProcessorCount : maxDegreeOfParallelism == 0 ? 1 : maxDegreeOfParallelism;

    private static async Task RunChannelMorselsAsync(
        int batchCount,
        int dop,
        Action<int> body,
        CancellationToken cancellationToken)
    {
        var ch = System.Threading.Channels.Channel.CreateBounded<int>(
            new System.Threading.Channels.BoundedChannelOptions(Math.Max(16, dop * 4))
            {
                SingleWriter = true,
                SingleReader = false,
                FullMode = System.Threading.Channels.BoundedChannelFullMode.Wait,
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
}

internal struct AggregateAccumulator
{
    public long ContributingRows;
    public long Count;
    public double FloatSum;
    public double FloatMin;
    public double FloatMax;
    public long IntSum;
    public bool HasMin;
    public bool HasMax;
}

internal static class AggregateRowOps
{
    public static void AddCountStar(ref AggregateAccumulator acc) => acc.Count++;

    public static void AddCountColumn(ref AggregateAccumulator acc, IColumnChunk col, int row)
    {
        var nb = col.HasNulls ? col.NullBitmap.Span : ReadOnlySpan<byte>.Empty;
        if (!SelectionEvaluator.IsNull(nb, row, col.HasNulls))
            acc.Count++;
    }

    public static void AddRow(ref AggregateAccumulator acc, IColumnChunk col, AggregateKind kind, int row)
    {
        var nb = col.HasNulls ? col.NullBitmap.Span : ReadOnlySpan<byte>.Empty;
        if (SelectionEvaluator.IsNull(nb, row, col.HasNulls))
            return;

        var values = col.Values.Span;
        switch (kind)
        {
            case AggregateKind.Sum when col.PhysicalType == RainDbType.Float64:
            {
                var bits = BinaryPrimitives.ReadInt64LittleEndian(values.Slice(row * sizeof(double), sizeof(double)));
                acc.FloatSum += BitConverter.Int64BitsToDouble(bits);
                acc.ContributingRows++;
                break;
            }
            case AggregateKind.Sum when col.PhysicalType == RainDbType.Int32:
            {
                acc.IntSum += BinaryPrimitives.ReadInt32LittleEndian(values.Slice(row * sizeof(int), sizeof(int)));
                acc.ContributingRows++;
                break;
            }
            case AggregateKind.Sum when col.PhysicalType == RainDbType.Int64:
            {
                acc.IntSum += BinaryPrimitives.ReadInt64LittleEndian(values.Slice(row * sizeof(long), sizeof(long)));
                acc.ContributingRows++;
                break;
            }
            case AggregateKind.Min when col.PhysicalType == RainDbType.Float64:
            {
                var bits = BinaryPrimitives.ReadInt64LittleEndian(values.Slice(row * sizeof(double), sizeof(double)));
                var v = BitConverter.Int64BitsToDouble(bits);
                if (!acc.HasMin)
                {
                    acc.FloatMin = v;
                    acc.HasMin = true;
                }
                else if (v < acc.FloatMin)
                    acc.FloatMin = v;

                acc.ContributingRows++;
                break;
            }
            case AggregateKind.Max when col.PhysicalType == RainDbType.Float64:
            {
                var bits = BinaryPrimitives.ReadInt64LittleEndian(values.Slice(row * sizeof(double), sizeof(double)));
                var v = BitConverter.Int64BitsToDouble(bits);
                if (!acc.HasMax)
                {
                    acc.FloatMax = v;
                    acc.HasMax = true;
                }
                else if (v > acc.FloatMax)
                    acc.FloatMax = v;

                acc.ContributingRows++;
                break;
            }
            default:
                throw new InvalidOperationException($"Unsupported aggregate {kind} on {col.PhysicalType}.");
        }
    }

    public static AggregateAccumulator Combine(AggregateAccumulator a, AggregateAccumulator b, AggregateKind kind) =>
        kind switch
        {
            AggregateKind.Count => new AggregateAccumulator { Count = a.Count + b.Count },
            AggregateKind.Sum => new AggregateAccumulator
            {
                ContributingRows = a.ContributingRows + b.ContributingRows,
                FloatSum = a.FloatSum + b.FloatSum,
                IntSum = a.IntSum + b.IntSum,
            },
            AggregateKind.Min => CombineMinMax(a, b, isMin: true),
            AggregateKind.Max => CombineMinMax(a, b, isMin: false),
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null),
        };

    private static AggregateAccumulator CombineMinMax(AggregateAccumulator a, AggregateAccumulator b, bool isMin)
    {
        var rows = a.ContributingRows + b.ContributingRows;
        if (isMin)
        {
            if (!a.HasMin)
            {
                var x = b;
                x.ContributingRows = rows;
                return x;
            }

            if (!b.HasMin)
            {
                var y = a;
                y.ContributingRows = rows;
                return y;
            }

            return new AggregateAccumulator
            {
                ContributingRows = rows,
                FloatMin = Math.Min(a.FloatMin, b.FloatMin),
                HasMin = true,
            };
        }

        if (!a.HasMax)
        {
            var x = b;
            x.ContributingRows = rows;
            return x;
        }

        if (!b.HasMax)
        {
            var y = a;
            y.ContributingRows = rows;
            return y;
        }

        return new AggregateAccumulator
        {
            ContributingRows = rows,
            FloatMax = Math.Max(a.FloatMax, b.FloatMax),
            HasMax = true,
        };
    }
}
