using System.Buffers;
using System.Buffers.Binary;
using RainDB.Catalog;
using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Execution;
using RainDB.Query.Execution.Operators;
using RainDB.Query.Plans;
using RainDB.Query.Results;
using RainDB.Query.Runtime;
using RainDB.Query.Vectorized;
using RainDB.Schema;

namespace RainDB.Query.Execution;

/// <summary>Parallel partial hash maps per source batch, deterministic global merge, sorted key materialization.</summary>
public sealed class HashAggregateOperator : Operators.IHashAggregateOperator, Operators.IHashAggregateGroupingSupport
{
    private readonly QueryOperatorDependencies _deps;

    public HashAggregateOperator()
        : this(new QueryOperatorDependencies())
    {
    }

    internal HashAggregateOperator(QueryOperatorDependencies dependencies) =>
        _deps = dependencies ?? throw new ArgumentNullException(nameof(dependencies));

    public async ValueTask<IQueryResult> ExecuteAsync(
        HashAggregatePhysicalPlan plan,
        IColumnarTableSource table,
        IExecutionContext context)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(context);

        ValidatePlan(plan, table);

        var subFilters = await ResolveSubqueryFiltersAsync(plan, context).ConfigureAwait(false);
        if (subFilters.IsDenyAll)
        {
            var emptyCols = MaterializeEmptyOutput(plan, table.Schema);
            return new ColumnarMaterializedQueryResult([new ColumnarBatch(0, emptyCols)]);
        }

        if (AnyUtf8GroupKey(plan, table.Schema))
            return await ExecuteWithCompositeKeysAsync(plan, table, context, subFilters).ConfigureAwait(false);

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
                partials[i] = AccumulateBatch(batches[i], plan, subFilters.InFilters, ct);
        }
        else if (plan.Options.UseChannelScheduler)
        {
            await RunChannelMorselsAsync(
                    n,
                    dop,
                    i => partials[i] = AccumulateBatch(batches[i], plan, subFilters.InFilters, ct),
                    ct)
                .ConfigureAwait(false);
        }
        else
        {
            Parallel.For(
                0,
                n,
                new ParallelOptions { MaxDegreeOfParallelism = dop, CancellationToken = ct },
                i => partials[i] = AccumulateBatch(batches[i], plan, subFilters.InFilters, ct));
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
        outBatch = ApplyHavingIfNeeded(outBatch, plan, context);
        return new ColumnarMaterializedQueryResult([outBatch]);
    }

    private static ColumnarBatch ApplyHavingIfNeeded(ColumnarBatch batch, HashAggregatePhysicalPlan plan, IExecutionContext context)
    {
        if (plan.HavingFilters is not { Length: > 0 } hf)
            return batch;
        return GroupHavingEvaluator.Apply(batch, hf, context.BufferPool, context.AlignedBufferPool);
    }

    void Operators.IHashAggregateGroupingSupport.ValidatePlanForInputSchema(HashAggregatePhysicalPlan plan, TableSchema schema)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(schema);
        ValidatePlanColumns(plan, schema);
    }

    private static void ValidatePlan(HashAggregatePhysicalPlan plan, IColumnarTableSource table)
    {
        if (plan.TableId != table.Id)
            throw new ArgumentException("Physical plan table id does not match resolved table.", nameof(table));

        ValidatePlanColumns(plan, table.Schema);
    }

    private static void ValidatePlanColumns(HashAggregatePhysicalPlan plan, TableSchema schema)
    {
        var colCount = schema.Columns.Count;
        foreach (var idx in plan.GroupKeyColumnIndices)
        {
            if ((uint)idx >= (uint)colCount)
                throw new ArgumentException($"Group key column index {idx} is out of range.", nameof(plan));
            var kt = schema.Columns[idx].Type;
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
            var t = schema.Columns[a.SourceColumnIndex].Type;
            ValidateAggregate(t, a.Kind);
        }
    }

    Dictionary<GroupKey, AggregateAccumulator[]> Operators.IHashAggregateGroupingSupport.AccumulateBatchForGrouped(
        IColumnarBatch batch,
        HashAggregatePhysicalPlan plan,
        CancellationToken cancellationToken) =>
        AccumulateBatch(batch, plan, null, cancellationToken);

    Dictionary<CompositeJoinKey, AggregateAccumulator[]> Operators.IHashAggregateGroupingSupport.AccumulateBatchCompositeForGrouped(
        IColumnarBatch batch,
        HashAggregatePhysicalPlan plan,
        TableSchema schema,
        CancellationToken cancellationToken) =>
        AccumulateBatchComposite(batch, plan, schema, null, cancellationToken);

    void Operators.IHashAggregateGroupingSupport.MergePartialIntoGlobal(
        Dictionary<GroupKey, AggregateAccumulator[]> global,
        Dictionary<GroupKey, AggregateAccumulator[]> partial,
        AggregateSpec[] specs) =>
        MergePartialIntoGlobalCore(global, partial, specs);

    void Operators.IHashAggregateGroupingSupport.MergePartialIntoGlobalComposite(
        Dictionary<CompositeJoinKey, AggregateAccumulator[]> global,
        Dictionary<CompositeJoinKey, AggregateAccumulator[]> partial,
        AggregateSpec[] specs) =>
        MergePartialIntoGlobalCompositeCore(global, partial, specs);

    private static void MergePartialIntoGlobalCore(
        Dictionary<GroupKey, AggregateAccumulator[]> global,
        Dictionary<GroupKey, AggregateAccumulator[]> partial,
        AggregateSpec[] specs)
    {
        var aggCount = specs.Length;
        foreach (var kv in partial)
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

    private static void MergePartialIntoGlobalCompositeCore(
        Dictionary<CompositeJoinKey, AggregateAccumulator[]> global,
        Dictionary<CompositeJoinKey, AggregateAccumulator[]> partial,
        AggregateSpec[] specs)
    {
        var aggCount = specs.Length;
        foreach (var kv in partial)
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

    ValueTask<IQueryResult> Operators.IHashAggregateGroupingSupport.MaterializeFromGlobalAsync(
        HashAggregatePhysicalPlan plan,
        TableSchema inputSchema,
        Dictionary<GroupKey, AggregateAccumulator[]> global)
    {
        if (global.Count == 0)
        {
            var emptyCols = MaterializeEmptyOutput(plan, inputSchema);
            return new ValueTask<IQueryResult>(new ColumnarMaterializedQueryResult([new ColumnarBatch(0, emptyCols)]));
        }

        var sortedKeys = SortKeys(global.Keys, inputSchema, plan.GroupKeyColumnIndices);
        var outBatch = MaterializeOutput(sortedKeys, global, plan, inputSchema);
        return new ValueTask<IQueryResult>(new ColumnarMaterializedQueryResult([outBatch]));
    }

    ValueTask<IQueryResult> Operators.IHashAggregateGroupingSupport.MaterializeFromGlobalCompositeAsync(
        HashAggregatePhysicalPlan plan,
        TableSchema inputSchema,
        Dictionary<CompositeJoinKey, AggregateAccumulator[]> global)
    {
        if (global.Count == 0)
        {
            var emptyCols = MaterializeEmptyOutput(plan, inputSchema);
            return new ValueTask<IQueryResult>(new ColumnarMaterializedQueryResult([new ColumnarBatch(0, emptyCols)]));
        }

        var sortedKeys = SortCompositeKeys(global.Keys, inputSchema, plan.GroupKeyColumnIndices);
        var outBatch = MaterializeOutputComposite(sortedKeys, global, plan, inputSchema);
        return new ValueTask<IQueryResult>(new ColumnarMaterializedQueryResult([outBatch]));
    }

    private static void ValidateAggregate(RainDbType columnType, AggregateKind kind)
    {
        switch (kind)
        {
            case AggregateKind.Count or AggregateKind.CountDistinct:
                return;
            case AggregateKind.Sum when columnType is RainDbType.Int32 or RainDbType.Int64 or RainDbType.Float64:
                return;
            case AggregateKind.Min or AggregateKind.Max when columnType is RainDbType.Float64 or RainDbType.Int32 or RainDbType.Int64 or RainDbType.Utf8:
                return;
            default:
                throw new NotSupportedException($"Aggregate {kind} on {columnType} is not supported for hash aggregation.");
        }
    }

    private Dictionary<GroupKey, AggregateAccumulator[]> AccumulateBatch(
        IColumnarBatch batch,
        HashAggregatePhysicalPlan plan,
        ColumnInSetFilter[]? inFilters,
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
            var compare = plan.Filters is { Length: > 0 } filters ? filters.AsSpan() : ReadOnlySpan<ColumnCompareFilter>.Empty;
            var inSpan = inFilters is { Length: > 0 } inf ? inf.AsSpan() : ReadOnlySpan<ColumnInSetFilter>.Empty;
            if (compare.Length > 0 || inSpan.Length > 0)
            {
                k = _deps.Selection.FillSelectedRowsConjunctive(batch, compare, inSpan, rent.AsSpan(0, batch.RowCount));
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
                    var key = _deps.GroupKeys.BuildKey(batch, row, plan.GroupKeyColumnIndices, scratch);
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
                        else if (spec.Kind == AggregateKind.CountDistinct)
                            AggregateRowOps.AddCountDistinct(ref slot, _deps.Selection, batch.Columns[spec.SourceColumnIndex], row);
                        else if (spec.Kind == AggregateKind.Count)
                            AggregateRowOps.AddCountColumn(ref slot, _deps.Selection, batch.Columns[spec.SourceColumnIndex], row);
                        else
                            AggregateRowOps.AddRow(ref slot, _deps.Selection, batch.Columns[spec.SourceColumnIndex], spec.Kind, row);
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
        var global = new Dictionary<GroupKey, AggregateAccumulator[]>();
        for (var bi = 0; bi < partials.Length; bi++)
            MergePartialIntoGlobalCore(global, partials[bi], specs);

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
        if (resultType == RainDbType.Utf8)
            return MaterializeUtf8AggregateColumn(sortedKeys, global, aggIndex, spec, rowCount);

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

    private static IColumnChunk MaterializeUtf8AggregateColumn(
        GroupKey[] sortedKeys,
        Dictionary<GroupKey, AggregateAccumulator[]> global,
        int aggIndex,
        AggregateSpec spec,
        int rowCount)
    {
        var offsets = new int[rowCount + 1];
        var blob = new List<byte>();
        var nbBytes = ColumnTypeSizes.NullBitmapBytes(rowCount);
        var nb = nbBytes > 0 ? new byte[nbBytes] : Array.Empty<byte>();
        var anyNull = false;
        for (var r = 0; r < rowCount; r++)
        {
            offsets[r] = blob.Count;
            var acc = global[sortedKeys[r]][aggIndex];
            if (ShouldEmitAggregateNull(spec, acc))
            {
                anyNull = true;
                SetNull(nb, r);
                continue;
            }

            var bytes = spec.Kind == AggregateKind.Min ? acc.Utf8MinBytes! : acc.Utf8MaxBytes!;
            blob.AddRange(bytes);
        }

        offsets[rowCount] = blob.Count;
        return new Utf8ColumnChunk(rowCount, offsets, blob.ToArray(), nb, anyNull);
    }

    private static bool ShouldEmitAggregateNull(AggregateSpec spec, AggregateAccumulator acc)
    {
        return spec.Kind switch
        {
            AggregateKind.Sum => acc.ContributingRows == 0,
            AggregateKind.Min => !acc.HasMin,
            AggregateKind.Max => !acc.HasMax,
            AggregateKind.Count or AggregateKind.CountDistinct => false,
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
            case AggregateKind.CountDistinct:
                BinaryPrimitives.WriteInt64LittleEndian(dest, AggregateRowOps.DistinctCount(acc));
                break;
            case AggregateKind.Sum when srcType == RainDbType.Float64:
                BinaryPrimitives.WriteInt64LittleEndian(dest, BitConverter.DoubleToInt64Bits(acc.FloatSum));
                break;
            case AggregateKind.Sum:
                BinaryPrimitives.WriteInt64LittleEndian(dest, acc.IntSum);
                break;
            case AggregateKind.Min when srcType == RainDbType.Int32:
                BinaryPrimitives.WriteInt32LittleEndian(dest, acc.Int32Min);
                break;
            case AggregateKind.Max when srcType == RainDbType.Int32:
                BinaryPrimitives.WriteInt32LittleEndian(dest, acc.Int32Max);
                break;
            case AggregateKind.Min when srcType == RainDbType.Int64:
                BinaryPrimitives.WriteInt64LittleEndian(dest, acc.Int64Min);
                break;
            case AggregateKind.Max when srcType == RainDbType.Int64:
                BinaryPrimitives.WriteInt64LittleEndian(dest, acc.Int64Max);
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
            AggregateKind.Count or AggregateKind.CountDistinct => RainDbType.Int64,
            AggregateKind.Sum when spec.SourceColumnIndex >= 0 && schema.Columns[spec.SourceColumnIndex].Type == RainDbType.Float64 => RainDbType.Float64,
            AggregateKind.Sum => RainDbType.Int64,
            AggregateKind.Min or AggregateKind.Max when spec.SourceColumnIndex >= 0 => schema.Columns[spec.SourceColumnIndex].Type,
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

    bool Operators.IHashAggregateGroupingSupport.UsesCompositeGroupKeys(HashAggregatePhysicalPlan plan, TableSchema schema) =>
        AnyUtf8GroupKey(plan, schema);

    private static bool AnyUtf8GroupKey(HashAggregatePhysicalPlan plan, TableSchema schema)
    {
        foreach (var ix in plan.GroupKeyColumnIndices)
        {
            if (schema.Columns[ix].Type == RainDbType.Utf8)
                return true;
        }

        return false;
    }

    private static async ValueTask<ResolvedSubqueryFilters> ResolveSubqueryFiltersAsync(
        HashAggregatePhysicalPlan plan,
        IExecutionContext context)
    {
        if (plan.InSubqueries is null && plan.ExistsSubqueries is null)
            return ResolvedSubqueryFilters.Empty;
        if (context is not RainDbExecutionContext { NestedExecutor: { } executor })
            throw new InvalidOperationException("Subquery predicates require NestedExecutor on the execution context.");
        return await SubqueryFilterResolver.ResolveAsync(
            plan.InSubqueries,
            plan.ExistsSubqueries,
            executor,
            context).ConfigureAwait(false);
    }

    private async ValueTask<IQueryResult> ExecuteWithCompositeKeysAsync(
        HashAggregatePhysicalPlan plan,
        IColumnarTableSource table,
        IExecutionContext context,
        ResolvedSubqueryFilters subFilters)
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
                partials[i] = AccumulateBatchComposite(batches[i], plan, schema, subFilters.InFilters, ct);
        }
        else if (plan.Options.UseChannelScheduler)
        {
            await RunChannelMorselsAsync(
                    n,
                    dop,
                    i => partials[i] = AccumulateBatchComposite(batches[i], plan, schema, subFilters.InFilters, ct),
                    ct)
                .ConfigureAwait(false);
        }
        else
        {
            Parallel.For(
                0,
                n,
                new ParallelOptions { MaxDegreeOfParallelism = dop, CancellationToken = ct },
                i => partials[i] = AccumulateBatchComposite(batches[i], plan, schema, subFilters.InFilters, ct));
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
        outBatch = ApplyHavingIfNeeded(outBatch, plan, context);
        return new ColumnarMaterializedQueryResult([outBatch]);
    }

    private Dictionary<CompositeJoinKey, AggregateAccumulator[]> AccumulateBatchComposite(
        IColumnarBatch batch,
        HashAggregatePhysicalPlan plan,
        TableSchema schema,
        ColumnInSetFilter[]? inFilters,
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
            var compare = plan.Filters is { Length: > 0 } filters ? filters.AsSpan() : ReadOnlySpan<ColumnCompareFilter>.Empty;
            var inSpan = inFilters is { Length: > 0 } inf ? inf.AsSpan() : ReadOnlySpan<ColumnInSetFilter>.Empty;
            if (compare.Length > 0 || inSpan.Length > 0)
            {
                k = _deps.Selection.FillSelectedRowsConjunctive(batch, compare, inSpan, rent.AsSpan(0, batch.RowCount));
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
                var key = _deps.CompositeJoinKeys.Build(schema, batch, row, plan.GroupKeyColumnIndices);
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
                    else if (spec.Kind == AggregateKind.CountDistinct)
                        AggregateRowOps.AddCountDistinct(ref slot, _deps.Selection, batch.Columns[spec.SourceColumnIndex], row);
                    else if (spec.Kind == AggregateKind.Count)
                        AggregateRowOps.AddCountColumn(ref slot, _deps.Selection, batch.Columns[spec.SourceColumnIndex], row);
                    else
                        AggregateRowOps.AddRow(ref slot, _deps.Selection, batch.Columns[spec.SourceColumnIndex], spec.Kind, row);
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
        var global = new Dictionary<CompositeJoinKey, AggregateAccumulator[]>();
        for (var bi = 0; bi < partials.Length; bi++)
            MergePartialIntoGlobalCompositeCore(global, partials[bi], specs);

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

    public HashSet<int>? DistinctInt32;
    public HashSet<long>? DistinctInt64;
    public HashSet<byte[]>? DistinctUtf8;
    public double FloatSum;
    public double FloatMin;
    public double FloatMax;
    public long IntSum;
    public int Int32Min;
    public int Int32Max;
    public long Int64Min;
    public long Int64Max;
    public byte[]? Utf8MinBytes;
    public byte[]? Utf8MaxBytes;
    public RainDbType ExtremumPhysicalType;
    public bool HasMin;
    public bool HasMax;
}

internal static class AggregateRowOps
{
    public static void AddCountStar(ref AggregateAccumulator acc) => acc.Count++;

    public static long DistinctCount(AggregateAccumulator acc) =>
        acc.DistinctInt32?.Count ?? acc.DistinctInt64?.Count ?? acc.DistinctUtf8?.Count ?? 0;

    public static void AddCountDistinct(
        ref AggregateAccumulator acc,
        SelectionEvaluator selection,
        IColumnChunk col,
        int row)
    {
        var nb = col.HasNulls ? col.NullBitmap.Span : ReadOnlySpan<byte>.Empty;
        if (selection.IsNull(nb, row, col.HasNulls))
            return;

        var values = col.Values.Span;
        switch (col.PhysicalType)
        {
            case RainDbType.Int32:
                acc.DistinctInt32 ??= new HashSet<int>();
                acc.DistinctInt32.Add(BinaryPrimitives.ReadInt32LittleEndian(values.Slice(row * sizeof(int), sizeof(int))));
                break;
            case RainDbType.Int64:
                acc.DistinctInt64 ??= new HashSet<long>();
                acc.DistinctInt64.Add(BinaryPrimitives.ReadInt64LittleEndian(values.Slice(row * sizeof(long), sizeof(long))));
                break;
            case RainDbType.Float64:
                acc.DistinctInt64 ??= new HashSet<long>();
                acc.DistinctInt64.Add(BinaryPrimitives.ReadInt64LittleEndian(values.Slice(row * sizeof(double), sizeof(double))));
                break;
            case RainDbType.Utf8:
                acc.DistinctUtf8 ??= new HashSet<byte[]>(ByteArrayComparer.Instance);
                acc.DistinctUtf8.Add(Utf8Payload(col, row).ToArray());
                break;
            default:
                throw new NotSupportedException($"COUNT(DISTINCT) is not supported for type {col.PhysicalType}.");
        }
    }

    private sealed class ByteArrayComparer : IEqualityComparer<byte[]>
    {
        public static readonly ByteArrayComparer Instance = new();

        public bool Equals(byte[]? x, byte[]? y) =>
            x is not null && y is not null && x.AsSpan().SequenceEqual(y);

        public int GetHashCode(byte[] obj)
        {
            var hc = new HashCode();
            hc.AddBytes(obj);
            return hc.ToHashCode();
        }
    }

    public static void AddCountColumn(
        ref AggregateAccumulator acc,
        SelectionEvaluator selection,
        IColumnChunk col,
        int row)
    {
        var nb = col.HasNulls ? col.NullBitmap.Span : ReadOnlySpan<byte>.Empty;
        if (!selection.IsNull(nb, row, col.HasNulls))
            acc.Count++;
    }

    public static void AddRow(
        ref AggregateAccumulator acc,
        SelectionEvaluator selection,
        IColumnChunk col,
        AggregateKind kind,
        int row)
    {
        var nb = col.HasNulls ? col.NullBitmap.Span : ReadOnlySpan<byte>.Empty;
        if (selection.IsNull(nb, row, col.HasNulls))
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
                    acc.ExtremumPhysicalType = RainDbType.Float64;
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
                    acc.ExtremumPhysicalType = RainDbType.Float64;
                    acc.HasMax = true;
                }
                else if (v > acc.FloatMax)
                    acc.FloatMax = v;

                acc.ContributingRows++;
                break;
            }
            case AggregateKind.Min when col.PhysicalType == RainDbType.Int32:
            {
                var v = BinaryPrimitives.ReadInt32LittleEndian(values.Slice(row * sizeof(int), sizeof(int)));
                if (!acc.HasMin)
                {
                    acc.Int32Min = v;
                    acc.ExtremumPhysicalType = RainDbType.Int32;
                    acc.HasMin = true;
                }
                else if (v < acc.Int32Min)
                    acc.Int32Min = v;
                acc.ContributingRows++;
                break;
            }
            case AggregateKind.Max when col.PhysicalType == RainDbType.Int32:
            {
                var v = BinaryPrimitives.ReadInt32LittleEndian(values.Slice(row * sizeof(int), sizeof(int)));
                if (!acc.HasMax)
                {
                    acc.Int32Max = v;
                    acc.ExtremumPhysicalType = RainDbType.Int32;
                    acc.HasMax = true;
                }
                else if (v > acc.Int32Max)
                    acc.Int32Max = v;
                acc.ContributingRows++;
                break;
            }
            case AggregateKind.Min when col.PhysicalType == RainDbType.Int64:
            {
                var v = BinaryPrimitives.ReadInt64LittleEndian(values.Slice(row * sizeof(long), sizeof(long)));
                if (!acc.HasMin)
                {
                    acc.Int64Min = v;
                    acc.ExtremumPhysicalType = RainDbType.Int64;
                    acc.HasMin = true;
                }
                else if (v < acc.Int64Min)
                    acc.Int64Min = v;
                acc.ContributingRows++;
                break;
            }
            case AggregateKind.Max when col.PhysicalType == RainDbType.Int64:
            {
                var v = BinaryPrimitives.ReadInt64LittleEndian(values.Slice(row * sizeof(long), sizeof(long)));
                if (!acc.HasMax)
                {
                    acc.Int64Max = v;
                    acc.ExtremumPhysicalType = RainDbType.Int64;
                    acc.HasMax = true;
                }
                else if (v > acc.Int64Max)
                    acc.Int64Max = v;
                acc.ContributingRows++;
                break;
            }
            case AggregateKind.Min when col.PhysicalType == RainDbType.Utf8:
            {
                var payload = Utf8Payload(col, row);
                if (!acc.HasMin)
                {
                    acc.Utf8MinBytes = payload.ToArray();
                    acc.ExtremumPhysicalType = RainDbType.Utf8;
                    acc.HasMin = true;
                }
                else if (Utf8Compare(payload, acc.Utf8MinBytes!) < 0)
                    acc.Utf8MinBytes = payload.ToArray();
                acc.ContributingRows++;
                break;
            }
            case AggregateKind.Max when col.PhysicalType == RainDbType.Utf8:
            {
                var payload = Utf8Payload(col, row);
                if (!acc.HasMax)
                {
                    acc.Utf8MaxBytes = payload.ToArray();
                    acc.ExtremumPhysicalType = RainDbType.Utf8;
                    acc.HasMax = true;
                }
                else if (Utf8Compare(payload, acc.Utf8MaxBytes!) > 0)
                    acc.Utf8MaxBytes = payload.ToArray();
                acc.ContributingRows++;
                break;
            }
            default:
                throw new InvalidOperationException($"Unsupported aggregate {kind} on {col.PhysicalType}.");
        }
    }

    private static AggregateAccumulator CombineDistinct(AggregateAccumulator a, AggregateAccumulator b)
    {
        if (a.DistinctInt32 is not null || b.DistinctInt32 is not null)
        {
            a.DistinctInt32 ??= new HashSet<int>();
            if (b.DistinctInt32 is not null)
                a.DistinctInt32.UnionWith(b.DistinctInt32);
            return a;
        }

        if (a.DistinctInt64 is not null || b.DistinctInt64 is not null)
        {
            a.DistinctInt64 ??= new HashSet<long>();
            if (b.DistinctInt64 is not null)
                a.DistinctInt64.UnionWith(b.DistinctInt64);
            return a;
        }

        if (a.DistinctUtf8 is not null || b.DistinctUtf8 is not null)
        {
            a.DistinctUtf8 ??= new HashSet<byte[]>(ByteArrayComparer.Instance);
            if (b.DistinctUtf8 is not null)
            {
                foreach (var u in b.DistinctUtf8)
                    a.DistinctUtf8.Add(u);
            }

            return a;
        }

        return a;
    }

    public static AggregateAccumulator Combine(AggregateAccumulator a, AggregateAccumulator b, AggregateKind kind) =>
        kind switch
        {
            AggregateKind.Count => new AggregateAccumulator { Count = a.Count + b.Count },
            AggregateKind.CountDistinct => CombineDistinct(a, b),
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

            return CombineTypedMinMax(a, b, rows, isMin: true);
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

        return CombineTypedMinMax(a, b, rows, isMin: false);
    }

    private static AggregateAccumulator CombineTypedMinMax(AggregateAccumulator a, AggregateAccumulator b, long rows, bool isMin)
    {
        var t = a.ExtremumPhysicalType != default ? a.ExtremumPhysicalType : b.ExtremumPhysicalType;
        return t switch
        {
            RainDbType.Utf8 => new AggregateAccumulator
            {
                ContributingRows = rows,
                ExtremumPhysicalType = RainDbType.Utf8,
                Utf8MinBytes = isMin
                    ? (Utf8Compare(a.Utf8MinBytes, b.Utf8MinBytes) <= 0 ? a.Utf8MinBytes : b.Utf8MinBytes)
                    : null,
                Utf8MaxBytes = isMin
                    ? null
                    : (Utf8Compare(a.Utf8MaxBytes, b.Utf8MaxBytes) >= 0 ? a.Utf8MaxBytes : b.Utf8MaxBytes),
                HasMin = isMin,
                HasMax = !isMin,
            },
            RainDbType.Int32 => new AggregateAccumulator
            {
                ContributingRows = rows,
                ExtremumPhysicalType = RainDbType.Int32,
                Int32Min = isMin ? Math.Min(a.Int32Min, b.Int32Min) : 0,
                Int32Max = isMin ? 0 : Math.Max(a.Int32Max, b.Int32Max),
                HasMin = isMin,
                HasMax = !isMin,
            },
            RainDbType.Int64 => new AggregateAccumulator
            {
                ContributingRows = rows,
                ExtremumPhysicalType = RainDbType.Int64,
                Int64Min = isMin ? Math.Min(a.Int64Min, b.Int64Min) : 0,
                Int64Max = isMin ? 0 : Math.Max(a.Int64Max, b.Int64Max),
                HasMin = isMin,
                HasMax = !isMin,
            },
            _ => new AggregateAccumulator
            {
                ContributingRows = rows,
                ExtremumPhysicalType = RainDbType.Float64,
                FloatMin = isMin ? Math.Min(a.FloatMin, b.FloatMin) : 0,
                FloatMax = isMin ? 0 : Math.Max(a.FloatMax, b.FloatMax),
                HasMin = isMin,
                HasMax = !isMin,
            },
        };
    }

    private static ReadOnlySpan<byte> Utf8Payload(IColumnChunk col, int row) =>
        col switch
        {
            Utf8ColumnChunk u => u.Values.Span[u.Offsets.Span[row]..u.Offsets.Span[row + 1]],
            Utf8LengthPrefixedColumnChunk lp => lp.GetPayloadSpan(row),
            _ => throw new InvalidOperationException(),
        };

    private static int Utf8Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) =>
        a.SequenceCompareTo(b);

    private static int Utf8Compare(byte[]? a, byte[]? b)
    {
        if (a is null && b is null)
            return 0;
        if (a is null)
            return -1;
        if (b is null)
            return 1;
        return a.AsSpan().SequenceCompareTo(b);
    }
}
