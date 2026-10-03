using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Execution;
using RainDB.Query.Execution.Operators;
using RainDB.Query.Plans;
using RainDB.Query.Results;
using RainDB.Query.Vectorized;
using RainDB.Schema;

namespace RainDB.Query.Execution;

public sealed class DistinctOperator : Operators.IDistinctOperator
{
    private readonly QueryOperatorDependencies _deps = new();

    public async ValueTask<IQueryResult> ExecuteAsync(
        DistinctPhysicalPlan plan,
        IQueryExecutor nestedExecutor,
        IExecutionContext context)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(nestedExecutor);
        ArgumentNullException.ThrowIfNull(context);
        var child = await nestedExecutor.ExecuteAsync(plan.Input, context).ConfigureAwait(false);
        if (child is not IColumnarQueryResult col)
            throw new InvalidOperationException("DISTINCT input must be columnar.");
        if (col.RowCount == 0)
            return new ColumnarMaterializedQueryResult([]);

        var schema = plan.OutputSchema;
        var seen = new HashSet<RowSignature>();
        var outBatches = new List<IColumnarBatch>();
        var buffer = new List<RowSignature>();
        const int chunk = 1024;
        foreach (var batch in col.Batches)
        {
            for (var row = 0; row < batch.RowCount; row++)
            {
                var sig = RowSignature.FromBatch(batch, row, schema.Columns.Count, _deps.Selection);
                if (!seen.Add(sig))
                    continue;
                buffer.Add(sig);
                if (buffer.Count >= chunk)
                {
                    outBatches.Add(MaterializeSignatures(buffer, schema));
                    buffer.Clear();
                }
            }
        }

        if (buffer.Count > 0)
            outBatches.Add(MaterializeSignatures(buffer, schema));

        return new ColumnarMaterializedQueryResult(outBatches);
    }

    private static ColumnarBatch MaterializeSignatures(List<RowSignature> rows, TableSchema schema)
    {
        var cols = new IColumnChunk[schema.Columns.Count];
        for (var c = 0; c < schema.Columns.Count; c++)
        {
            var type = schema.Columns[c].Type;
            cols[c] = type switch
            {
                RainDbType.Int32 => MaterializeInt32Column(rows, c),
                RainDbType.Int64 => MaterializeInt64Column(rows, c),
                RainDbType.Float64 => MaterializeFloat64Column(rows, c),
                RainDbType.Utf8 => MaterializeUtf8Column(rows, c),
                _ => throw new NotSupportedException($"DISTINCT does not support type {type}."),
            };
        }

        return new ColumnarBatch(rows.Count, cols);
    }

    private static FixedWidthColumnChunk MaterializeInt32Column(List<RowSignature> rows, int col)
    {
        var bytes = new byte[rows.Count * sizeof(int)];
        for (var i = 0; i < rows.Count; i++)
            System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(i * 4, 4), rows[i].Int32Cells[col]);
        return new FixedWidthColumnChunk(RainDbType.Int32, rows.Count, bytes, ReadOnlyMemory<byte>.Empty, false);
    }

    private static FixedWidthColumnChunk MaterializeInt64Column(List<RowSignature> rows, int col)
    {
        var bytes = new byte[rows.Count * sizeof(long)];
        for (var i = 0; i < rows.Count; i++)
            System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(i * 8, 8), rows[i].Int64Cells[col]);
        return new FixedWidthColumnChunk(RainDbType.Int64, rows.Count, bytes, ReadOnlyMemory<byte>.Empty, false);
    }

    private static FixedWidthColumnChunk MaterializeFloat64Column(List<RowSignature> rows, int col)
    {
        var bytes = new byte[rows.Count * sizeof(double)];
        for (var i = 0; i < rows.Count; i++)
            System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(
                bytes.AsSpan(i * 8, 8),
                System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(rows[i].Float64Bits[col]));
        return new FixedWidthColumnChunk(RainDbType.Float64, rows.Count, bytes, ReadOnlyMemory<byte>.Empty, false);
    }

    private static Utf8ColumnChunk MaterializeUtf8Column(List<RowSignature> rows, int col)
    {
        var offsets = new int[rows.Count + 1];
        var blob = new List<byte>();
        for (var i = 0; i < rows.Count; i++)
        {
            offsets[i] = blob.Count;
            blob.AddRange(rows[i].Utf8Cells[col]);
        }

        offsets[rows.Count] = blob.Count;
        return new Utf8ColumnChunk(rows.Count, offsets, blob.ToArray(), ReadOnlyMemory<byte>.Empty, false);
    }

    private readonly struct RowSignature : IEquatable<RowSignature>
    {
        public bool[] NullCells { get; init; }
        public int[] Int32Cells { get; init; }
        public long[] Int64Cells { get; init; }
        public byte[][] Float64Bits { get; init; }
        public byte[][] Utf8Cells { get; init; }

        public static RowSignature FromBatch(IColumnarBatch batch, int row, int colCount, SelectionEvaluator selection)
        {
            var nulls = new bool[colCount];
            var i32 = new int[colCount];
            var i64 = new long[colCount];
            var f64 = new byte[colCount][];
            var utf8 = new byte[colCount][];
            for (var c = 0; c < colCount; c++)
            {
                var col = batch.Columns[c];
                var nb = col.HasNulls ? col.NullBitmap.Span : ReadOnlySpan<byte>.Empty;
                if (selection.IsNull(nb, row, col.HasNulls))
                {
                    nulls[c] = true;
                    f64[c] = [];
                    utf8[c] = [];
                    continue;
                }

                switch (col.PhysicalType)
                {
                    case RainDbType.Int32:
                        i32[c] = System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(
                            col.Values.Span.Slice(row * sizeof(int), sizeof(int)));
                        break;
                    case RainDbType.Int64:
                        i64[c] = System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(
                            col.Values.Span.Slice(row * sizeof(long), sizeof(long)));
                        break;
                    case RainDbType.Float64:
                        f64[c] = col.Values.Span.Slice(row * sizeof(double), sizeof(double)).ToArray();
                        break;
                    case RainDbType.Utf8:
                        utf8[c] = ReadUtf8Payload(col, row);
                        break;
                }
            }

            return new RowSignature { NullCells = nulls, Int32Cells = i32, Int64Cells = i64, Float64Bits = f64, Utf8Cells = utf8 };
        }

        private static byte[] ReadUtf8Payload(IColumnChunk col, int row) =>
            col switch
            {
                Utf8ColumnChunk u => u.Values.Span[u.Offsets.Span[row]..u.Offsets.Span[row + 1]].ToArray(),
                Utf8LengthPrefixedColumnChunk lp => lp.GetPayloadSpan(row).ToArray(),
                _ => throw new InvalidOperationException(),
            };

        public bool Equals(RowSignature other)
        {
            for (var i = 0; i < Int32Cells.Length; i++)
            {
                if (NullCells[i] != other.NullCells[i])
                    return false;
                if (NullCells[i])
                    continue;
                if (Int32Cells[i] != other.Int32Cells[i]
                    || Int64Cells[i] != other.Int64Cells[i]
                    || !Float64Bits[i].AsSpan().SequenceEqual(other.Float64Bits[i])
                    || !Utf8Cells[i].AsSpan().SequenceEqual(other.Utf8Cells[i]))
                    return false;
            }

            return true;
        }

        public override bool Equals(object? obj) => obj is RowSignature rs && Equals(rs);

        public override int GetHashCode()
        {
            var hc = new HashCode();
            foreach (var n in NullCells)
                hc.Add(n);
            foreach (var v in Int32Cells)
                hc.Add(v);
            foreach (var v in Int64Cells)
                hc.Add(v);
            foreach (var f in Float64Bits)
                hc.AddBytes(f);
            foreach (var u in Utf8Cells)
                hc.AddBytes(u);
            return hc.ToHashCode();
        }
    }
}
