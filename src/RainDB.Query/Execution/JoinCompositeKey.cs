using RainDB.Columnar;
using RainDB.Core.Columnar;
using RainDB.Query.Vectorized;
using RainDB.Schema;

namespace RainDB.Query.Execution;

/// <summary>Join/group-style composite key supporting fixed-width columns and UTF-8 payloads.</summary>
internal sealed class CompositeJoinKey : IEquatable<CompositeJoinKey>
{
    public CompositeJoinKey(uint nullMask, ulong[] numericParts, byte[]?[] utf8Payloads)
    {
        NullMask = nullMask;
        NumericParts = numericParts;
        Utf8Payloads = utf8Payloads;
    }

    public uint NullMask { get; }

    public ulong[] NumericParts { get; }

    /// <summary>Per key column; non-null only when the schema column is UTF-8 and the value is non-null.</summary>
    public byte[]?[] Utf8Payloads { get; }

    public bool Equals(CompositeJoinKey? other)
    {
        if (other is null)
            return false;
        if (NullMask != other.NullMask)
            return false;
        if (!NumericParts.AsSpan().SequenceEqual(other.NumericParts))
            return false;
        if (Utf8Payloads.Length != other.Utf8Payloads.Length)
            return false;
        for (var i = 0; i < Utf8Payloads.Length; i++)
        {
            var a = Utf8Payloads[i];
            var b = other.Utf8Payloads[i];
            if (a is null && b is null)
                continue;
            if (a is null || b is null)
                return false;
            if (!a.AsSpan().SequenceEqual(b))
                return false;
        }

        return true;
    }

    public override bool Equals(object? obj) => Equals(obj as CompositeJoinKey);

    public override int GetHashCode()
    {
        var hc = new HashCode();
        hc.Add(NullMask);
        foreach (var n in NumericParts)
            hc.Add(n);
        foreach (var u in Utf8Payloads)
        {
            if (u is null)
            {
                hc.Add(0);
                continue;
            }

            hc.Add(u.Length);
            foreach (var b in u)
                hc.Add(b);
        }

        return hc.ToHashCode();
    }

    /// <summary>Deep copy for use as stable dictionary keys when merging partial aggregates.</summary>
    public CompositeJoinKey DeepClone()
    {
        var nums = (ulong[])NumericParts.Clone();
        byte[]?[] utf = new byte[Utf8Payloads.Length][];
        for (var i = 0; i < Utf8Payloads.Length; i++)
            utf[i] = Utf8Payloads[i] is { } u ? (byte[])u.Clone() : null;
        return new CompositeJoinKey(NullMask, nums, utf);
    }
}

internal sealed class CompositeJoinKeyComparer : IComparer<CompositeJoinKey>
{
    private readonly TableSchema _schema;
    private readonly int[] _keyIndices;

    public CompositeJoinKeyComparer(TableSchema schema, int[] keyIndices)
    {
        _schema = schema;
        _keyIndices = keyIndices;
    }

    public int Compare(CompositeJoinKey? x, CompositeJoinKey? y)
    {
        if (ReferenceEquals(x, y))
            return 0;
        if (x is null)
            return -1;
        if (y is null)
            return 1;

        var c = x.NullMask.CompareTo(y.NullMask);
        if (c != 0)
            return c;

        for (var i = 0; i < _keyIndices.Length; i++)
        {
            var t = _schema.Columns[_keyIndices[i]].Type;
            if (t == RainDbType.Utf8)
            {
                var ax = x.Utf8Payloads[i];
                var ay = y.Utf8Payloads[i];
                ReadOnlySpan<byte> sx = ax ?? ReadOnlySpan<byte>.Empty;
                ReadOnlySpan<byte> sy = ay ?? ReadOnlySpan<byte>.Empty;
                c = sx.SequenceCompareTo(sy);
                if (c != 0)
                    return c;
            }
            else
            {
                c = CompareFixedPart(t, x.NumericParts[i], y.NumericParts[i]);
                if (c != 0)
                    return c;
            }
        }

        return 0;
    }

    private static int CompareFixedPart(RainDbType type, ulong xa, ulong xb) =>
        FixedWidthKeyCompare.ComparePart(type, xa, xb);
}

internal static class CompositeJoinKeyBuilder
{
    public static CompositeJoinKey Build(TableSchema schema, IColumnarBatch batch, int row, int[] keyIndices)
    {
        var n = keyIndices.Length;
        var numeric = new ulong[n];
        byte[]?[] utf8Payloads = new byte[n][];
        uint mask = 0;
        for (var i = 0; i < n; i++)
        {
            var colIx = keyIndices[i];
            var col = batch.Columns[colIx];
            var typ = schema.Columns[colIx].Type;
            var nb = col.HasNulls ? col.NullBitmap.Span : ReadOnlySpan<byte>.Empty;
            if (SelectionEvaluator.IsNull(nb, row, col.HasNulls))
            {
                mask |= 1u << i;
                numeric[i] = 0;
                utf8Payloads[i] = null;
                continue;
            }

            if (typ == RainDbType.Utf8)
            {
                numeric[i] = 0;
                utf8Payloads[i] = CopyUtf8Payload(col, row);
            }
            else
            {
                utf8Payloads[i] = null;
                numeric[i] = FixedWidthGroupKeyBuilder.PhysicalValueToULong(col, row);
            }
        }

        return new CompositeJoinKey(mask, numeric, utf8Payloads);
    }

    private static byte[] CopyUtf8Payload(IColumnChunk col, int row) =>
        col switch
        {
            Utf8ColumnChunk utf8 =>
                utf8.Values.Span[utf8.Offsets.Span[row]..utf8.Offsets.Span[row + 1]].ToArray(),
            Utf8LengthPrefixedColumnChunk lp => lp.GetPayloadSpan(row).ToArray(),
            _ => throw new InvalidOperationException($"Unexpected UTF-8 physical chunk {col.GetType().Name}."),
        };
}
