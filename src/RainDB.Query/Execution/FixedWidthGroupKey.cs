using System.Buffers.Binary;
using RainDB.Columnar;
using RainDB.Query.Vectorized;
using RainDB.Schema;

namespace RainDB.Query.Execution;

/// <summary>Composite fixed-width group/join key with per-column null bits.</summary>
internal sealed class GroupKey : IEquatable<GroupKey>
{
    public GroupKey(ulong[] parts, uint nullMask)
    {
        Parts = parts;
        NullMask = nullMask;
    }

    public ulong[] Parts { get; }

    public uint NullMask { get; }

    public bool Equals(GroupKey? other) =>
        other != null && NullMask == other.NullMask && Parts.AsSpan().SequenceEqual(other.Parts);

    public override bool Equals(object? obj) => Equals(obj as GroupKey);

    public override int GetHashCode()
    {
        var hc = new HashCode();
        hc.Add(NullMask);
        foreach (var p in Parts)
            hc.Add(p);
        return hc.ToHashCode();
    }
}

internal sealed class GroupKeyComparer : IComparer<GroupKey>
{
    private readonly TableSchema _schema;
    private readonly int[] _keyIndices;

    public GroupKeyComparer(TableSchema schema, int[] keyIndices)
    {
        _schema = schema;
        _keyIndices = keyIndices;
    }

    public int Compare(GroupKey? x, GroupKey? y)
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
            c = ComparePart(t, x.Parts[i], y.Parts[i]);
            if (c != 0)
                return c;
        }

        return 0;
    }

    private static int ComparePart(RainDbType type, ulong xa, ulong xb) =>
        FixedWidthKeyCompare.ComparePart(type, xa, xb);
}

