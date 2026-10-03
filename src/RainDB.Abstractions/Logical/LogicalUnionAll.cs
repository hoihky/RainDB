using System.Text;

namespace RainDB.Logical;

/// <summary>Combines row sets from multiple SELECT branches (<c>UNION</c> / <c>UNION ALL</c>).</summary>
public sealed class LogicalUnionAll : ILogicalRoot
{
    public required IReadOnlyList<ILogicalRoot> Branches { get; init; }

    /// <summary>
    /// For each <c>i</c> in <c>0 .. Branches.Count - 2</c>, when <see langword="true"/> the merge before branch <c>i + 1</c> is
    /// <c>UNION</c> (dedup); when <see langword="false"/>, <c>UNION ALL</c>. Left-associative chain.
    /// </summary>
    public IReadOnlyList<bool>? DistinctBetweenBranches { get; init; }

    /// <summary>When <see langword="true"/>, entire chain is <c>UNION ALL</c> (legacy when <see cref="DistinctBetweenBranches"/> is unset).</summary>
    public bool UnionAll { get; init; } = true;

    public string Explain(string indent = "")
    {
        var sb = new StringBuilder();
        sb.Append(indent).Append("LogicalUnionAll[").Append(Branches.Count).Append(" branches]");
        for (var i = 0; i < Branches.Count; i++)
            sb.AppendLine().Append(Branches[i].Explain(indent + "  "));
        return sb.ToString();
    }
}
