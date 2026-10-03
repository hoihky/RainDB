using System.Text;

namespace RainDB.Logical;

/// <summary>Combines row sets from multiple SELECT branches (<c>UNION</c> / <c>UNION ALL</c>).</summary>
public sealed class LogicalUnionAll : ILogicalRoot
{
    public required IReadOnlyList<ILogicalRoot> Branches { get; init; }

    /// <summary>When <see langword="true"/>, keeps duplicate rows (<c>UNION ALL</c>); otherwise <c>UNION</c> deduplicates.</summary>
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
