using System.Text;

namespace RainDB.Logical;

/// <summary>Concatenates row sets from multiple SELECT branches (<c>UNION ALL</c>).</summary>
public sealed class LogicalUnionAll : ILogicalRoot
{
    public required IReadOnlyList<ILogicalRoot> Branches { get; init; }

    public string Explain(string indent = "")
    {
        var sb = new StringBuilder();
        sb.Append(indent).Append("LogicalUnionAll[").Append(Branches.Count).Append(" branches]");
        for (var i = 0; i < Branches.Count; i++)
            sb.AppendLine().Append(Branches[i].Explain(indent + "  "));
        return sb.ToString();
    }
}
